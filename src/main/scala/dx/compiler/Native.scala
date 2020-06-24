/** Generate a dx:worflow and dx:applets from an intermediate representation.
  */
package dx.compiler

import java.io.EOFException
import java.nio.file.{Path, Paths}
import java.security.MessageDigest

import dx.api.{
  ConstraintOper,
  DxAccessLevel,
  DxApi,
  DxAppDescribe,
  DxApplet,
  DxAppletDescribe,
  DxConstraint,
  DxDataObject,
  DxExecutable,
  DxFile,
  DxFileDescribe,
  DxIOSpec,
  DxPath,
  DxProject,
  DxWorkflow,
  DxWorkflowDescribe,
  Field,
  InstanceTypeDB,
  InstanceTypeReq
}
import dx.compiler.IR.{CVar, SArg}
import dx.core.languages.IORef
import dx.core.languages.wdl.{
  DxlStage,
  DxlWorkflowInput,
  TypeSerialization,
  WdlVarLinks,
  WdlVarLinksConverter
}
import dx.core.io.{DxPathConfig, ExecLinkInfo}
import dx.core.util.SysUtils
import dx.util.{JsUtils, Logger, getVersion}
import wdlTools.generators.code.WdlV1Generator

import scala.collection.immutable.TreeMap
import spray.json._
import wdlTools.eval.WdlValues
import wdlTools.types.WdlTypes

// The end result of the compiler
object Native {
  case class ExecRecord(callable: IR.Callable, dxExec: DxExecutable, links: Vector[ExecLinkInfo])
  case class Results(primaryCallable: Option[ExecRecord], execDict: Map[String, ExecRecord])
}

// An overall design principal here, is that the json requests
// have to be deterministic. This is because the checksums rely
// on that property.
case class Native(dxWDLrtId: Option[String],
                  folder: String,
                  dxProject: DxProject,
                  dxObjDir: DxObjectDirectory,
                  instanceTypeDB: InstanceTypeDB,
                  dxPathConfig: DxPathConfig,
                  fileInfoDir: Map[String, (DxFile, DxFileDescribe)],
                  typeAliases: Map[String, WdlTypes.T],
                  extras: Option[Extras],
                  runtimeDebugLevel: Option[Int],
                  leaveWorkflowsOpen: Boolean,
                  force: Boolean,
                  archive: Boolean,
                  locked: Boolean,
                  dxApi: DxApi) {
  private val logger2: Logger = dxApi.logger.withTraceIfContainsKey("Native")
  private val rtDebugLvl = runtimeDebugLevel.getOrElse(DEFAULT_RUNTIME_DEBUG_LEVEL)
  private val wdlVarLinksConverter = WdlVarLinksConverter(dxApi, fileInfoDir, typeAliases)
  private val streamAllFiles: Boolean = dxPathConfig.streamAllFiles
  private lazy val appCompileDirPath: Path = {
    val p = Paths.get("/tmp/dxWDL_Compile")
    SysUtils.safeMkdir(p)
    p
  }

  // Are we setting up a private docker registry?
  private val dockerRegistryInfo: Option[DockerRegistry] = extras match {
    case None => None
    case Some(extras) =>
      extras.dockerRegistry match {
        case None    => None
        case Some(x) => Some(x)
      }
  }

  lazy val runtimeLibrary: Option[JsValue] =
    dxWDLrtId match {
      case None     => None
      case Some(id) =>
        // Open the archive
        // Extract the archive from the details field
        val record = dxApi.record(id)
        val desc = record.describe(Set(Field.Details))
        val details = desc.details.get
        val dxLink = details.asJsObject.fields.get("archiveFileId") match {
          case Some(x) => x
          case None =>
            throw new Exception(
                s"record does not have an archive field ${details}"
            )
        }
        val dxFile = DxFile.fromJsValue(dxApi, dxLink)
        val name = dxFile.describe().name
        Some(
            JsObject(
                "name" -> JsString(name),
                "id" -> JsObject("$dnanexus_link" -> JsString(dxFile.id))
            )
        )
    }

  private def jsValueFromConstraint(constraint: IR.ConstraintRepr): JsValue = {
    constraint match {
      case IR.ConstraintReprString(s) => JsString(s)
      case IR.ConstraintReprOper(oper, constraints) =>
        val dxOper = oper match {
          case ConstraintOper.AND => DxConstraint.AND
          case ConstraintOper.OR  => DxConstraint.OR
          case _                  => throw new Exception(s"Invalid operation ${oper}")
        }
        JsObject(Map(dxOper -> JsArray(constraints.map(jsValueFromConstraint))))
    }
  }

  private def jsValueFromDefault(value: IR.DefaultRepr): JsValue = {
    value match {
      case IR.DefaultReprString(s)    => JsString(s)
      case IR.DefaultReprInteger(i)   => JsNumber(i)
      case IR.DefaultReprFloat(f)     => JsNumber(f)
      case IR.DefaultReprBoolean(b)   => JsBoolean(b)
      case IR.DefaultReprFile(f)      => dxApi.resolveDxUrlFile(f).getLinkAsJson
      case IR.DefaultReprArray(array) => JsArray(array.map(jsValueFromDefault))
    }
  }

  // For primitive types, and arrays of such types, we can map directly
  // to the equivalent dx types. For example,
  //   Int  -> int
  //   Array[String] -> array:string
  //
  // Arrays can be empty, which is why they are always marked "optional".
  // This notifies the platform runtime system not to throw an exception
  // for an empty input/output array.
  //
  // Ragged arrays, maps, and objects, cannot be mapped in such a trivial way.
  // These are called "Complex Types", or "Complex". They are handled
  // by passing a JSON structure and a vector of dx:files.
  private def cVarToSpec(cVar: CVar): Vector[JsValue] = {
    val name = cVar.dxVarName
    val attrs = cVar.attrs

    val defaultVals: Map[String, JsValue] = cVar.default match {
      case None => Map.empty
      case Some(wdlValue) =>
        val wvl = wdlVarLinksConverter.importFromWDL(cVar.wdlType, wdlValue)
        wdlVarLinksConverter.genFields(wvl, name).toMap
    }

    // Create the IO Attributes
    def jsMapFromAttrs(help: Option[Vector[IR.IOAttr]],
                       hasDefault: Boolean): Map[String, JsValue] = {
      help match {
        case None => Map.empty
        case Some(attributes) => {
          attributes.flatMap {
            case IR.IOAttrGroup(text) =>
              Some(DxIOSpec.GROUP -> JsString(text))
            case IR.IOAttrHelp(text) =>
              Some(DxIOSpec.HELP -> JsString(text))
            case IR.IOAttrLabel(text) =>
              Some(DxIOSpec.LABEL -> JsString(text))
            case IR.IOAttrPatterns(patternRepr) =>
              patternRepr match {
                case IR.PatternsReprArray(patterns) =>
                  Some(DxIOSpec.PATTERNS -> JsArray(patterns.map(JsString(_))))
                // If we have the alternative patterns object, extrac the values, if any at all
                case IR.PatternsReprObj(name, klass, tags) =>
                  val attrs: Map[String, JsValue] = List(
                      if (name.isDefined) Some("name" -> JsArray(name.get.map(JsString(_))))
                      else None,
                      if (tags.isDefined) Some("tag" -> JsArray(tags.get.map(JsString(_))))
                      else None,
                      if (klass.isDefined) Some("class" -> JsString(klass.get)) else None
                  ).flatten.toMap
                  // If all three keys for the object version of patterns are None, return None
                  if (attrs.isEmpty) None else Some(DxIOSpec.PATTERNS -> JsObject(attrs))
              }
            case IR.IOAttrChoices(choices) =>
              Some(DxIOSpec.CHOICES -> JsArray(choices.map {
                case IR.ChoiceReprString(value)  => JsString(value)
                case IR.ChoiceReprInteger(value) => JsNumber(value)
                case IR.ChoiceReprFloat(value)   => JsNumber(value)
                case IR.ChoiceReprBoolean(value) => JsBoolean(value)
                case IR.ChoiceReprFile(value, name) => {
                  // TODO: support project and record choices
                  val dxLink = dxApi.resolveDxUrlFile(value).getLinkAsJson
                  if (name.isDefined) {
                    JsObject(Map("name" -> JsString(name.get), "value" -> dxLink))
                  } else {
                    dxLink
                  }
                }
              }))
            case IR.IOAttrSuggestions(suggestions) =>
              Some(DxIOSpec.SUGGESTIONS -> JsArray(suggestions.map {
                case IR.SuggestionReprString(value)  => JsString(value)
                case IR.SuggestionReprInteger(value) => JsNumber(value)
                case IR.SuggestionReprFloat(value)   => JsNumber(value)
                case IR.SuggestionReprBoolean(value) => JsBoolean(value)
                case IR.SuggestionReprFile(value, name, project, path) => {
                  // TODO: support project and record suggestions
                  val dxLink: Option[JsValue] = value match {
                    case Some(str) => Some(dxApi.resolveDxUrlFile(str).getLinkAsJson)
                    case None      => None
                  }
                  if (name.isDefined || project.isDefined || path.isDefined) {
                    val attrs: Map[String, JsValue] = List(
                        if (dxLink.isDefined) Some("value" -> dxLink.get) else None,
                        if (name.isDefined) Some("name" -> JsString(name.get)) else None,
                        if (project.isDefined) Some("project" -> JsString(project.get)) else None,
                        if (path.isDefined) Some("path" -> JsString(path.get)) else None
                    ).flatten.toMap
                    JsObject(attrs)
                  } else if (dxLink.isDefined) {
                    dxLink.get
                  } else {
                    throw new Exception(
                        "Either 'value' or 'project' + 'path' must be defined for suggestions"
                    )
                  }
                }
              }))
            case IR.IOAttrType(constraint) =>
              Some(DxIOSpec.TYPE -> jsValueFromConstraint(constraint))
            case IR.IOAttrDefault(value) if !hasDefault =>
              // The default was specified in parameter_meta and was not specified in the
              // parameter declaration
              Some(DxIOSpec.DEFAULT -> jsValueFromDefault(value))
            case _ => None
          }.toMap
        }
      }
    }

    def jsMapFromDefault(name: String): Map[String, JsValue] = {
      defaultVals.get(name) match {
        case None      => Map.empty
        case Some(jsv) => Map("default" -> jsv)
      }
    }

    def jsMapFromOptional(optional: Boolean): Map[String, JsValue] = {
      if (optional) {
        Map("optional" -> JsBoolean(true))
      } else {
        Map.empty[String, JsValue]
      }
    }
    def mkPrimitive(dxType: String, optional: Boolean): Vector[JsValue] = {
      Vector(
          JsObject(
              Map("name" -> JsString(name), "class" -> JsString(dxType))
                ++ jsMapFromOptional(optional)
                ++ jsMapFromDefault(name)
                ++ jsMapFromAttrs(attrs, defaultVals.contains(name))
          )
      )
    }
    def mkPrimitiveArray(dxType: String, optional: Boolean): Vector[JsValue] = {
      Vector(
          JsObject(
              Map("name" -> JsString(name), "class" -> JsString("array:" ++ dxType))
                ++ jsMapFromOptional(optional)
                ++ jsMapFromDefault(name)
                ++ jsMapFromAttrs(attrs, defaultVals.contains(name))
          )
      )
    }

    def mkComplex(optional: Boolean): Vector[JsValue] = {
      // A large JSON structure passed as a hash, and a
      // vector of platform files.
      Vector(
          JsObject(
              Map("name" -> JsString(name), "class" -> JsString("hash"))
                ++ jsMapFromOptional(optional)
                ++ jsMapFromDefault(name)
                ++ jsMapFromAttrs(attrs, defaultVals.contains(name))
          ),
          JsObject(
              Map(
                  "name" -> JsString(name + WdlVarLinksConverter.FLAT_FILES_SUFFIX),
                  "class" -> JsString("array:file"),
                  "optional" -> JsBoolean(true)
              )
                ++ jsMapFromDefault(name + WdlVarLinksConverter.FLAT_FILES_SUFFIX)
                ++ jsMapFromAttrs(attrs, defaultVals.contains(name))
          )
      )
    }

    def handleType(wdlType: WdlTypes.T, optional: Boolean): Vector[JsValue] = {
      wdlType match {
        // primitive types
        case WdlTypes.T_Boolean => mkPrimitive("boolean", optional)
        case WdlTypes.T_Int     => mkPrimitive("int", optional)
        case WdlTypes.T_Float   => mkPrimitive("float", optional)
        case WdlTypes.T_String  => mkPrimitive("string", optional)
        case WdlTypes.T_File    => mkPrimitive("file", optional)

        // single dimension arrays of primitive types
        // non-empty array
        case WdlTypes.T_Array(WdlTypes.T_Boolean, true) => mkPrimitiveArray("boolean", optional)
        case WdlTypes.T_Array(WdlTypes.T_Int, true)     => mkPrimitiveArray("int", optional)
        case WdlTypes.T_Array(WdlTypes.T_Float, true)   => mkPrimitiveArray("float", optional)
        case WdlTypes.T_Array(WdlTypes.T_String, true)  => mkPrimitiveArray("string", optional)
        case WdlTypes.T_Array(WdlTypes.T_File, true)    => mkPrimitiveArray("file", optional)

        // array that may be empty
        case WdlTypes.T_Array(WdlTypes.T_Boolean, false) =>
          mkPrimitiveArray("boolean", optional = true)
        case WdlTypes.T_Array(WdlTypes.T_Int, false)   => mkPrimitiveArray("int", optional = true)
        case WdlTypes.T_Array(WdlTypes.T_Float, false) => mkPrimitiveArray("float", optional = true)
        case WdlTypes.T_Array(WdlTypes.T_String, false) =>
          mkPrimitiveArray("string", optional = true)
        case WdlTypes.T_Array(WdlTypes.T_File, false) => mkPrimitiveArray("file", optional = true)

        // complex type, that may contains files
        case _ => mkComplex(optional)
      }
    }
    cVar.wdlType match {
      case WdlTypes.T_Optional(t) => handleType(t, optional = true)
      case t                      => handleType(t, optional = false)
    }
  }

  // Create a bunch of bash export declarations, describing
  // the variables required to login to the docker private repository (if needed).
  private lazy val dockerPreamble: String = {
    dockerRegistryInfo match {
      case None                                                  => ""
      case Some(DockerRegistry(registry, username, credentials)) =>
        // check that the credentials file is a valid platform path
        try {
          val dxFile = dxApi.resolveDxUrlFile(credentials)
          dxApi.logger.ignore(dxFile)
        } catch {
          case e: Throwable =>
            throw new Exception(s"""|credentials has to point to a platform file.
                                    |It is now:
                                    |   ${credentials}
                                    |Error:
                                    |  ${e}
                                    |""".stripMargin)
        }

        // strip the URL from the dx:// prefix, so we can use dx-download directly
        val credentialsWithoutPrefix = credentials.substring(DxPath.DX_URL_PREFIX.length)
        s"""|
            |# if we need to set up a private docker registry,
            |# download the credentials file and login. Do not expose the
            |# credentials to the logs or to stdout.
            |
            |export DOCKER_REGISTRY=${registry}
            |export DOCKER_USERNAME=${username}
            |export DOCKER_CREDENTIALS=${credentialsWithoutPrefix}
            |
            |echo "Logging in to docker registry $${DOCKER_REGISTRY}, as user $${DOCKER_USERNAME}"
            |
            |# there has to be a single credentials file
            |num_lines=$$(dx ls $${DOCKER_CREDENTIALS} | wc --lines)
            |if [[ $$num_lines != 1 ]]; then
            |    echo "There has to be exactly one credentials file, found $$num_lines."
            |    dx ls -l $${DOCKER_CREDENTIALS}
            |    exit 1
            |fi
            |dx download $${DOCKER_CREDENTIALS} -o $${HOME}/docker_credentials
            |creds=$$(cat $${HOME}/docker_credentials)
            |
            |# Log into docker. Retry several times.
            |set -e
            |i=0
            |while [[ $$i -le 5 ]]; do
            |   echo $${creds} | docker login $${DOCKER_REGISTRY} -u $${DOCKER_USERNAME} --password-stdin
            |   rc=$$?
            |   if [[ $$rc == 0 ]]; then
            |      break
            |   fi
            |   i=$$((i + 1))
            |   sleep 3
            |   echo "retry docker login ($$i)"
            |done
            |set +e
            |
            |rm -f $${HOME}/docker_credentials
            |""".stripMargin
    }
  }

  private def genBashScriptTaskBody(): String = {
    s"""|    # evaluate input arguments, and download input files
        |    java -jar $${DX_FS_ROOT}/dxWDL.jar internal taskProlog $${HOME} ${rtDebugLvl.toString} ${streamAllFiles.toString}
        |
        |    echo "Using dxda version: $$(dx-download-agent version)"
        |    echo "Using dxfuse version: $$(dxfuse -version)"
        |
        |    # run the dx-download-agent (dxda) on a manifest of files
        |    if [[ -e ${dxPathConfig.dxdaManifest} ]]; then
        |       head -n 20 ${dxPathConfig.dxdaManifest}
        |       bzip2 ${dxPathConfig.dxdaManifest}
        |
        |       # run the download agent, and store the return code; do not exit on error.
        |       # we need to run it from the root directory, because it uses relative paths.
        |       cd /
        |       rc=0
        |       dx-download-agent download ${dxPathConfig.dxdaManifest}.bz2 || rc=$$? && true
        |
        |       # if there was an error during download, print out the download log
        |       if [[ $$rc != 0 ]]; then
        |           echo "download agent failed rc=$$rc"
        |           if [[ -e ${dxPathConfig.dxdaManifest}.bz2.download.log ]]; then
        |              echo "The download log is:"
        |              cat ${dxPathConfig.dxdaManifest}.bz2.download.log
        |           fi
        |           exit $$rc
        |       fi
        |
        |       # The download was ok, check file integrity on disk
        |       dx-download-agent inspect ${dxPathConfig.dxdaManifest.toString}.bz2
        |
        |       # go back to home directory
        |       cd ${dxPathConfig.homeDir.toString}
        |    fi
        |
        |    # run dxfuse on a manifest of files. It will provide remote access
        |    # to DNAx files.
        |    if [[ -e ${dxPathConfig.dxfuseManifest} ]]; then
        |       head -n 20 ${dxPathConfig.dxfuseManifest.toString}
        |
        |       # make sure the mountpoint exists
        |       mkdir -p ${dxPathConfig.dxfuseMountpoint.toString}
        |
        |       # don't leak the token to stdout. We need the DNAx token to be accessible
        |       # in the environment, so that dxfuse could get it.
        |       source environment >& /dev/null
        |
        |       dxfuse_version=$$(dxfuse -version)
        |       echo "dxfuse version $${dxfuse_version}}"
        |
        |       # run dxfuse so that it will not exit after the bash script exists.
        |       echo "mounting dxfuse on ${dxPathConfig.dxfuseMountpoint.toString}"
        |       dxfuse_log=/root/.dxfuse/dxfuse.log
        |
        |       dxfuse -readOnly ${dxPathConfig.dxfuseMountpoint.toString} ${dxPathConfig.dxfuseManifest.toString}
        |       dxfuse_err_code=$$?
        |       if [[ $$dxfuse_err_code != 0 ]]; then
        |           echo "error starting dxfuse, rc=$$dxfuse_err_code"
        |           if [[ -f $$dxfuse_log ]]; then
        |               cat $$dxfuse_log
        |           fi
        |           exit 1
        |       fi
        |
        |       # do we really need this?
        |       sleep 1
        |       cat $$dxfuse_log
        |       echo ""
        |       ls -Rl ${dxPathConfig.dxfuseMountpoint.toString}
        |    fi
        |
        |    # construct the bash command and write it to a file
        |    java -jar $${DX_FS_ROOT}/dxWDL.jar internal taskInstantiateCommand $${HOME} ${rtDebugLvl.toString} ${streamAllFiles.toString}
        |
        |    echo "bash command encapsulation script:"
        |    cat ${dxPathConfig.script.toString}
        |
        |    # Run the shell script generated by the prolog.
        |    # Capture the stderr/stdout in files
        |    if [[ -e ${dxPathConfig.dockerSubmitScript.toString} ]]; then
        |        echo "docker submit script:"
        |        cat ${dxPathConfig.dockerSubmitScript.toString}
        |        ${dxPathConfig.dockerSubmitScript.toString}
        |    else
        |        whoami
        |        /bin/bash ${dxPathConfig.script.toString}
        |    fi
        |
        |    #  check return code of the script
        |    rc=`cat ${dxPathConfig.rcPath}`
        |    if [[ $$rc != 0 ]]; then
        |        if [[ -f $$dxfuse_log ]]; then
        |            echo "=== dxfuse filesystem log === "
        |            cat $$dxfuse_log
        |        fi
        |        exit $$rc
        |    fi
        |
        |    # evaluate applet outputs, and upload result files
        |    java -jar $${DX_FS_ROOT}/dxWDL.jar internal taskEpilog $${HOME} ${rtDebugLvl.toString} ${streamAllFiles.toString}
        |
        |    # unmount dxfuse
        |    if [[ -e ${dxPathConfig.dxfuseManifest} ]]; then
        |        echo "unmounting dxfuse"
        |        sudo umount ${dxPathConfig.dxfuseMountpoint}
        |    fi
        |""".stripMargin.trim
  }

  private def genBashScriptWfFragment(): String = {
    s"""|main() {
        |    java -jar $${DX_FS_ROOT}/dxWDL.jar internal wfFragment $${HOME} ${rtDebugLvl.toString} ${streamAllFiles.toString}
        |}
        |
        |collect() {
        |    java -jar $${DX_FS_ROOT}/dxWDL.jar internal collect $${HOME} ${rtDebugLvl.toString} ${streamAllFiles.toString}
        |}""".stripMargin.trim
  }

  private def genBashScriptCmd(cmd: String): String = {
    s"""|main() {
        |    java -jar $${DX_FS_ROOT}/dxWDL.jar internal ${cmd} $${HOME} ${rtDebugLvl.toString} ${streamAllFiles.toString}
        |}""".stripMargin.trim
  }

  private def genBashScript(applet: IR.Applet, instanceType: IR.InstanceType): String = {
    val body: String = applet.kind match {
      case IR.AppletKindNative(_) =>
        throw new Exception("Sanity: generating a bash script for a native applet")
      case IR.AppletKindWorkflowCustomReorg(_) =>
        throw new Exception("Sanity: generating a bash script for a custom reorg applet")
      case IR.AppletKindWfFragment(_, _, _) =>
        genBashScriptWfFragment()
      case IR.AppletKindWfInputs =>
        genBashScriptCmd("wfInputs")
      case IR.AppletKindWfOutputs =>
        genBashScriptCmd("wfOutputs")
      case IR.AppletKindWfCustomReorgOutputs =>
        genBashScriptCmd("wfCustomReorgOutputs")
      case IR.AppletKindWorkflowOutputReorg =>
        genBashScriptCmd("workflowOutputReorg")
      case IR.AppletKindTask(_) =>
        instanceType match {
          case IR.InstanceTypeDefault | IR.InstanceTypeConst(_, _, _, _, _) =>
            s"""|${dockerPreamble}
                |
                |set -e -o pipefail -x
                |main() {
                |${genBashScriptTaskBody()}
                |}""".stripMargin
          case IR.InstanceTypeRuntime =>
            s"""|${dockerPreamble}
                |
                |set -e -o pipefail
                |main() {
                |    # check if this is the correct instance type
                |    correctInstanceType=`java -jar $${DX_FS_ROOT}/dxWDL.jar internal taskCheckInstanceType $${HOME} ${rtDebugLvl.toString} ${streamAllFiles.toString}`
                |    if [[ $$correctInstanceType == "true" ]]; then
                |        body
                |    else
                |       # evaluate the instance type, and launch a sub job on it
                |       java -jar $${DX_FS_ROOT}/dxWDL.jar internal taskRelaunch $${HOME} ${rtDebugLvl.toString} ${streamAllFiles.toString}
                |    fi
                |}
                |
                |# We are on the correct instance type, run the task
                |body() {
                |${genBashScriptTaskBody()}
                |}""".stripMargin.trim
        }
    }

    s"""|#!/bin/bash -ex
        |
        |${body}""".stripMargin
  }

  // Calculate the MD5 checksum of a string
  private def chksum(s: String): String = {
    val digest = MessageDigest.getInstance("MD5").digest(s.getBytes)
    digest.map("%02X" format _).mkString
  }

  // Add a checksum to a request
  private def checksumReq(name: String,
                          fields: Map[String, JsValue]): (String, Map[String, JsValue]) = {
    logger2.trace(
        s"""|${name} -> checksum request
            |fields = ${JsObject(fields).prettyPrint}
            |
            |""".stripMargin
    )

    // We need to sort the hash-tables. They are natually unsorted,
    // causing the same object to have different checksums.
    val jsDet = JsUtils.makeDeterministic(JsObject(fields))
    val digest = chksum(jsDet.prettyPrint)

    // Add the checksum to the properies
    val preExistingProps: Map[String, JsValue] =
      fields.get("properties") match {
        case Some(JsObject(props)) => props
        case None                  => Map.empty
        case other                 => throw new Exception(s"Bad properties json value ${other}")
      }
    val props = preExistingProps ++ Map(
        VERSION_PROP -> JsString(getVersion),
        CHECKSUM_PROP -> JsString(digest)
    )

    // Add properties and attributes we don't want to fall under the checksum
    // This allows, for example, moving the dx:executable, while
    // still being able to reuse it.
    val req = fields ++ Map(
        "project" -> JsString(dxProject.id),
        "folder" -> JsString(folder),
        "parents" -> JsBoolean(true),
        "properties" -> JsObject(props)
    )
    (digest, req)
  }

  // Do we need to build this applet/workflow?
  //
  // Returns:
  //   None: build is required
  //   Some(dxobject) : the right object is already on the platform
  private def isBuildRequired(name: String, digest: String): Option[DxDataObject] = {
    // Have we built this applet already, but placed it elsewhere in the project?
    dxObjDir.lookupOtherVersions(name, digest) match {
      case None => ()
      case Some((dxObj, desc)) =>
        desc match {
          case _: DxAppDescribe =>
            dxApi.logger.trace(s"Found existing version of app ${name}")
          case apl: DxAppletDescribe =>
            dxApi.logger.trace(s"Found existing version of applet ${name} in folder ${apl.folder}")
          case wf: DxWorkflowDescribe =>
            dxApi.logger.trace(s"Found existing version of workflow ${name} in folder ${wf.folder}")
          case other =>
            throw new Exception(s"bad object ${other}")
        }
        return Some(dxObj)
    }

    val existingDxObjs = dxObjDir.lookup(name)
    val buildRequired: Boolean = existingDxObjs.size match {
      case 0 => true
      case 1 =>
        // Check if applet code has changed
        val dxObjInfo = existingDxObjs.head
        dxObjInfo.digest match {
          case None =>
            throw new Exception(s"There is an existing non-dxWDL applet ${name}")
          case Some(digest2) if digest != digest2 =>
            dxApi.logger.trace(s"${dxObjInfo.dxClass} ${name} has changed, rebuild required")
            true
          case Some(_) =>
            dxApi.logger.trace(s"${dxObjInfo.dxClass} ${name} has not changed")
            false
        }
      case _ =>
        val dxClass = existingDxObjs.head.dxClass
        dxApi.logger.warning(s"""|More than one ${dxClass} ${name} found in
                                 |path ${dxProject.id}:${folder}""".stripMargin)
        true
    }

    if (buildRequired) {
      if (existingDxObjs.nonEmpty) {
        if (archive) {
          // archive the applet/workflow(s)
          existingDxObjs.foreach(x => dxObjDir.archiveDxObject(x))
        } else if (force) {
          // the dx:object exists, and needs to be removed. There
          // may be several versions, all are removed.
          val objs = existingDxObjs.map(_.dxObj)
          dxApi.logger.trace(s"Removing old ${name} ${objs.map(_.id)}")
          dxProject.removeObjects(objs)
        } else {
          val dxClass = existingDxObjs.head.dxClass
          throw new Exception(s"""|${dxClass} ${name} already exists in
                                  | ${dxProject.id}:${folder}""".stripMargin)
        }
      }
      None
    } else {
      assert(existingDxObjs.size == 1)
      Some(existingDxObjs.head.dxObj)
    }
  }

  // Create linking information for a dx:executable
  private def genLinkInfo(irCall: IR.Callable, dxObj: DxExecutable): ExecLinkInfo = {
    val callInputDefs: Map[String, WdlTypes.T] = irCall.inputVars.map {
      case CVar(name, wdlType, _, _) => name -> wdlType
    }.toMap
    val callOutputDefs: Map[String, WdlTypes.T] = irCall.outputVars.map {
      case CVar(name, wdlType, _, _) => name -> wdlType
    }.toMap
    ExecLinkInfo(irCall.name, callInputDefs, callOutputDefs, dxObj)
  }

  private def apiParseReplyID(repJs: JsObject): String = {
    repJs.asJsObject.fields.get("id") match {
      case None              => throw new Exception("API call did not returnd an ID")
      case Some(JsString(x)) => x
      case other             => throw new Exception(s"API call returned invalid ID ${other}")
    }
  }

  // Match everything up to the first period; truncate after 50 characters.
  private lazy val firstLineRegex = "^([^.]{1,50}).*".r

  private def calcSummary(description: Option[JsValue],
                          summary: Option[JsValue]): Map[String, JsValue] = {
    summary match {
      case Some(JsString(text)) if text.length() > 0 => Map("summary" -> summary.get)
      case _ =>
        description match {
          case Some(JsString(text)) if text.length() > 0 =>
            text match {
              case firstLineRegex(line) =>
                val descSummary = if (line.length() == 50 && !line.endsWith(".")) {
                  line + "..."
                } else {
                  line
                }
                Map("summary" -> JsString(descSummary))
              case _ => Map.empty
            }
          case _ => Map.empty
        }
    }
  }

  // If whatsNew is in array format, convert it to a string
  private def calcWhatsNew(whatsNew: Option[JsValue]): Map[String, JsValue] = {
    whatsNew match {
      case Some(JsArray(array)) =>
        val changelog = array
          .map {
            case JsObject(fields) =>
              val formattedFields = fields
                .map {
                  case ("version", JsString(value)) => Some("version" -> value)
                  case ("changes", JsArray(array)) =>
                    Some("changes",
                         array
                           .map {
                             case JsString(item) => s"* ${item}"
                             case other =>
                               throw new Exception(s"Invalid change list item: ${other}")
                           }
                           .mkString("\n"))
                  case _ => None
                }
                .flatten
                .toMap
              s"### Version ${formattedFields("version")}\n${formattedFields("changes")}"
            case other => throw new Exception(s"Invalid whatsNew ${other}")
          }
          .mkString("\n")
        Map("whatsNew" -> JsString(s"## Changelog\n${changelog}"))
      case _ => Map.empty
    }
  }

  private def metaValueToJs(value: IR.MetaValue): JsValue = {
    value match {
      case IR.MetaValueNull           => JsNull
      case IR.MetaValueBoolean(b)     => JsBoolean(b)
      case IR.MetaValueInt(i)         => JsNumber(i)
      case IR.MetaValueFloat(f)       => JsNumber(f)
      case IR.MetaValueString(text)   => JsString(text)
      case IR.MetaValueObject(fields) => JsObject(fields.view.mapValues(metaValueToJs).toMap)
      case IR.MetaValueArray(array)   => JsArray(array.map(metaValueToJs))
    }
  }

  private def anyToJs(value: Any): JsValue = {
    value match {
      case s: String    => JsString(s)
      case i: Int       => JsNumber(i)
      case f: Double    => JsNumber(f)
      case b: Boolean   => JsBoolean(b)
      case a: Vector[_] => JsArray(a.map(anyToJs))
      case m: Map[_, _] => JsObject(m.asInstanceOf[Map[String, Any]].view.mapValues(anyToJs).toMap)
      case other        => throw new EOFException(s"Unsupported value ${other}")
    }
  }

  // Convert the applet meta to JS, and overlay details from task-specific extras
  private def buildTaskMetadata(
      applet: IR.Applet,
      defaultTags: Vector[JsString]
  ): (Map[String, JsValue], Map[String, JsValue]) = {
    val metaDefaults = Map(
        "title" -> JsString(applet.name),
        "tags" -> JsArray(defaultTags)
        // These are currently ignored because they only apply to apps
        //"version" -> JsString("0.0.1"),
        //"openSource" -> JsBoolean(false),
    )

    var meta: Map[String, JsValue] = applet.meta match {
      case Some(appAttrs) =>
        appAttrs.flatMap {
          case IR.TaskAttrTitle(text)          => Some("title" -> JsString(text))
          case IR.TaskAttrDescription(text)    => Some("description" -> JsString(text))
          case IR.TaskAttrSummary(text)        => Some("summary" -> JsString(text))
          case IR.TaskAttrDeveloperNotes(text) => Some("developerNotes" -> JsString(text))
          case IR.TaskAttrTypes(array)         => Some("types" -> JsArray(array.map(anyToJs)))
          case IR.TaskAttrTags(array)          =>
            // merge default and user-specified tags
            Some("tags" -> JsArray((array.map(anyToJs).toSet ++ defaultTags.toSet).toVector))
          case IR.TaskAttrProperties(props) =>
            Some("properties" -> JsObject(props.view.mapValues(anyToJs).toMap))
          case IR.TaskAttrDetails(details) =>
            Some("details" -> JsObject(details.view.mapValues(metaValueToJs).toMap))
          // These are currently ignored because they only apply to apps
          //case IR.TaskAttrVersion(text) => Some("version" -> JsString(text))
          //case IR.TaskAttrOpenSource(isOpenSource) =>
          //  Some("openSource" -> JsBoolean(isOpenSource))
          //case IR.TaskAttrCategories(categories) =>
          //  Some("categories" -> categories.mapValues(anyToJs))
          case _ => None
        }.toMap
      case None => Map.empty
    }

    // Default 'summary' to be the first line of 'description'
    val summary = calcSummary(meta.get("description"), meta.get("summary"))

    val metaDetails: Map[String, JsValue] = meta.get("details") match {
      case Some(JsObject(fields)) =>
        meta -= "details"
        fields
      case _ => Map.empty
    }

    // If whatsNew is in array format, convert it to a string
    val whatsNew: Map[String, JsValue] = calcWhatsNew(metaDetails.get("whatsNew"))

    // Default and WDL-specified details can be overridden in task-specific extras
    val taskSpecificDetails: Map[String, JsValue] =
      if (applet.kind.isInstanceOf[IR.AppletKindTask] && extras.isDefined) {
        extras.get.perTaskDxAttributes.get(applet.name) match {
          case None      => Map.empty
          case Some(dta) => dta.getDetailsJson
        }
      } else {
        Map.empty
      }

    (metaDefaults ++ meta ++ summary, metaDetails ++ whatsNew ++ taskSpecificDetails)
  }

  // Set the run spec.
  //
  private def calcRunSpec(applet: IR.Applet,
                          bashScript: String): (JsValue, Map[String, JsValue]) = {
    // find the dxWDL asset
    val instanceType: String = applet.instanceType match {
      case x: IR.InstanceTypeConst =>
        val xDesc = InstanceTypeReq(x.dxInstanceType, x.memoryMB, x.diskGB, x.cpu, x.gpu)
        instanceTypeDB.apply(xDesc)
      case IR.InstanceTypeDefault | IR.InstanceTypeRuntime =>
        instanceTypeDB.defaultInstanceType
    }
    val runSpec: Map[String, JsValue] = Map(
        "code" -> JsString(bashScript),
        "interpreter" -> JsString("bash"),
        "systemRequirements" ->
          JsObject(
              "main" ->
                JsObject("instanceType" -> JsString(instanceType))
          ),
        "distribution" -> JsString("Ubuntu"),
        "release" -> JsString(DEFAULT_UBUNTU_VERSION)
    )

    // Add default timeout
    val defaultTimeout: Map[String, JsValue] =
      DxRunSpec(
          None,
          None,
          None,
          Some(DxTimeout(Some(DEFAULT_APPLET_TIMEOUT_IN_DAYS), Some(0), Some(0)))
      ).toRunSpecJson

    // Start with the default dx-attribute section, and override
    // any field that is specified in the runtime hints or the individual task section.
    val extraRunSpec: Map[String, JsValue] = extras match {
      case None => Map.empty
      case Some(ext) =>
        ext.defaultTaskDxAttributes match {
          case None      => Map.empty
          case Some(dta) => dta.getRunSpecJson
        }
    }

    val hintRunspec: Map[String, JsValue] = applet.runtimeHints match {
      case Some(hints) =>
        hints
          .flatMap {
            case IR.RuntimeHintRestart(max, default, errors) =>
              val defaultMap: Option[Map[String, Int]] = default match {
                case Some(i) => Some(Map("*" -> i))
                case _       => None
              }
              Some(
                  DxExecPolicy(
                      (errors, defaultMap) match {
                        case (Some(e), Some(d)) => Some(e ++ d)
                        case (Some(e), None)    => Some(e)
                        case (None, Some(d))    => Some(d)
                        case _                  => None
                      },
                      max
                  ).toJson
              )
            case IR.RuntimeHintTimeout(days, hours, minutes) =>
              Some(
                  DxTimeout(days.orElse(Some(0)), hours.orElse(Some(0)), minutes.orElse(Some(0))).toJson
              )
            case _ => None
          }
          .reduceOption(_ ++ _)
          .getOrElse(Map.empty)
      case _ => Map.empty
    }

    val taskSpecificRunSpec: Map[String, JsValue] =
      if (applet.kind.isInstanceOf[IR.AppletKindTask]) {
        // A task can override the default dx attributes
        extras match {
          case None => Map.empty
          case Some(ext) =>
            ext.perTaskDxAttributes.get(applet.name) match {
              case None      => Map.empty
              case Some(dta) => dta.getRunSpecJson
            }
        }
      } else {
        Map.empty
      }

    val runSpecWithExtras = runSpec ++ defaultTimeout ++ extraRunSpec ++ hintRunspec ++ taskSpecificRunSpec

    // - If the docker image is a tarball, add a link in the details field.
    val dockerFile: Option[DxFile] = applet.docker match {
      case IR.DockerImageNone              => None
      case IR.DockerImageNetwork           => None
      case IR.DockerImageDxFile(_, dxfile) =>
        // A docker image stored as a tar ball in a platform file
        Some(dxfile)
    }
    val bundledDepends = runtimeLibrary match {
      case None        => Map.empty
      case Some(rtLib) => Map("bundledDepends" -> JsArray(Vector(rtLib)))
    }
    val runSpecEverything = JsObject(runSpecWithExtras ++ bundledDepends)

    val details: Map[String, JsValue] = dockerFile match {
      case None         => Map.empty
      case Some(dxfile) => Map("docker-image" -> DxFile.toJsValue(dxfile))
    }

    (runSpecEverything, details)
  }

  def calcAccess(applet: IR.Applet): JsValue = {
    val extraAccess: DxAccess = extras match {
      case None      => DxAccess.empty
      case Some(ext) => ext.getDefaultAccess
    }

    val hintAccess: DxAccess = applet.runtimeHints match {
      case Some(hints) =>
        hints
          .flatMap {
            case IR.RuntimeHintAccess(network, project, allProjects, developer, projectCreation) =>
              Some(
                  DxAccess(network,
                           project.map(DxAccessLevel.withName),
                           allProjects.map(DxAccessLevel.withName),
                           developer,
                           projectCreation)
              )
            case _ => None
          }
          .headOption
          .getOrElse(DxAccess.empty)
      case _ => DxAccess.empty
    }

    val taskSpecificAccess: DxAccess =
      if (applet.kind.isInstanceOf[IR.AppletKindTask]) {
        // A task can override the default dx attributes
        extras match {
          case None      => DxAccess.empty
          case Some(ext) => ext.getTaskAccess(applet.name)
        }
      } else {
        DxAccess.empty
      }

    // If we are using a private docker registry, add the allProjects: VIEW
    // access to tasks.
    val allProjectsAccess: DxAccess = dockerRegistryInfo match {
      case None    => DxAccess.empty
      case Some(_) => DxAccess(None, None, Some(DxAccessLevel.View), None, None)
    }

    val taskAccess = extraAccess
      .merge(hintAccess)
      .merge(taskSpecificAccess)
      .merge(allProjectsAccess)

    val access: DxAccess = applet.kind match {
      case IR.AppletKindTask(_) =>
        if (applet.docker == IR.DockerImageNetwork) {
          // docker requires network access, because we are downloading
          // the image from the network
          taskAccess.merge(DxAccess(Some(Vector("*")), None, None, None, None))
        } else {
          taskAccess
        }
      case IR.AppletKindWorkflowOutputReorg =>
        // The WorkflowOutput applet requires higher permissions
        // to organize the output directory.
        DxAccess(None, Some(DxAccessLevel.Contribute), None, None, None)
      case _ =>
        // Even scatters need network access, because
        // they spawn subjobs that (may) use dx-docker.
        // We end up allowing all applets to use the network
        taskAccess.merge(DxAccess(Some(Vector("*")), None, None, None, None))
    }
    val fields = access.toJson
    if (fields.isEmpty) JsNull
    else JsObject(fields)
  }

  // Build an '/applet/new' request
  //
  // For applets that call other applets, we pass a directory
  // of the callees, so they could be found a runtime. This is
  // equivalent to linking, in a standard C compiler.
  private def appletNewReq(applet: IR.Applet,
                           bashScript: String,
                           aplLinks: Map[String, ExecLinkInfo]): (String, Map[String, JsValue]) = {
    logger2.trace(s"Building /applet/new request for ${applet.name}")

    val inputSpec: Vector[JsValue] = applet.inputs
      .sortWith(_.name < _.name)
      .flatMap(cVar => cVarToSpec(cVar))

    // create linking information
    val linkInfo: Map[String, JsValue] =
      aplLinks.map {
        case (name, ali) =>
          name -> ExecLinkInfo.writeJson(ali, typeAliases)
      }

    val metaInfo: Map[String, JsValue] =
      applet.kind match {
        case IR.AppletKindWfFragment(_, blockPath, fqnDictTypes) =>
          // meta information used for running workflow fragments
          Map(
              "execLinkInfo" -> JsObject(linkInfo),
              "blockPath" -> JsArray(blockPath.map(JsNumber(_))),
              "fqnDictTypes" -> JsObject(fqnDictTypes.map {
                case (k, t) =>
                  val tStr = TypeSerialization(typeAliases).toString(t)
                  k -> JsString(tStr)
              })
          )

        case IR.AppletKindWfInputs | IR.AppletKindWfOutputs | IR.AppletKindWfCustomReorgOutputs |
            IR.AppletKindWorkflowOutputReorg =>
          // meta information used for running workflow fragments
          val fqnDictTypes = JsObject(applet.inputVars.map { cVar =>
            val tStr = TypeSerialization(typeAliases).toString(cVar.wdlType)
            cVar.name -> JsString(tStr)
          }.toMap)
          Map("fqnDictTypes" -> fqnDictTypes)

        case _ =>
          Map.empty
      }

    val outputSpec: Vector[JsValue] = applet.outputs
      .sortWith(_.name < _.name)
      .flatMap(cVar => cVarToSpec(cVar))

    // put the WDL source code into the details field.
    // Add the pricing model, and make the prices opaque.
    val generator = WdlV1Generator()
    val sourceLines = generator.generateDocument(applet.document)
    val sourceCode = SysUtils.gzipAndBase64Encode(sourceLines.mkString("\n"))
    val dbOpaque = InstanceTypeDB.opaquePrices(instanceTypeDB)
    val dbOpaqueInstance = SysUtils.gzipAndBase64Encode(dbOpaque.toJson.prettyPrint)
    val runtimeAttrs = extras match {
      case None      => JsNull
      case Some(ext) => ext.defaultRuntimeAttributes.toJson
    }

    val defaultTags = Vector(JsString("dxWDL"))
    val (taskMeta, taskDetails) = buildTaskMetadata(applet, defaultTags)

    // Compute all the bits that get merged together into 'details'
    val auxInfo = Map("wdlSourceCode" -> JsString(sourceCode),
                      "instanceTypeDB" -> JsString(dbOpaqueInstance),
                      "runtimeAttrs" -> runtimeAttrs)

    // Links to applets that could get called at runtime. If
    // this applet is copied, we need to maintain referential integrity.
    val dxLinks = aplLinks.map {
      case (name, execLinkInfo) =>
        ("link_" + name) -> JsObject("$dnanexus_link" -> JsString(execLinkInfo.dxExec.getId))
    }

    val (runSpec: JsValue, runSpecDetails: Map[String, JsValue]) =
      calcRunSpec(applet, bashScript)

    val delayWD: Map[String, JsValue] = extras match {
      case None => Map.empty
      case Some(ext) =>
        ext.delayWorkspaceDestruction match {
          case Some(true) => Map("delayWorkspaceDestruction" -> JsTrue)
          case _          => Map.empty
        }
    }

    // merge all the separate details maps
    val details: Map[String, JsValue] =
      taskDetails ++ auxInfo ++ dxLinks ++ metaInfo ++ runSpecDetails ++ delayWD

    val access: JsValue = calcAccess(applet)

    // A fragemnt is hidden, not visible under default settings. This
    // allows the workflow copying code to traverse it, and link to
    // anything it calls.
    val hidden: Boolean =
      applet.kind match {
        case _: IR.AppletKindWfFragment => true
        case _                          => false
      }

    // pack all the core arguments into a single request
    val reqCore = Map(
        "name" -> JsString(applet.name),
        "inputSpec" -> JsArray(inputSpec),
        "outputSpec" -> JsArray(outputSpec),
        "runSpec" -> runSpec,
        "dxapi" -> JsString("1.0.0"),
        "details" -> JsObject(details),
        "hidden" -> JsBoolean(hidden)
    )
    // look for ignoreReuse in runtime hints and in extras - the later overrides the former
    val ignoreReuseHint: Map[String, JsValue] = applet.runtimeHints match {
      case Some(hints) =>
        hints
          .flatMap {
            case IR.RuntimeHintIgnoreReuse(flag) => Some(Map("ignoreReuse" -> JsBoolean(flag)))
            case _                               => None
          }
          .headOption
          .getOrElse(Map.empty)
      case _ => Map.empty
    }
    val ignoreReuseExtras: Map[String, JsValue] = extras match {
      case None => Map.empty
      case Some(ext) =>
        ext.ignoreReuse match {
          case None       => Map.empty
          case Some(flag) => Map("ignoreReuse" -> JsBoolean(flag))
        }
    }
    val accessField =
      if (access == JsNull) Map.empty
      else Map("access" -> access)

    // Add a checksum
    val reqCoreAll = taskMeta ++ reqCore ++ accessField ++ ignoreReuseHint ++ ignoreReuseExtras
    checksumReq(applet.name, reqCoreAll)
  }

  // Rebuild the applet if needed.
  //
  // When [force] is true, always rebuild. Otherwise, rebuild only
  // if the WDL code has changed.
  private def buildAppletIfNeeded(
      applet: IR.Applet,
      execDict: Map[String, Native.ExecRecord]
  ): (DxApplet, Vector[ExecLinkInfo]) = {
    logger2.trace(s"Compiling applet ${applet.name}")

    // limit the applet dictionary, only to actual dependencies
    val calls: Vector[String] = applet.kind match {
      case IR.AppletKindWfFragment(calls, _, _) => calls
      case _                                    => Vector.empty
    }

    val aplLinks: Map[String, ExecLinkInfo] = calls.map { tName =>
      val Native.ExecRecord(irCall, dxObj, _) = execDict(tName)
      tName -> genLinkInfo(irCall, dxObj)
    }.toMap

    // Build an applet script
    val bashScript = genBashScript(applet, applet.instanceType)

    // Calculate a checksum of the inputs that went into the
    // making of the applet.
    val (digest, appletApiRequest) = appletNewReq(applet, bashScript, aplLinks)
    if (logger2.verbose) {
      val fName = s"${applet.name}_req.json"
      val trgPath = appCompileDirPath.resolve(fName)
      SysUtils.writeFileContent(trgPath, JsObject(appletApiRequest).prettyPrint)
    }

    val buildRequired = isBuildRequired(applet.name, digest)
    val dxApplet = buildRequired match {
      case None =>
        // Compile a WDL snippet into an applet.
        val rep = dxApi.appletNew(appletApiRequest)
        val id = apiParseReplyID(rep)
        val dxApplet = dxApi.applet(id)
        dxObjDir.insert(applet.name, dxApplet, digest)
        dxApplet
      case Some(dxObj) =>
        // Old applet exists, and it has not changed. Return the
        // applet-id.
        dxObj.asInstanceOf[DxApplet]
    }
    (dxApplet, aplLinks.values.toVector)
  }

  // Calculate the stage inputs from the call closure
  //
  // It comprises mappings from variable name to WdlTypes.T.
  private def genStageInputs(inputs: Vector[(CVar, SArg)]): JsValue = {
    // sort the inputs, to make the request deterministic
    val jsInputs: TreeMap[String, JsValue] = inputs.foldLeft(TreeMap.empty[String, JsValue]) {
      case (m, (cVar, sArg)) =>
        sArg match {
          case IR.SArgEmpty =>
            // We do not have a value for this input at compile time.
            // For compulsory applet inputs, the user will have to fill
            // in a value at runtime.
            m
          case IR.SArgConst(wValue) =>
            val wvl = wdlVarLinksConverter.importFromWDL(cVar.wdlType, wValue)
            val fields = wdlVarLinksConverter.genFields(wvl, cVar.dxVarName)
            m ++ fields.toMap
          case IR.SArgLink(dxStage, argName) =>
            val wvl = WdlVarLinks(cVar.wdlType, DxlStage(dxStage, IORef.Output, argName.dxVarName))
            val fields = wdlVarLinksConverter.genFields(wvl, cVar.dxVarName)
            m ++ fields.toMap
          case IR.SArgWorkflowInput(argName) =>
            val wvl = WdlVarLinks(cVar.wdlType, DxlWorkflowInput(argName.dxVarName))
            val fields = wdlVarLinksConverter.genFields(wvl, cVar.dxVarName)
            m ++ fields.toMap
        }
    }
    JsObject(jsInputs)
  }

  // construct the workflow input DNAx types and defaults.
  //
  // A WDL input can generate one or two DNAx inputs.  This requires
  // creating a vector of JSON values from each input.
  //
  private def buildWorkflowInputSpec(cVar: CVar, sArg: SArg): Vector[JsValue] = {
    // deal with default values
    val sArgDefault: Option[WdlValues.V] = sArg match {
      case IR.SArgConst(wdlValue) =>
        Some(wdlValue)
      case _ =>
        None
    }

    // The default value can come from the SArg, or the CVar
    val default = (sArgDefault, cVar.default) match {
      case (Some(x), _) => Some(x)
      case (_, Some(x)) => Some(x)
      case _            => None
    }

    val cVarWithDflt = cVar.copy(default = default)
    cVarToSpec(cVarWithDflt)
  }

  // Note: a single WDL output can generate one or two JSON outputs.
  private def buildWorkflowOutputSpec(cVar: CVar, sArg: SArg): Vector[JsValue] = {
    val oSpec: Vector[JsValue] = cVarToSpec(cVar)

    // add the field names, to help index this structure
    val oSpecMap: Map[String, JsValue] = oSpec.map { jso =>
      val nm = jso.asJsObject.fields.get("name") match {
        case Some(JsString(nm)) => nm
        case _                  => throw new Exception("sanity")
      }
      nm -> jso
    }.toMap

    val outputSources: List[(String, JsValue)] = sArg match {
      case IR.SArgConst(wdlValue) =>
        // constant
        throw new Exception(
            s"Constant workflow outputs not currently handled (${cVar}, ${sArg}, ${wdlValue})"
        )
      case IR.SArgLink(dxStage, argName: CVar) =>
        val wvl = WdlVarLinks(cVar.wdlType, DxlStage(dxStage, IORef.Output, argName.dxVarName))
        wdlVarLinksConverter.genFields(wvl, cVar.dxVarName)
      case IR.SArgWorkflowInput(argName: CVar) =>
        val wvl = WdlVarLinks(cVar.wdlType, DxlWorkflowInput(argName.dxVarName))
        wdlVarLinksConverter.genFields(wvl, cVar.dxVarName)
      case other =>
        throw new Exception(s"Bad value for sArg ${other}")
    }

    // merge the specification and the output sources
    outputSources.map {
      case (fieldName, outputJs) =>
        val specJs: JsValue = oSpecMap(fieldName)
        JsObject(
            specJs.asJsObject.fields ++
              Map("outputSource" -> outputJs)
        )
    }.toVector
  }

  private def buildWorkflowMetadata(
      wf: IR.Workflow,
      defaultTags: Vector[JsString]
  ): (Map[String, JsValue], Map[String, JsValue]) = {
    val metaDefaults = Map(
        "title" -> JsString(wf.name),
        "tags" -> JsArray(defaultTags)
        //"version" -> JsString("0.0.1")
    )

    var meta: Map[String, JsValue] = wf.meta match {
      case Some(appAttrs) =>
        appAttrs.flatMap {
          case IR.WorkflowAttrTitle(text)       => Some("title" -> JsString(text))
          case IR.WorkflowAttrDescription(text) => Some("description" -> JsString(text))
          case IR.WorkflowAttrSummary(text)     => Some("summary" -> JsString(text))
          case IR.WorkflowAttrTypes(array)      => Some("types" -> JsArray(array.map(anyToJs)))
          case IR.WorkflowAttrTags(array) =>
            Some("tags" -> JsArray((array.map(anyToJs).toSet ++ defaultTags.toSet).toVector))
          case IR.WorkflowAttrProperties(props) =>
            Some("properties" -> JsObject(props.view.mapValues(anyToJs).toMap))
          case IR.WorkflowAttrDetails(details) =>
            Some("details" -> JsObject(details.view.mapValues(metaValueToJs).toMap))
          // These are currently ignored because they only apply to apps
          //case IR.WorkflowAttrVersion(text) => Some("version" -> JsString(text))
          // These will be implemented in a future PR
          case IR.WorkflowAttrCallNames(_)       => None
          case IR.WorkflowAttrRunOnSingleNode(_) => None
          case _                                 => None
        }.toMap
      case None => Map.empty
    }

    // Default 'summary' to be the first line of 'description'
    val summary = calcSummary(meta.get("description"), meta.get("summary"))

    val metaDetails: Map[String, JsValue] = meta.get("details") match {
      case Some(JsObject(fields)) =>
        meta -= "details"
        fields
      case _ => Map.empty
    }

    // If whatsNew is in array format, convert it to a string
    val whatsNew: Map[String, JsValue] = calcWhatsNew(metaDetails.get("whatsNew"))

    (metaDefaults ++ meta ++ summary, metaDetails ++ whatsNew)
  }

  // Create a request for a workflow encapsulated in single API call.
  // Prepare the list of stages, and the checksum in advance.
  private def workflowNewReq(
      wf: IR.Workflow,
      execDict: Map[String, Native.ExecRecord]
  ): (String, Map[String, JsValue]) = {
    logger2.trace(s"build workflow ${wf.name}")

    val stagesReq =
      wf.stages.foldLeft(Vector.empty[JsValue]) {
        case (stagesReq, stg) =>
          val Native.ExecRecord(irApplet, dxExec, _) = execDict(stg.calleeName)
          val linkedInputs: Vector[(CVar, SArg)] = irApplet.inputVars zip stg.inputs
          val inputs = genStageInputs(linkedInputs)
          // convert the per-stage metadata into JSON
          val stageReqDesc = JsObject(
              Map("id" -> JsString(stg.id.getId),
                  "executable" -> JsString(dxExec.getId),
                  "name" -> JsString(stg.description),
                  "input" -> inputs)
          )
          stagesReq :+ stageReqDesc
      }

    // Sub-workflow are compiled to hidden objects.
    val hidden = wf.level == IR.Level.Sub

    // links through applets that run workflow fragments
    val transitiveDependencies: Vector[ExecLinkInfo] =
      wf.stages.foldLeft(Vector.empty[ExecLinkInfo]) {
        case (accu, stg) =>
          val Native.ExecRecord(_, _, dependencies) = execDict(stg.calleeName)
          accu ++ dependencies
      }

    // pack all the arguments into a single API call
    val reqFields = Map("name" -> JsString(wf.name),
                        "stages" -> JsArray(stagesReq),
                        "hidden" -> JsBoolean(hidden))

    val wfInputOutput: Map[String, JsValue] =
      if (wf.locked) {
        // Locked workflows have well defined inputs and outputs
        val wfInputSpec: Vector[JsValue] = wf.inputs
          .sortWith(_._1.name < _._1.name)
          .flatMap { case (cVar, sArg) => buildWorkflowInputSpec(cVar, sArg) }
        val wfOutputSpec: Vector[JsValue] = wf.outputs
          .sortWith(_._1.name < _._1.name)
          .flatMap { case (cVar, sArg) => buildWorkflowOutputSpec(cVar, sArg) }
        Map("inputs" -> JsArray(wfInputSpec), "outputs" -> JsArray(wfOutputSpec))
      } else {
        Map.empty
      }

    // Add the workflow source into the details field.
    // There could be JSON-invalid characters in the source code, so we use base64 encoding.
    // It could be quite large, so we use compression.
    val generator = WdlV1Generator()
    val sourceLines = generator.generateElement(wf.document)
    val sourceCode = SysUtils.gzipAndBase64Encode(sourceLines.mkString("\n"))
    val sourceCodeField: Map[String, JsValue] = Map("wdlSourceCode" -> JsString(sourceCode))

    // link to applets used by the fragments. This notifies the platform that they
    // need to be cloned when copying workflows.
    val dxLinks: Map[String, JsValue] = transitiveDependencies.map { execLinkInfo =>
      ("link_" + execLinkInfo.name) -> JsObject(
          "$dnanexus_link" -> JsString(execLinkInfo.dxExec.getId)
      )
    }.toMap

    val delayWD: Map[String, JsValue] = extras match {
      case None => Map.empty
      case Some(ext) =>
        ext.delayWorkspaceDestruction match {
          case Some(true) => Map("delayWorkspaceDestruction" -> JsTrue)
          case _          => Map.empty
        }
    }

    val defaultTags = Vector(JsString("dxWDL"))
    val execTreeMap = buildWorkflowExecTree(wf, execDict)
    val (wfMeta, wfMetaDetails) = buildWorkflowMetadata(wf, defaultTags)

    val details = Map(
        "details" -> JsObject(
            wfMetaDetails ++ sourceCodeField ++ dxLinks ++ delayWD ++ execTreeMap
        )
    )

    val ignoreReuse: Map[String, JsValue] = extras match {
      case None => Map.empty
      case Some(ext) =>
        ext.ignoreReuse match {
          case None        => Map.empty
          case Some(false) => Map.empty
          case Some(true)  =>
            // We want to ignore reuse for all stages.
            Map("ignoreReuse" -> JsArray(JsString("*")))
        }
    }

    // pack all the arguments into a single API call
    val reqFieldsAll = wfMeta ++ reqFields ++ wfInputOutput ++ details ++ ignoreReuse

    // Add a checksum
    val (digest, reqWithChecksum) = checksumReq(wf.name, reqFieldsAll)

    // Add properties we do not want to fall under the checksum.
    // This allows, for example, moving the dx:executable, while
    // still being able to reuse it.
    val reqWithEverything = reqWithChecksum ++ Map(
        "project" -> JsString(dxProject.id),
        "folder" -> JsString(folder),
        "parents" -> JsBoolean(true)
    )

    (digest, reqWithEverything)
  }

  private def buildWorkflow(req: Map[String, JsValue]): DxWorkflow = {
    val rep = dxApi.workflowNew(req)
    val id = apiParseReplyID(rep)
    val dxwf = dxApi.workflow(id)

    // Close the workflow
    if (!leaveWorkflowsOpen) {
      dxwf.close()
    }
    dxwf
  }

  // Compile an entire workflow
  //
  // - Calculate the workflow checksum from the intermediate representation
  // - Do not rebuild the workflow if it has a correct checksum
  private def buildWorkflowIfNeeded(wf: IR.Workflow,
                                    execDict: Map[String, Native.ExecRecord]): DxWorkflow = {
    val (digest, wfNewReq) = workflowNewReq(wf, execDict)
    val buildRequired = isBuildRequired(wf.name, digest)
    buildRequired match {
      case None =>
        val dxWorkflow = buildWorkflow(wfNewReq)
        dxObjDir.insert(wf.name, dxWorkflow, digest)
        dxWorkflow
      case Some(dxObj) =>
        // Old workflow exists, and it has not changed.
        // we need to find a way to get the dependencies.
        dxObj.asInstanceOf[DxWorkflow]
    }
  }

  private def buildWorkflowExecTree(
      wf: IR.Workflow,
      execDict: Map[String, Native.ExecRecord]
  ): Map[String, JsString] = {
    val jsonTreeString = Tree(execDict).fromWorkflowIR(wf).toString
    val compressedTree = SysUtils.gzipAndBase64Encode(jsonTreeString)
    Map("execTree" -> JsString(compressedTree))
  }

  def apply(bundle: IR.Bundle): Native.Results = {
    dxApi.logger.trace("Native pass, generate dx:applets and dx:workflows")

    // build applets and workflows if they aren't on the platform already
    val execDict = bundle.dependencies.foldLeft(Map.empty[String, Native.ExecRecord]) {
      case (accu, cName) =>
        val execIr = bundle.allCallables(cName)
        execIr match {
          case apl: IR.Applet =>
            val execRecord = apl.kind match {
              case IR.AppletKindNative(id) =>
                // native applets do not depend on other data-objects
                val dxExec = dxApi.executable(id)
                Native.ExecRecord(apl, dxExec, Vector.empty)
              case IR.AppletKindWorkflowCustomReorg(id) =>
                // does this have to be a different class?
                val dxExec = dxApi.executable(id)
                Native.ExecRecord(apl, dxExec, Vector.empty)
              case _ =>
                val (dxApplet, dependencies) = buildAppletIfNeeded(apl, accu)
                Native.ExecRecord(apl, dxApplet, dependencies)
            }
            accu + (apl.name -> execRecord)
          case wf: IR.Workflow =>
            val dxwfl = buildWorkflowIfNeeded(wf, accu)
            accu + (wf.name -> Native.ExecRecord(wf, dxwfl, Vector.empty))
        }
    }

    // build the toplevel workflow, if it is defined
    val primary: Option[Native.ExecRecord] = bundle.primaryCallable.flatMap { callable =>
      execDict.get(callable.name)
    }

    Native.Results(primary, execDict)
  }
}
