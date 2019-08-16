/** Generate a dx:worflow and dx:applets from an intermediate representation.
  */
package dxWDL.compiler

// DX bindings
import com.fasterxml.jackson.databind.JsonNode
import com.dnanexus._
import java.security.MessageDigest
import scala.collection.JavaConverters._
import scala.collection.immutable.TreeMap
import spray.json._
import DefaultJsonProtocol._

import wom.types._
import wom.values._

import dxWDL.base._
//import dxWDL.base.WomTypeSerialization
import dxWDL.util._
import dxWDL.dx._
import IR.{CVar, SArg}

case class Native(dxWDLrtId: Option[String],
                  folder: String,
                  dxProject: DXProject,
                  dxObjDir: DxObjectDirectory,
                  instanceTypeDB: InstanceTypeDB,
                  dxPathConfig: DxPathConfig,
                  fileInfoDir : Map[DXFile, DxDescribe],
                  typeAliases: Map[String, WomType],
                  extras: Option[Extras],
                  runtimeDebugLevel: Option[Int],
                  leaveWorkflowsOpen: Boolean,
                  force: Boolean,
                  archive: Boolean,
                  locked: Boolean,
                  verbose: Verbose) {
    case class ExecRecord(callable: IR.Callable,
                          dxExec: DxExec,
                          links: Vector[ExecLinkInfo])

    private val verbose2:Boolean = verbose.containsKey("Native")
    private val rtDebugLvl = runtimeDebugLevel.getOrElse(Utils.DEFAULT_RUNTIME_DEBUG_LEVEL)
    private val wdlVarLinksConverter = WdlVarLinksConverter(fileInfoDir, typeAliases)

    // Are we setting up a private docker registry?
    private val dockerRegistryInfo : Option[DockerRegistry]= extras match {
        case None => None
        case Some(extras) =>
            extras.dockerRegistry match {
                case None => None
                case Some(x) => Some(x)
            }
    }

    lazy val runtimeLibrary: Option[JsValue] =
        dxWDLrtId match {
            case None => None
            case Some(id) =>
                // Open the archive
                // Extract the archive from the details field
                val record = DXRecord.getInstance(id)
                val descOptions = DXDataObject.DescribeOptions.get().inProject(dxProject).withDetails
                val details = DxUtils.jsValueOfJsonNode(
                    record.describe(descOptions).getDetails(classOf[JsonNode]))
                val dxLink = details.asJsObject.fields.get("archiveFileId") match {
                    case Some(x) => x
                    case None => throw new Exception(s"record does not have an archive field ${details}")
                }
                val dxFile = DxUtils.dxFileFromJsValue(dxLink)
                val name = dxFile.describe.getName()
                Some(JsObject(
                         "name" -> JsString(name),
                         "id" -> JsObject("$dnanexus_link" -> JsString(dxFile.getId()))
                     ))
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
    private def cVarToSpec(cVar: CVar) : Vector[JsValue] = {
        val name = cVar.dxVarName
        val defaultVals:Map[String, JsValue] = cVar.default match {
            case None => Map.empty
            case Some(wdlValue) =>
                val wvl = wdlVarLinksConverter.importFromWDL(cVar.womType, wdlValue)
                wdlVarLinksConverter.genFields(wvl, name).toMap
        }
        def jsMapFromDefault(name: String) : Map[String, JsValue] = {
            defaultVals.get(name) match {
                case None => Map.empty
                case Some(jsv) => Map("default" -> jsv)
            }
        }
        def jsMapFromOptional(optional: Boolean) : Map[String, JsValue] = {
            if (optional) {
                Map("optional" -> JsBoolean(true))
            } else {
                Map.empty[String, JsValue]
            }
        }
        def mkPrimitive(dxType: String, optional: Boolean) : Vector[JsValue] = {
            Vector(JsObject(Map("name" -> JsString(name),
                                "class" -> JsString(dxType))
                                ++ jsMapFromOptional(optional)
                                ++ jsMapFromDefault(name)))
        }
        def mkPrimitiveArray(dxType: String, optional:Boolean) : Vector[JsValue] = {
            Vector(JsObject(Map("name" -> JsString(name),
                                "class" -> JsString("array:" ++ dxType))
                                ++ jsMapFromOptional(optional)
                                ++ jsMapFromDefault(name)))
        }
        def mkComplex(optional: Boolean) : Vector[JsValue] = {
            // A large JSON structure passed as a hash, and a
            // vector of platform files.
            Vector(JsObject(Map("name" -> JsString(name),
                                "class" -> JsString("hash"))
                                ++ jsMapFromOptional(optional)
                                ++ jsMapFromDefault(name)),
                   JsObject(Map("name" -> JsString(name + Utils.FLAT_FILES_SUFFIX),
                                "class" -> JsString("array:file"),
                                "optional" -> JsBoolean(true))
                                ++ jsMapFromDefault(name + Utils.FLAT_FILES_SUFFIX)))
        }
        def handleType(wdlType: WomType,
                       optional: Boolean) : Vector[JsValue] = {
            wdlType match {
                // primitive types
                case WomBooleanType => mkPrimitive("boolean", optional)
                case WomIntegerType => mkPrimitive("int", optional)
                case WomFloatType => mkPrimitive("float", optional)
                case WomStringType =>mkPrimitive("string", optional)
                case WomSingleFileType => mkPrimitive("file", optional)

                // single dimension arrays of primitive types
                case WomNonEmptyArrayType(WomBooleanType) => mkPrimitiveArray("boolean", optional)
                case WomNonEmptyArrayType(WomIntegerType) => mkPrimitiveArray("int", optional)
                case WomNonEmptyArrayType(WomFloatType) => mkPrimitiveArray("float", optional)
                case WomNonEmptyArrayType(WomStringType) => mkPrimitiveArray("string", optional)
                case WomNonEmptyArrayType(WomSingleFileType) => mkPrimitiveArray("file", optional)
                case WomMaybeEmptyArrayType(WomBooleanType) => mkPrimitiveArray("boolean", true)
                case WomMaybeEmptyArrayType(WomIntegerType) => mkPrimitiveArray("int", true)
                case WomMaybeEmptyArrayType(WomFloatType) => mkPrimitiveArray("float", true)
                case WomMaybeEmptyArrayType(WomStringType) => mkPrimitiveArray("string", true)
                case WomMaybeEmptyArrayType(WomSingleFileType) => mkPrimitiveArray("file", true)

                // complex type, that may contains files
                case _ => mkComplex(optional)
            }
        }
        cVar.womType match {
            case WomOptionalType(t) => handleType(t, true)
            case t => handleType(t, false)
        }
    }


    // Create a bunch of bash export declarations, describing
    // the variables required to login to the docker private repository (if needed).
     private def dockerPreamble(dockerImage: IR.DockerImage) : String = {
         dockerRegistryInfo match {
            case None => ""
            case Some(DockerRegistry(registry, username, credentials)) =>
                // check that the credentials file is a valid platform path
                try {
                    val dxFile = DxPath.resolveDxURLFile(credentials)
                    Utils.ignore(dxFile)
                } catch {
                    case e : Throwable =>
                        throw new Exception(s"""|credentials has to point to a platform file.
                                                |It is now:
                                                |   ${credentials}
                                                |Error:
                                                |  ${e}
                                                |""".stripMargin)
                }

                // strip the URL from the dx:// prefix, so we can use dx-download directly
                val credentialsWithoutPrefix = credentials.substring(Utils.DX_URL_PREFIX.length)
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
                    |cat $${HOME}/docker_credentials | docker login $${DOCKER_REGISTRY} -u $${DOCKER_USERNAME} --password-stdin
                    |rm -f $${HOME}/docker_credentials
                    |""".stripMargin
         }
     }

    private def genBashScriptTaskBody(): String = {
        s"""|    # Keep track of streaming files. Each such file
            |    # is converted into a fifo, and a 'dx cat' process
            |    # runs in the background.
            |    background_pids=()
            |
            |    # evaluate input arguments, and download input files
            |    java -jar $${DX_FS_ROOT}/dxWDL.jar internal taskProlog $${HOME} ${rtDebugLvl.toString}
            |
            |    # setup any file streams. Keep track of background
            |    # processes in the 'background_pids' array.
            |    # We 'source' the sub-script here, because we
            |    # need to wait for the pids. This can only be done
            |    # for child processes (not grand-children).
            |    if [[ -e ${dxPathConfig.setupStreams} ]]; then
            |       cat ${dxPathConfig.setupStreams}
            |       source ${dxPathConfig.setupStreams} > $${HOME}/meta/background_pids.txt
            |
            |       # reads the file line by line, and convert into a bash array
            |       mapfile -t background_pids < $${HOME}/meta/background_pids.txt
            |       echo "Background processes ids: $${background_pids[@]}"
            |    fi
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
            |       dx-download-agent inspect ${dxPathConfig.dxdaManifest}.bz2
            |
            |       # go back to home directory
            |       cd ${dxPathConfig.homeDir}
            |    fi
            |
            |    echo "bash command encapsulation script:"
            |    cat ${dxPathConfig.script}
            |
            |    # Run the shell script generated by the prolog.
            |    # Capture the stderr/stdout in files
            |    if [[ -e ${dxPathConfig.dockerSubmitScript} ]]; then
            |        echo "docker submit script:"
            |        cat ${dxPathConfig.dockerSubmitScript}
            |        ${dxPathConfig.dockerSubmitScript}
            |    else
            |        /bin/bash ${dxPathConfig.script.toString}
            |    fi
            |
            |    # This section deals with streaming files.
            |    #
            |    # We cannot wait for all background processes to complete,
            |    # because the worker process may not read one of the fifo streams.
            |    # We want to make sure there were no abnormal terminations.
            |    #
            |    # Assumptions
            |    #  1) 'dx cat' returns zero status when a user reads only the beginning
            |    #  of a file
            |    for pid in $${background_pids[@]}; do
            |        p_status=0
            |        p_status=`ps --pid $$pid --no-headers | wc -l`  || p_status=0
            |
            |        if [[ $$p_status == 0 ]]; then
            |            # the process is already dead, check correct exit status
            |            echo "wait $$pid"
            |            rc=0
            |            wait $$pid || rc=$$?
            |            if [[ $$rc != 0 ]]; then
            |                echo "Background download process $$pid failed"
            |                exit $$rc
            |            fi
            |        else
            |            echo "Warning: background download process $$pid is still running."
            |            echo "Perhaps the worker process did not read it."
            |        fi
            |    done
            |
            |    #  check return code of the script
            |    rc=`cat ${dxPathConfig.rcPath}`
            |    if [[ $$rc != 0 ]]; then
            |        exit $$rc
            |    fi
            |
            |    # evaluate applet outputs, and upload result files
            |    java -jar $${DX_FS_ROOT}/dxWDL.jar internal taskEpilog $${HOME} ${rtDebugLvl.toString}
            |""".stripMargin.trim
    }

    private def genBashScriptWfFragment() : String = {
        s"""|main() {
            |    java -jar $${DX_FS_ROOT}/dxWDL.jar internal wfFragment $${HOME} ${rtDebugLvl.toString}
            |}
            |
            |collect() {
            |    java -jar $${DX_FS_ROOT}/dxWDL.jar internal collect $${HOME} ${rtDebugLvl.toString}
            |}""".stripMargin.trim
    }

    private def genBashScriptCmd(cmd: String) : String = {
        s"""|main() {
            |    java -jar $${DX_FS_ROOT}/dxWDL.jar internal ${cmd} $${HOME} ${rtDebugLvl.toString}
            |}""".stripMargin.trim
    }

    private def genBashScript(applet: IR.Applet,
                              instanceType: IR.InstanceType) : String = {
        val body:String = applet.kind match {
            case IR.AppletKindNative(_) =>
                throw new Exception("Sanity: generating a bash script for a native applet")
            case IR.AppletKindWfFragment(_, _, _) =>
                genBashScriptWfFragment()
            case IR.AppletKindWfInputs =>
                genBashScriptCmd("wfInputs")
            case IR.AppletKindWfOutputs =>
                genBashScriptCmd("wfOutputs")
            case IR.AppletKindWorkflowOutputReorg =>
                genBashScriptCmd("workflowOutputReorg")
            case IR.AppletKindTask(_) =>
                instanceType match {
                    case IR.InstanceTypeDefault | IR.InstanceTypeConst(_,_,_,_) =>
                        s"""|${dockerPreamble(applet.docker)}
                            |
                            |main() {
                            |${genBashScriptTaskBody()}
                            |}""".stripMargin
                    case IR.InstanceTypeRuntime =>
                        s"""|${dockerPreamble(applet.docker)}
                            |
                            |main() {
                            |    # check if this is the correct instance type
                            |    correctInstanceType=`java -jar $${DX_FS_ROOT}/dxWDL.jar internal taskCheckInstanceType $${HOME} ${rtDebugLvl.toString}`
                            |    if [[ $$correctInstanceType == "true" ]]; then
                            |        body
                            |    else
                            |       # evaluate the instance type, and launch a sub job on it
                            |       java -jar $${DX_FS_ROOT}/dxWDL.jar internal taskRelaunch $${HOME} ${rtDebugLvl.toString}
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
    private def chksum(s: String) : String = {
        val digest = MessageDigest.getInstance("MD5").digest(s.getBytes)
        digest.map("%02X" format _).mkString
    }

    // Add a checksum to a request
    private def checksumReq(name: String,
                            fields: Map[String, JsValue]) : (String, JsValue) = {
        Utils.trace(verbose2, s"""|${name} -> checksum request
                                  |fields = ${JsObject(fields).prettyPrint}
                                  |
                                  |""".stripMargin)
        val jsDet = Utils.makeDeterministic(JsObject(fields))
        val digest = chksum(jsDet.prettyPrint)
        val props = Map("properties" ->
                            JsObject(Utils.CHECKSUM_PROP -> JsString(digest)))
        val reqChk = fields ++ props
        (digest, JsObject(reqChk))
    }

    // Do we need to build this applet/workflow?
    //
    // Returns:
    //   None: build is required
    //   Some(dxobject) : the right object is already on the platform
    private def isBuildRequired(name: String,
                                digest: String) : Option[DXDataObject] = {
        // Have we built this applet already, but placed it elsewhere in the project?
        dxObjDir.lookupOtherVersions(name, digest) match {
            case None => ()
            case Some((dxObj, desc)) =>
                Utils.trace(verbose.on, s"Found existing version of ${name} in folder ${desc.folder}")
                return Some(dxObj)
        }

        val existingDxObjs = dxObjDir.lookup(name)
        val buildRequired:Boolean = existingDxObjs.size match {
            case 0 => true
            case 1 =>
                // Check if applet code has changed
                val dxObjInfo = existingDxObjs.head
                dxObjInfo.digest match {
                    case None =>
                        throw new Exception(s"There is an existing non-dxWDL applet ${name}")
                    case Some(digest2) if digest != digest2 =>
                        Utils.trace(verbose.on,
                                    s"${dxObjInfo.dxClass} ${name} has changed, rebuild required")
                        true
                    case Some(_) =>
                        Utils.trace(verbose.on, s"${dxObjInfo.dxClass} ${name} has not changed")
                        false
                }
            case _ =>
                val dxClass = existingDxObjs.head.dxClass
                Utils.warning(verbose, s"""|More than one ${dxClass} ${name} found in
                                           |path ${dxProject.getId()}:${folder}""".stripMargin)
                true
        }

        if (buildRequired) {
            if (existingDxObjs.size > 0) {
                if (archive) {
                    // archive the applet/workflow(s)
                    existingDxObjs.foreach(x => dxObjDir.archiveDxObject(x))
                } else if (force) {
                    // the dx:object exists, and needs to be removed. There
                    // may be several versions, all are removed.
                    val objs = existingDxObjs.map(_.dxObj)
                    Utils.trace(verbose.on, s"Removing old ${name} ${objs.map(_.getId)}")
                    dxProject.removeObjects(objs.asJava)
                } else {
                    val dxClass = existingDxObjs.head.dxClass
                    throw new Exception(s"""|${dxClass} ${name} already exists in
                                            | ${dxProject.getId}:${folder}""".stripMargin)
                }
            }
            None
        } else {
            assert(existingDxObjs.size == 1)
            Some(existingDxObjs.head.dxObj)
        }
    }

    // Create linking information for a dx:executable
    private def genLinkInfo(irCall: IR.Callable,
                            dxObj: DxExec) : ExecLinkInfo = {
        val callInputDefs: Map[String, WomType] = irCall.inputVars.map{
            case CVar(name, wdlType, _) => (name -> wdlType)
        }.toMap
        val callOutputDefs: Map[String, WomType] = irCall.outputVars.map{
            case CVar(name, wdlType, _) => (name -> wdlType)
        }.toMap
        ExecLinkInfo(irCall.name,
                     callInputDefs,
                     callOutputDefs,
                     dxObj)
    }

    private def apiParseReplyID(rep: JsonNode) : String = {
        val repJs:JsValue = DxUtils.jsValueOfJsonNode(rep)
        repJs.asJsObject.fields.get("id") match {
            case None => throw new Exception("API call did not returnd an ID")
            case Some(JsString(x)) => x
            case other => throw new Exception(s"API call returned invalid ID ${other}")
        }
    }

    private def addLicences(applet: IR.Applet) : Map[String, JsValue] = {

        val taskSpecificDetails : Map[String, JsValue] =
            if (applet.kind.isInstanceOf[IR.AppletKindTask]) {
                // A task can override the default dx attributes
                extras match {
                    case None => Map.empty
                    case Some(ext) => ext.perTaskDxAttributes.get(applet.name) match {
                        case None => Map.empty
                        case Some(dta) => dta.getDetailsJson
                    }
                }
            } else {
                Map("test"->"something".toJson)
            }


        return taskSpecificDetails
    }

    // Set the run spec.
    //
    private def calcRunSpec(applet: IR.Applet,
                            details: Map[String, JsValue],
                            bashScript: String) : (JsValue, Map[String, JsValue]) = {
        // find the dxWDL asset
        val instanceType:String = applet.instanceType match {
            case x : IR.InstanceTypeConst =>
                val xDesc = InstanceTypeReq(x.dxInstanceType,
                                            x.memoryMB,
                                            x.diskGB,
                                            x.cpu)
                instanceTypeDB.apply(xDesc)
            case IR.InstanceTypeDefault | IR.InstanceTypeRuntime =>
                instanceTypeDB.defaultInstanceType
        }
        val runSpec: Map[String, JsValue] = Map(
            "code" -> JsString(bashScript),
            "interpreter" -> JsString("bash"),
            "systemRequirements" ->
                JsObject("main" ->
                             JsObject("instanceType" -> JsString(instanceType))),
            "distribution" -> JsString("Ubuntu"),
            "release" -> JsString(Utils.UBUNTU_VERSION),
        )

        // Start with the default dx-attribute section, and override
        // any field that is specified in the individual task section.
        val extraRunSpec : Map[String, JsValue] = extras match {
            case None => Map.empty
            case Some(ext) => ext.defaultTaskDxAttributes match {
                case None => Map.empty
                case Some(dta) => dta.getRunSpecJson
            }
        }

        val taskSpecificRunSpec : Map[String, JsValue] =
            if (applet.kind.isInstanceOf[IR.AppletKindTask]) {
                // A task can override the default dx attributes
                extras match {
                    case None => Map.empty
                    case Some(ext) => ext.perTaskDxAttributes.get(applet.name) match {
                        case None => Map.empty
                        case Some(dta) => dta.getRunSpecJson
                    }
                }
            } else {
                Map.empty
            }

        val runSpecWithExtras = runSpec ++ extraRunSpec ++ taskSpecificRunSpec

        // - If the docker image is a tarball, add a link in the details field.
        val dockerFile: Option[DXFile] = applet.docker match {
            case IR.DockerImageNone => None
            case IR.DockerImageNetwork => None
            case IR.DockerImageDxFile(_, dxfile) =>
                // A docker image stored as a tar ball in a platform file
                Some(dxfile)
        }
        val bundledDepends = runtimeLibrary match {
            case None => Map.empty
            case Some(rtLib) => Map("bundledDepends" -> JsArray(Vector(rtLib)))
        }
        val runSpecEverything = JsObject(runSpecWithExtras ++ bundledDepends)

        val details2 = dockerFile match {
            case None => details
            case Some(dxfile) =>
                details + ("docker-image" -> DxUtils.dxFileToJsValue(dxfile))
        }
        (runSpecEverything, details2)
    }

    def calcAccess(applet: IR.Applet) : JsValue = {
        val extraAccess: DxAccess = extras match {
            case None => DxAccess.empty
            case Some(ext) => ext.getDefaultAccess
        }
        val taskSpecificAccess : DxAccess =
            if (applet.kind.isInstanceOf[IR.AppletKindTask]) {
                // A task can override the default dx attributes
                extras match {
                    case None => DxAccess.empty
                    case Some(ext) => ext.getTaskAccess(applet.name)
                }
            } else {
                DxAccess.empty
            }

        // If we are using a private docker registry, add the allProjects: VIEW
        // access to tasks.
        val allProjectsAccess: DxAccess = dockerRegistryInfo match {
            case None => DxAccess.empty
            case Some(_) => DxAccess(None, None, Some(AccessLevel.VIEW), None, None)
        }
        val taskAccess = extraAccess.merge(taskSpecificAccess).merge(allProjectsAccess)

        val access: DxAccess = applet.kind match {
            case IR.AppletKindTask(_) =>
                if (applet.docker == IR.DockerImageNetwork) {
                    // docker requires network access, because we are downloading
                    // the image from the network
                    taskAccess.merge(DxAccess(Some(Vector("*")), None,  None,  None,  None))
                } else {
                    taskAccess
                }
            case IR.AppletKindWorkflowOutputReorg =>
                // The WorkflowOutput applet requires higher permissions
                // to organize the output directory.
                DxAccess(None, Some(AccessLevel.CONTRIBUTE), None, None, None)
            case _ =>
                // Even scatters need network access, because
                // they spawn subjobs that (may) use dx-docker.
                // We end up allowing all applets to use the network
                taskAccess.merge(DxAccess(Some(Vector("*")), None,  None,  None,  None))
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
                             folder : String,
                             aplLinks: Map[String, ExecLinkInfo]) : (String, JsValue) = {
        Utils.trace(verbose2, s"Building /applet/new request for ${applet.name}")

        val inputSpec : Vector[JsValue] = applet.inputs
            .sortWith(_.name < _.name)
            .map(cVar => cVarToSpec(cVar))
            .flatten.toVector

        // create linking information
        val linkInfo : Map[String, JsValue] =
            aplLinks.map{ case (name, ali) =>
                name -> ExecLinkInfo.writeJson(ali, typeAliases)
            }.toMap

        val metaInfo : Map[String, JsValue] =
            applet.kind match {
                case IR.AppletKindWfFragment(calls, blockPath, fqnDictTypes) =>
                    // meta information used for running workflow fragments
                    Map("execLinkInfo" -> JsObject(linkInfo),
                        "blockPath" -> JsArray(blockPath.map(JsNumber(_))),
                        "fqnDictTypes" -> JsObject(
                            fqnDictTypes.map{ case (k,t) =>
                                val tStr = WomTypeSerialization(typeAliases).toString(t)
                                k -> JsString(tStr)
                            }.toMap))

                case IR.AppletKindWfInputs |
                        IR.AppletKindWfOutputs |
                        IR.AppletKindWorkflowOutputReorg =>
                    // meta information used for running workflow fragments
                    val fqnDictTypes = JsObject(
                        applet.inputVars.map{
                            case cVar =>
                                val tStr = WomTypeSerialization(typeAliases).toString(cVar.womType)
                                cVar.name -> JsString(tStr)
                        }.toMap)
                    Map("fqnDictTypes" -> fqnDictTypes)

                case _ =>
                    Map.empty
            }

        val outputSpec : Vector[JsValue] = applet.outputs
            .sortWith(_.name < _.name)
            .map(cVar => cVarToSpec(cVar))
            .flatten.toVector

        // put the wom source code into the details field.
        // Add the pricing model, and make the prices opaque.
        val womSourceCode = Utils.gzipAndBase64Encode(applet.womSourceCode)
        val dbOpaque = InstanceTypeDB.opaquePrices(instanceTypeDB)
        val dbOpaqueInstance = Utils.gzipAndBase64Encode(dbOpaque.toJson.prettyPrint)
        val auxInfo = Map("womSourceCode" -> JsString(womSourceCode),
                           "instanceTypeDB" -> JsString(dbOpaqueInstance))

        // Links to applets that could get called at runtime. If
        // this applet is copied, we need to maintain referential integrity.
        val dxLinks = aplLinks.map{ case (name, execLinkInfo) =>
            ("link_" + name) -> JsObject(
                "$dnanexus_link" -> JsString(execLinkInfo.dxExec.getId))
        }.toMap
        val (runSpec : JsValue, details: Map[String, JsValue]) =
            calcRunSpec(applet,
                        auxInfo ++ dxLinks ++ metaInfo,
                        bashScript)
        val detailsWithLicense: Map[String, JsValue] = addLicences(applet)
        val jsDetails: JsValue = JsObject(details ++ detailsWithLicense)
        val access : JsValue = calcAccess(applet)

        // A fragemnt is hidden, not visible under default settings. This
        // allows the workflow copying code to traverse it, and link to
        // anything it calls.
        val hidden : Boolean =
            applet.kind match {
                case _ : IR.AppletKindWfFragment => true
                case _ => false
            }

        // pack all the core arguments into a single request
        val reqCore = Map(
            "name" -> JsString(applet.name),
            "inputSpec" -> JsArray(inputSpec),
            "outputSpec" -> JsArray(outputSpec),
            "runSpec" -> runSpec,
            "dxapi" -> JsString("1.0.0"),
            "tags" -> JsArray(JsString("dxWDL")),
            "details" -> jsDetails,
            "hidden" -> JsBoolean(hidden),
        )
        val accessField =
            if (access == JsNull) Map.empty
            else Map("access" -> access)

        // Add a checksum
        val (digest, req) = checksumReq(applet.name, reqCore ++ accessField)

        // Add properties we do not want to fall under the checksum.
        // This allows, for example, moving the dx:executable, while
	// still being able to reuse it.
        val reqWithEverything =
            JsObject(req.asJsObject.fields ++ Map(
                               "project" -> JsString(dxProject.getId),
                               "folder" -> JsString(folder),
                               "parents" -> JsBoolean(true)
                           ))
        (digest, reqWithEverything)
    }

    // Rebuild the applet if needed.
    //
    // When [force] is true, always rebuild. Otherwise, rebuild only
    // if the WDL code has changed.
    private def buildAppletIfNeeded(applet: IR.Applet,
                                    execDict: Map[String, ExecRecord])
            : (DXApplet, Vector[ExecLinkInfo]) = {
        Utils.trace(verbose2, s"Compiling applet ${applet.name}")

        // limit the applet dictionary, only to actual dependencies
        val calls: Vector[String] = applet.kind match {
            case IR.AppletKindWfFragment(calls, _, _) => calls
            case _ => Vector.empty
        }

        val aplLinks : Map[String, ExecLinkInfo] = calls.map{ tName =>
            val ExecRecord(irCall, dxObj, _) = execDict(tName)
            tName -> genLinkInfo(irCall, dxObj)
        }.toMap

        // Build an applet script
        val bashScript = genBashScript(applet, applet.instanceType)

        // Calculate a checksum of the inputs that went into the
        // making of the applet.
        val (digest,appletApiRequest) = appletNewReq(applet, bashScript, folder, aplLinks)
        if (verbose2) {
            val fName = s"${applet.name}_req.json"
            val trgPath = Utils.appCompileDirPath.resolve(fName)
            Utils.writeFileContent(trgPath, appletApiRequest.prettyPrint)
        }

        val buildRequired = isBuildRequired(applet.name, digest)
        val dxApplet = buildRequired match {
            case None =>
                // Compile a WDL snippet into an applet.
                val rep = DXAPI.appletNew(DxUtils.jsonNodeOfJsValue(appletApiRequest), classOf[JsonNode])
                val id = apiParseReplyID(rep)
                val dxApplet = DXApplet.getInstance(id)
                dxObjDir.insert(applet.name, dxApplet, digest)
                dxApplet
            case Some(dxObj) =>
                // Old applet exists, and it has not changed. Return the
                // applet-id.
                dxObj.asInstanceOf[DXApplet]
        }
        (dxApplet, aplLinks.values.toVector)
    }

    // Calculate the stage inputs from the call closure
    //
    // It comprises mappings from variable name to WomType.
    private def genStageInputs(inputs: Vector[(CVar, SArg)]) : JsValue = {
        // sort the inputs, to make the request deterministic
        val jsInputs:TreeMap[String, JsValue] = inputs.foldLeft(TreeMap.empty[String, JsValue]){
            case (m, (cVar, sArg)) =>
                sArg match {
                    case IR.SArgEmpty =>
                        // We do not have a value for this input at compile time.
                        // For compulsory applet inputs, the user will have to fill
                        // in a value at runtime.
                        m
                    case IR.SArgConst(wValue) =>
                        val wvl = wdlVarLinksConverter.importFromWDL(cVar.womType, wValue)
                        val fields = wdlVarLinksConverter.genFields(wvl, cVar.dxVarName)
                        m ++ fields.toMap
                    case IR.SArgLink(dxStage, argName) =>
                        val wvl = WdlVarLinks(cVar.womType,
                                              DxlStage(dxStage, IORef.Output, argName.dxVarName))
                        val fields = wdlVarLinksConverter.genFields(wvl, cVar.dxVarName)
                        m ++ fields.toMap
                    case IR.SArgWorkflowInput(argName) =>
                        val wvl = WdlVarLinks(cVar.womType,
                                              DxlWorkflowInput(argName.dxVarName))
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
    private def buildWorkflowInputSpec(cVar:CVar,
                                       sArg:SArg) : Vector[JsValue] = {
        // deal with default values
        val sArgDefault: Option[WomValue] = sArg match {
            case IR.SArgConst(wdlValue) =>
                Some(wdlValue)
            case _ =>
                None
        }

        // The default value can come from the SArg, or the CVar
        val default = (sArgDefault, cVar.default) match {
            case (Some(x), _) => Some(x)
            case (_, Some(x)) => Some(x)
            case _ => None
        }

        val cVarWithDflt = cVar.copy(default = default)
        cVarToSpec(cVarWithDflt)
    }

    // Note: a single WDL output can generate one or two JSON outputs.
    private def buildWorkflowOutputSpec(cVar:CVar,
                                        sArg:SArg) : Vector[JsValue] = {
        val oSpec: Vector[JsValue] = cVarToSpec(cVar)

        // add the field names, to help index this structure
        val oSpecMap:Map[String, JsValue] = oSpec.map{ jso =>
            val nm = jso.asJsObject.fields.get("name") match {
                case Some(JsString(nm)) => nm
                case _ => throw new Exception("sanity")
            }
            (nm -> jso)
        }.toMap

        val outputSources:List[(String, JsValue)] = sArg match {
            case IR.SArgConst(wdlValue) =>
                // constant
                throw new Exception(s"Constant workflow outputs not currently handled (${cVar}, ${sArg}, ${wdlValue})")
            case IR.SArgLink(dxStage, argName: CVar) =>
                val wvl = WdlVarLinks(cVar.womType,
                                      DxlStage(dxStage, IORef.Output, argName.dxVarName))
                wdlVarLinksConverter.genFields(wvl, cVar.dxVarName)
            case IR.SArgWorkflowInput(argName: CVar) =>
                val wvl = WdlVarLinks(cVar.womType,
                                      DxlWorkflowInput(argName.dxVarName))
                wdlVarLinksConverter.genFields(wvl, cVar.dxVarName)
            case other =>
                throw new Exception(s"Bad value for sArg ${other}")
        }

        // merge the specification and the output sources
        outputSources.map{ case (fieldName, outputJs) =>
            val specJs:JsValue = oSpecMap(fieldName)
            JsObject(specJs.asJsObject.fields ++
                               Map("outputSource" -> outputJs))
        }.toVector
    }

    // Create a request for a workflow encapsulated in single API call.
    // Prepare the list of stages, and the checksum in advance.
    private def workflowNewReq(wf: IR.Workflow,
                               execDict: Map[String, ExecRecord]) : (String, JsValue) = {
        Utils.trace(verbose2, s"build workflow ${wf.name}")

        val stagesReq =
            wf.stages.foldLeft(Vector.empty[JsValue]) {
                case (stagesReq, stg) =>
                    val ExecRecord(irApplet, dxExec, _) = execDict(stg.calleeName)
                    val linkedInputs : Vector[(CVar, SArg)] = irApplet.inputVars zip stg.inputs
                    val inputs = genStageInputs(linkedInputs)
                    // convert the per-stage metadata into JSON
                    val stageReqDesc = JsObject(Map(
                        "id" -> JsString(stg.id.getId),
                        "executable" -> JsString(dxExec.getId),
                        "name" -> JsString(stg.description),
                        "input" -> inputs))
                    stagesReq :+ stageReqDesc
            }

        // Sub-workflow are compiled to hidden objects.
        val hidden = wf.level == IR.Level.Sub

        // links through applets that run workflow fragments
        val transitiveDependencies : Vector[ExecLinkInfo] =
            wf.stages.foldLeft(Vector.empty[ExecLinkInfo]) {
                case (accu, stg) =>
                    val ExecRecord(_, _, dependencies) = execDict(stg.calleeName)
                    accu ++ dependencies
            }

        // pack all the arguments into a single API call
        val reqFields = Map("name" -> JsString(wf.name),
                            "stages" -> JsArray(stagesReq),
                            "tags" -> JsArray(JsString("dxWDL")),
                            "hidden" -> JsBoolean(hidden))

        val wfInputOutput: Map[String, JsValue] =
            if (wf.locked) {
                // Locked workflows have well defined inputs and outputs
                val wfInputSpec:Vector[JsValue] = wf.inputs
                    .sortWith(_._1.name < _._1.name)
                    .map{ case (cVar,sArg) => buildWorkflowInputSpec(cVar, sArg) }
                    .flatten
                val wfOutputSpec:Vector[JsValue] = wf.outputs
                    .sortWith(_._1.name < _._1.name)
                    .map{ case (cVar,sArg) => buildWorkflowOutputSpec(cVar, sArg) }
                    .flatten
                Map("inputs" -> JsArray(wfInputSpec),
                    "outputs" -> JsArray(wfOutputSpec))
            } else {
                Map.empty
            }

        // Add the workflow WOM source into the details field.
        // There could be JSON-invalid characters in the source code, so we use base64 encoding.
        // It could be quite large, so we use compression.
        val womSourceCode =Utils.gzipAndBase64Encode(wf.womSourceCode)
        val womSourceCodeField: Map[String, JsValue] =
            Map("womSourceCode" -> JsString(womSourceCode))

        // link to applets used by the fragments. This notifies the platform that they
        // need to be cloned when copying workflows.
        val dxLinks : Map[String, JsValue]= transitiveDependencies.map{
            case execLinkInfo =>
                ("link_" + execLinkInfo.name) -> JsObject(
                    "$dnanexus_link" -> JsString(execLinkInfo.dxExec.getId))
        }.toMap

        val details = Map("details" -> JsObject(womSourceCodeField ++ dxLinks))

        // pack all the arguments into a single API call
        val reqFieldsAll = reqFields ++ wfInputOutput ++ details

        // Add a checksum
        val (digest, reqWithChecksum) = checksumReq(wf.name, reqFieldsAll)

        // Add properties we do not want to fall under the checksum.
        // This allows, for example, moving the dx:executable, while
	// still being able to reuse it.
        val reqWithEverything =
            JsObject(reqWithChecksum.asJsObject.fields ++ Map(
                         "project" -> JsString(dxProject.getId),
                         "folder" -> JsString(folder),
                         "parents" -> JsBoolean(true)
                     ))
        (digest, reqWithEverything)
    }

    private def buildWorkflow(req: JsValue) : DXWorkflow = {
        val rep = DXAPI.workflowNew(DxUtils.jsonNodeOfJsValue(req), classOf[JsonNode])
        val id = apiParseReplyID(rep)
        val dxwf = DXWorkflow.getInstance(id)

        // Close the workflow
        if (!leaveWorkflowsOpen)
            dxwf.close()
        dxwf
    }

    // Compile an entire workflow
    //
    // - Calculate the workflow checksum from the intermediate representation
    // - Do not rebuild the workflow if it has a correct checksum
    private def buildWorkflowIfNeeded(wf: IR.Workflow,
                                      execDict: Map[String, ExecRecord]) : DXWorkflow = {
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
                dxObj.asInstanceOf[DXWorkflow]
        }
    }

    def apply(bundle: IR.Bundle) : CompilationResults = {
        Utils.trace(verbose.on, "Native pass, generate dx:applets and dx:workflows")
        Utils.traceLevelInc()

        // build applets and workflows if they aren't on the platform already
        val execDict = bundle.dependencies.foldLeft(Map.empty[String, ExecRecord]) {
            case (accu, cName) =>
                val execIr = bundle.allCallables(cName)
                execIr match {
                    case apl: IR.Applet =>
                        val execRecord = apl.kind match {
                            case IR.AppletKindNative(id) =>
                                // native applets do not depend on other data-objects
                                ExecRecord(apl, DxExec(id), Vector.empty)
                            case _ =>
                                val (dxApplet, dependencies) = buildAppletIfNeeded(apl, accu)
                                ExecRecord(apl, DxExec(dxApplet.getId), dependencies)
                        }
                        accu + (apl.name -> execRecord)
                    case wf : IR.Workflow =>
                        val dxwfl = buildWorkflowIfNeeded(wf, accu)
                        accu + (wf.name -> ExecRecord(wf, DxExec(dxwfl.getId), Vector.empty))
                }
        }

        // build the toplevel workflow, if it is defined
        val primary : Option[DxExec] = bundle.primaryCallable.flatMap{ callable =>
            execDict.get(callable.name) match {
                case None => None
                case Some(ExecRecord(_, dxExec, _)) => Some(dxExec)
            }
        }

        Utils.traceLevelDec()
        CompilationResults(primary,
                           execDict.map{ case (name, ExecRecord(_,dxExec, _)) => name -> dxExec }.toMap)
    }
}
