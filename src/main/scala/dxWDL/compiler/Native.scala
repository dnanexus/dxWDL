/** Generate a dx:worflow and dx:applets from an intermediate representation.
  */
package dxWDL.compiler

// DX bindings
import com.fasterxml.jackson.databind.JsonNode
import com.dnanexus._
import java.security.MessageDigest
import scala.collection.JavaConverters._
import spray.json._
import wom.types._
import wom.values._

import dxWDL.util._
import dxWDL.util.Utils.{CHECKSUM_PROP, jsonNodeOfJsValue, trace}
import IR.{CVar, SArg}

case class Native(dxWDLrtId: Option[String],
                  folder: String,
                  dxProject: DXProject,
                  dxObjDir: DxObjectDirectory,
                  instanceTypeDB: InstanceTypeDB,
                  dxPathConfig: DxPathConfig,
                  extras: Option[Extras],
                  runtimeDebugLevel: Option[Int],
                  leaveWorkflowsOpen: Boolean,
                  force: Boolean,
                  archive: Boolean,
                  locked: Boolean,
                  verbose: Verbose) {
    private type ExecDict = Map[String, (IR.Callable, DxExec)]
    private val execDictEmpty = Map.empty[String, (IR.Callable, DxExec)]

    private val verbose2:Boolean = verbose.containsKey("Native")
    private val rtDebugLvl = runtimeDebugLevel.getOrElse(Utils.DEFAULT_RUNTIME_DEBUG_LEVEL)

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
                val details = Utils.jsValueOfJsonNode(
                    record.describe(descOptions).getDetails(classOf[JsonNode]))
                val dxLink = details.asJsObject.fields.get("archiveFileId") match {
                    case Some(x) => x
                    case None => throw new Exception(s"record does not have an archive field ${details}")
                }
                val dxFile = Utils.dxFileFromJsValue(dxLink)
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
                val wvl = WdlVarLinks.importFromWDL(cVar.womType, wdlValue)
                WdlVarLinks.genFields(wvl, name).toMap
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
                    val dxFile = DxPath.lookupDxURLFile(credentials)
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
            |    java -jar $${DX_FS_ROOT}/dxWDL.jar internal taskProlog $${HOME} ${rtDebugLvl}
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
            |        /bin/bash ${dxPathConfig.script}
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
            |    java -jar $${DX_FS_ROOT}/dxWDL.jar internal taskEpilog $${HOME} ${rtDebugLvl}
            |""".stripMargin.trim
    }

    private def genBashScriptWfFragment() : String = {
        s"""|main() {
            |    java -jar $${DX_FS_ROOT}/dxWDL.jar internal wfFragment $${HOME} ${rtDebugLvl}
            |}
            |
            |collect() {
            |    java -jar $${DX_FS_ROOT}/dxWDL.jar internal collect $${HOME} ${rtDebugLvl}
            |}""".stripMargin.trim
    }

    private def genBashScriptCmd(cmd: String) : String = {
        s"""|main() {
            |    java -jar $${DX_FS_ROOT}/dxWDL.jar internal ${cmd} $${HOME} ${rtDebugLvl}
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
                            |    correctInstanceType=`java -jar $${DX_FS_ROOT}/dxWDL.jar internal taskCheckInstanceType $${HOME} ${rtDebugLvl}`
                            |    if [[ $$correctInstanceType == "true" ]]; then
                            |        body
                            |    else
                            |       # evaluate the instance type, and launch a sub job on it
                            |       java -jar $${DX_FS_ROOT}/dxWDL.jar internal taskRelaunch $${HOME} ${rtDebugLvl}
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
    private def checksumReq(req: JsValue) : (String, JsValue) = {
        val digest = chksum(req.prettyPrint)
        val props = Map("properties" ->
                            JsObject(CHECKSUM_PROP -> JsString(digest)))
        val reqChk = req match {
            case JsObject(fields) =>
                JsObject(fields ++ props)
            case _ => throw new Exception("sanity")
        }
        (digest, reqChk)
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
                Utils.trace(verbose.on, s"Found existing version of ${name} in folder ${desc.getFolder}")
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
        val repJs:JsValue = Utils.jsValueOfJsonNode(rep)
        repJs.asJsObject.fields.get("id") match {
            case None => throw new Exception("API call did not returnd an ID")
            case Some(JsString(x)) => x
            case other => throw new Exception(s"API call returned invalid ID ${other}")
        }
    }

    // Set the run spec.
    //
    private def calcRunSpec(applet: IR.Applet,
                            bashScript: String) : (JsValue, JsValue) = {
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
                case Some(dta) => dta.toRunSpecJson
            }
        }
        val taskSpecificRunSpec : Map[String, JsValue] =
            if (applet.kind.isInstanceOf[IR.AppletKindTask]) {
                // A task can override the default dx attributes
                extras match {
                    case None => Map.empty
                    case Some(ext) => ext.perTaskDxAttributes.get(applet.name) match {
                        case None => Map.empty
                        case Some(dta) => dta.toRunSpecJson
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

        val details = dockerFile match {
            case None => JsNull
            case Some(dxfile) =>
                JsObject("details" ->
                             JsObject("docker-image" -> Utils.dxFileToJsValue(dxfile)))
        }
        (runSpecEverything, details)
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

        val inputSpec : Vector[JsValue] = applet.inputs.map(cVar =>
            cVarToSpec(cVar)
        ).flatten.toVector

        val dbOpaque = InstanceTypeDB.opaquePrices(instanceTypeDB)
        val dbInstance = dbOpaque.toJson.prettyPrint

        // create linking information
        val linkInfo : Map[String, JsValue] =
            aplLinks.map{ case (name, ali) =>
                name -> ExecLinkInfo.writeJson(ali)
            }.toMap

        val metaInfo : Option[JsValue] =
            applet.kind match {
                case IR.AppletKindWfFragment(calls, blockPath, fqnDictTypes) =>
                    // meta information used for running workflow fragments
                    val hardCodedFragInfo = JsObject(
                        "womSourceCode" -> JsString(Utils.base64Encode(applet.womSourceCode)),
                        "instanceTypeDB" -> JsString(Utils.base64Encode(dbInstance)),
                        "execLinkInfo" -> JsObject(linkInfo),
                        "blockPath" -> JsArray(blockPath.map(JsNumber(_))),
                        "fqnDictTypes" -> JsObject(
                            fqnDictTypes.map{ case (k,t) =>
                                val tStr = WomTypeSerialization.toString(t)
                                k -> JsString(tStr)
                            }.toMap)
                    )
                    Some(JsObject("name" -> JsString(Utils.META_INFO),
                                  "class" -> JsString("hash"),
                                  "default" -> hardCodedFragInfo))


                case IR.AppletKindWfInputs |
                        IR.AppletKindWfOutputs |
                        IR.AppletKindWorkflowOutputReorg =>
                    // meta information used for running workflow fragments
                    val hardCodedFragInfo = JsObject(
                        "womSourceCode" -> JsString(Utils.base64Encode(applet.womSourceCode)),
                        "instanceTypeDB" -> JsString(Utils.base64Encode(dbInstance)),
                        "fqnDictTypes" -> JsObject(
                            applet.inputVars.map{
                                case cVar =>
                                    val tStr = WomTypeSerialization.toString(cVar.womType)
                                    cVar.name -> JsString(tStr)
                            }.toMap)
                    )
                    Some(JsObject("name" -> JsString(Utils.META_INFO),
                                  "class" -> JsString("hash"),
                                  "default" -> hardCodedFragInfo))

                case IR.AppletKindTask(_) =>
                    // meta information used for running workflow fragments
                    val hardCodedTaskInfo = JsObject(
                        "womSourceCode" -> JsString(Utils.base64Encode(applet.womSourceCode)),
                        "instanceTypeDB" -> JsString(Utils.base64Encode(dbInstance))
                    )
                    Some(JsObject("name" -> JsString(Utils.META_INFO),
                                  "class" -> JsString("hash"),
                                  "default" -> hardCodedTaskInfo))

                case IR.AppletKindNative(_) =>
                    None
            }
        val outputSpec : Vector[JsValue] = applet.outputs.map(cVar =>
            cVarToSpec(cVar)
        ).flatten.toVector
        val (runSpec : JsValue, details: JsValue) = calcRunSpec(applet, bashScript)
        val access : JsValue = calcAccess(applet)

         // pack all the core arguments into a single request
        var reqCore = Map(
	    "name" -> JsString(applet.name),
            "inputSpec" -> JsArray(inputSpec ++ metaInfo.toVector),
            "outputSpec" -> JsArray(outputSpec),
            "runSpec" -> runSpec,
            "dxapi" -> JsString("1.0.0"),
            "tags" -> JsArray(JsString("dxWDL"))
        )
        if (details != JsNull)
            reqCore += ("details" -> details)
        if (access != JsNull)
            reqCore += ("access" -> access)

        // Add a checksum
        val (digest, req) = checksumReq(JsObject(reqCore))

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
    private def buildAppletIfNeeded(applet: IR.Applet, execDict: ExecDict) : DXApplet = {
        Utils.trace(verbose2, s"Compiling applet ${applet.name}")

        // limit the applet dictionary, only to actual dependencies
        val calls: Vector[String] = applet.kind match {
            case IR.AppletKindWfFragment(calls, _, _) => calls
            case _ => Vector.empty
        }

        val aplLinks : Map[String, ExecLinkInfo] = calls.map{ tName =>
            val (irCall, dxObj) = execDict(tName)
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
        buildRequired match {
            case None =>
                // Compile a WDL snippet into an applet.
                val rep = DXAPI.appletNew(Utils.jsonNodeOfJsValue(appletApiRequest), classOf[JsonNode])
                val id = apiParseReplyID(rep)
                val dxApplet = DXApplet.getInstance(id)
                dxObjDir.insert(applet.name, dxApplet, digest)
                dxApplet
            case Some(dxObj) =>
                // Old applet exists, and it has not changed. Return the
                // applet-id.
                dxObj.asInstanceOf[DXApplet]
        }
    }

    // Calculate the stage inputs from the call closure
    //
    // It comprises mappings from variable name to WomType.
    private def genStageInputs(inputs: Vector[(CVar, SArg)]) : JsValue = {
        val jsInputs:Map[String, JsValue] = inputs.foldLeft(Map.empty[String, JsValue]){
            case (m, (cVar, sArg)) =>
                sArg match {
                    case IR.SArgEmpty =>
                        // We do not have a value for this input at compile time.
                        // For compulsory applet inputs, the user will have to fill
                        // in a value at runtime.
                        m
                    case IR.SArgConst(wValue) =>
                        val wvl = WdlVarLinks.importFromWDL(cVar.womType, wValue)
                        val fields = WdlVarLinks.genFields(wvl, cVar.dxVarName)
                        m ++ fields.toMap
                    case IR.SArgLink(dxStage, argName) =>
                        val wvl = WdlVarLinks(cVar.womType,
                                              DxlStage(dxStage, IORef.Output, argName.dxVarName))
                        val fields = WdlVarLinks.genFields(wvl, cVar.dxVarName)
                        m ++ fields.toMap
                    case IR.SArgWorkflowInput(argName) =>
                        val wvl = WdlVarLinks(cVar.womType,
                                              DxlWorkflowInput(argName.dxVarName))
                        val fields = WdlVarLinks.genFields(wvl, cVar.dxVarName)
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
                WdlVarLinks.genFields(wvl, cVar.dxVarName)
            case IR.SArgWorkflowInput(argName: CVar) =>
                val wvl = WdlVarLinks(cVar.womType,
                                      DxlWorkflowInput(argName.dxVarName))
                WdlVarLinks.genFields(wvl, cVar.dxVarName)
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

    // Create the workflow in a single API call.
    // - Prepare the list of stages, and the checksum in
    //   advance.
    //
    private def buildWorkflow(wf: IR.Workflow,
                              digest: String,
                              execDict: ExecDict) : DXWorkflow = {
        trace(verbose2, s"build workflow ${wf.name}")
        val stagesReq =
            wf.stages.foldLeft(Vector.empty[JsValue]) {
                case (stagesReq, stg) =>
                    val (irApplet,dxExec) = execDict(stg.calleeName)
                    val linkedInputs : Vector[(CVar, SArg)] = irApplet.inputVars zip stg.inputs
                    val inputs = genStageInputs(linkedInputs)
                    // convert the per-stage metadata into JSON
                    val stageReqDesc = JsObject(
                        "id" -> JsString(stg.id.getId),
                        "executable" -> JsString(dxExec.getId),
                        "name" -> JsString(stg.description),
                        "input" -> inputs)
                    stagesReq :+ stageReqDesc
            }

        // pack all the arguments into a single API call
        val reqFields = Map("project" -> JsString(dxProject.getId),
                            "name" -> JsString(wf.name),
                            "folder" -> JsString(folder),
                            "properties" -> JsObject(CHECKSUM_PROP -> JsString(digest)),
                            "stages" -> JsArray(stagesReq),
                            "tags" -> JsArray(JsString("dxWDL")))

        val wfInputOutput: Map[String, JsValue] =
            if (wf.locked) {
                // Locked workflows have well defined inputs and outputs
                val wfInputSpec:Vector[JsValue] = wf.inputs.map{ case (cVar,sArg) =>
                    buildWorkflowInputSpec(cVar, sArg)
                }.flatten
                val wfOutputSpec:Vector[JsValue] = wf.outputs.map{ case (cVar,sArg) =>
                    buildWorkflowOutputSpec(cVar, sArg)
                }.flatten

                if (verbose2) {
                    val inputSpecDbg = wfInputSpec.map("    " + _.toString).mkString("\n")
                    trace(verbose2, s"""|input spec
                                        |${inputSpecDbg}""".stripMargin)
                    val outputSpecDbg = wfOutputSpec.map("    " + _.toString).mkString("\n")
                    trace(verbose2, s"""|output spec
                                        |${outputSpecDbg}""".stripMargin)
                }

                Map("inputs" -> JsArray(wfInputSpec),
                    "outputs" -> JsArray(wfOutputSpec))
            } else {
                Map.empty
            }

        // pack all the arguments into a single API call
        val req = JsObject(reqFields ++ wfInputOutput)
        val rep = DXAPI.workflowNew(jsonNodeOfJsValue(req), classOf[JsonNode])
        val id = apiParseReplyID(rep)
        val dxwf = DXWorkflow.getInstance(id)

        // Close the workflow
        dxwf.close()
        dxwf
    }

    // Compile an entire workflow
    //
    // - Calculate the workflow checksum from the intermediate representation
    // - Do not rebuild the workflow if it has a correct checksum
    private def buildWorkflowIfNeeded(wf: IR.Workflow,
                                      execDict: ExecDict) : DXWorkflow = {
        // the workflow digest depends on the IR and the applets
        val digest:String = chksum(
            List(wf.toString, execDict.toString).mkString("\n")
        )
        val buildRequired = isBuildRequired(wf.name, digest)
        buildRequired match {
            case None =>
                val dxWorkflow = buildWorkflow(wf, digest, execDict)
                dxObjDir.insert(wf.name, dxWorkflow, digest)
                dxWorkflow
            case Some(dxObj) =>
                // Old workflow exists, and it has not changed.
                dxObj.asInstanceOf[DXWorkflow]
        }
    }

    def apply(bundle: IR.Bundle) : CompilationResults = {
        Utils.trace(verbose.on, "Native pass, generate dx:applets and dx:workflows")
        Utils.traceLevelInc()

        // build applets and workflows if they aren't on the platform already
        val execDict = bundle.dependencies.foldLeft(execDictEmpty) {
            case (accu, cName) =>
                val execIr = bundle.allCallables(cName)
                execIr match {
                    case apl: IR.Applet =>
                        val id = apl.kind match {
                            case IR.AppletKindNative(id) => id
                            case _ =>
                                val dxApplet = buildAppletIfNeeded(apl, accu)
                                dxApplet.getId
                        }
                        accu + (apl.name -> (apl, DxExec(id)))
                    case wf : IR.Workflow =>
                        val dxwfl = buildWorkflowIfNeeded(wf, accu)
                        accu + (wf.name -> (wf, DxExec(dxwfl.getId)))
                }
        }

        // build the toplevel workflow, if it is defined
        val primary : Option[DxExec] = bundle.primaryCallable.flatMap{ callable =>
            execDict.get(callable.name) match {
                case None => None
                case Some((_, dxExec)) => Some(dxExec)
            }
        }

        Utils.traceLevelDec()
        CompilationResults(primary,
                           execDict.map{ case (name, (_,dxExec)) => name -> dxExec }.toMap)
    }
}
