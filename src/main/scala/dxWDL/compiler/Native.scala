/** Generate a dx:worflow and dx:applets from an intermediate representation.
  */
package dxWDL.compiler

// DX bindings
import com.fasterxml.jackson.databind.JsonNode
import com.dnanexus._
import dxWDL._
import dxWDL.Utils._
import java.security.MessageDigest
import IR.{CVar, SArg}
import scala.collection.JavaConverters._
import spray.json._
import wom.types._

case class Native(dxWDLrtId: String,
                  folder: String,
                  dxProject: DXProject,
                  dxObjDir: DxObjectDirectory,
                  instanceTypeDB: InstanceTypeDB,
                  cOpt: CompilerOptions) {
    type ExecDict = Map[String, (IR.Callable, DxExec)]
    val execDictEmpty = Map.empty[String, (IR.Callable, DxExec)]

    val verbose = cOpt.verbose
    val verbose2:Boolean = verbose.keywords contains "native"
    val rtDebugLvl = cOpt.runtimeDebugLevel.getOrElse(Utils.DEFAULT_RUNTIME_DEBUG_LEVEL)

    // Are we setting up a private docker registry?
    val dockerRegistryInfo : Option[DockerRegistry]= cOpt.extras match {
        case None => None
        case Some(extras) =>
            extras.dockerRegistry match {
                case None => None
                case Some(x) => Some(x)
            }
    }

    lazy val runtimeLibrary:JsValue = getRuntimeLibrary()

    // Open the archive
    // Extract the archive from the details field
    private def getRuntimeLibrary(): JsValue = {
        val record = DXRecord.getInstance(dxWDLrtId)
        val descOptions = DXDataObject.DescribeOptions.get().inProject(dxProject).withDetails
        val details = jsValueOfJsonNode(
            record.describe(descOptions).getDetails(classOf[JsonNode]))
        val dxLink = details.asJsObject.fields.get("archiveFileId") match {
            case Some(x) => x
            case None => throw new Exception(s"record does not have an archive field ${details}")
        }
        val dxFile = dxFileFromJsValue(dxLink)
        val name = dxFile.describe.getName()
        JsObject(
            "name" -> JsString(name),
            "id" -> JsObject("$dnanexus_link" -> JsString(dxFile.getId()))
        )
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
        val name = Utils.encodeAppletVarName(cVar.dxVarName)
        val defaultVals:Map[String, JsValue] = cVar.attrs.getDefault match {
            case None => Map.empty
            case Some(wdlValue) =>
                val wvl = WdlVarLinks.importFromWDL(cVar.womType,
                                                    DeclAttrs.empty,
                                                    wdlValue,
                                                    IODirection.Zero)
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
                   JsObject(Map("name" -> JsString(name + FLAT_FILES_SUFFIX),
                                "class" -> JsString("array:file"),
                                "optional" -> JsBoolean(true))
                                ++ jsMapFromDefault(name + FLAT_FILES_SUFFIX)))
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

    private def genSourceFiles(wdlCode:String,
                               linkInfo: Option[String],
                               dbInstance: Option[String]): String = {
        // UU64 encode the WDL script to avoid characters that interact
        // badly with bash
        val wdlCodeUu64 = base64Encode(wdlCode)
        val part1 =
            s"""|
                |    # write the WDL script into a file
                |    cat >$${DX_FS_ROOT}/source.wdl.uu64 <<'EOL'
                |${wdlCodeUu64}
                |EOL
                |# decode the WDL script
                |base64 -d $${DX_FS_ROOT}/source.wdl.uu64 > $${DX_FS_ROOT}/source.wdl
                |""".stripMargin.trim

        val part2 = linkInfo.map{ info =>
            s"""|    # write the linking information into a file
                |    cat >$${DX_FS_ROOT}/${LINK_INFO_FILENAME} <<'EOL'
                |${info}
                |EOL
                |""".stripMargin.trim
        }

        val part3 = dbInstance.map { db =>
            s"""|    # write the instance type DB
                |    cat >$${DX_FS_ROOT}/${INSTANCE_TYPE_DB_FILENAME} <<'EOL'
                |${db}
                |EOL
                |""".stripMargin.trim
        }

        List(Some(part1), part2, part3).flatten.mkString("\n")
    }

    private def dockerPreamble(dockerImage: IR.DockerImage) : String = {
        val dockerCmd = dockerImage match {
            case _ if cOpt.nativeDocker => "docker"
            case IR.DockerImageNetwork if dockerRegistryInfo != None => "docker"
            case _ => "dx-docker"
        }
        val exportBashVars = dockerRegistryInfo match {
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
                s"""|# Docker private registry information
                    |export DOCKER_REGISTRY=${registry}
                    |export DOCKER_USERNAME=${username}
                    |export DOCKER_CREDENTIALS=${credentialsWithoutPrefix}
                    |""".stripMargin
        }
        s"""|# Docker definitions
            |export DOCKER_CMD=${dockerCmd}
            |${exportBashVars}
            |""".stripMargin
    }

    val bashScriptTaskBody: String =
        s"""|    # if we need to set up a private docker registry,
            |    # download the credentials file and login. Do not expose the
            |    # credentials to the logs or to stdout.
            |    if [[ -n $${DOCKER_REGISTRY} ]]; then
            |        echo "Logging in to docker registry $${DOCKER_REGISTRY}, as user $${DOCKER_USERNAME}"
            |
            |        # there has to be a single credentials file
            |        num_lines=$$(dx ls $${DOCKER_CREDENTIALS} | wc --lines)
            |        if [[ $$num_lines != 1 ]]; then
            |            echo "There has to be exactly one credentials file, found $$num_lines."
            |            dx ls -l $${DOCKER_CREDENTIALS}
            |            exit 1
            |        fi
            |        dx download $${DOCKER_CREDENTIALS} -o $${HOME}/docker_credentials
            |        cat $${HOME}/docker_credentials | docker login $${DOCKER_REGISTRY} -u $${DOCKER_USERNAME} --password-stdin
            |        rm -f $${HOME}/docker_credentials
            |    fi
            |
            |    # Keep track of streaming files. Each such file
            |    # is converted into a fifo, and a 'dx cat' process
            |    # runs in the background.
            |    background_pids=()
            |
            |    # evaluate input arguments, and download input files
            |    java -jar $${DX_FS_ROOT}/dxWDL.jar internal taskProlog $${DX_FS_ROOT}/source.wdl $${HOME} ${rtDebugLvl}
            |
            |    # setup any file streams. Keep track of background
            |    # processes in the 'background_pids' array.
            |    # We 'source' the sub-script here, because we
            |    # need to wait for the pids. This can only be done
            |    # for child processes (not grand-children).
            |    if [[ -e $${HOME}/execution/meta/setup_streams ]]; then
            |       source $${HOME}/execution/meta/setup_streams > $${HOME}/execution/meta/background_pids.txt
            |
            |       # reads the file line by line, and convert into a bash array
            |       mapfile -t background_pids < $${HOME}/execution/meta/background_pids.txt
            |       echo "Background processes ids: $${background_pids[@]}"
            |    fi
            |
            |    # Run the shell script generated by the prolog.
            |    # Capture the stderr/stdout in files
            |    if [[ -e $${HOME}/execution/meta/script.submit ]]; then
            |        echo "docker submit script:"
            |        cat $${HOME}/execution/meta/script.submit
            |        $${HOME}/execution/meta/script.submit
            |    else
            |        /bin/bash $${HOME}/execution/meta/script
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
            |    rc=`cat $${HOME}/execution/meta/rc`
            |    if [[ $$rc != 0 ]]; then
            |        exit $$rc
            |    fi
            |
            |    # evaluate applet outputs, and upload result files
            |    java -jar $${DX_FS_ROOT}/dxWDL.jar internal taskEpilog $${DX_FS_ROOT}/source.wdl $${HOME} ${rtDebugLvl}
            |""".stripMargin.trim

    private def genBashScriptNonTask(miniCmd:String) : String = {
        s"""|main() {
            |    java -jar $${DX_FS_ROOT}/dxWDL.jar internal ${miniCmd} $${DX_FS_ROOT}/source.wdl $${HOME} ${rtDebugLvl}
            |}""".stripMargin.trim
    }

    private def genBashScriptWfFragment() : String = {
        s"""|main() {
            |    java -jar $${DX_FS_ROOT}/dxWDL.jar internal wfFragment $${DX_FS_ROOT}/source.wdl $${HOME} ${rtDebugLvl}
            |}
            |
            |collect() {
            |    java -jar $${DX_FS_ROOT}/dxWDL.jar internal collect $${DX_FS_ROOT}/source.wdl $${HOME} ${rtDebugLvl}
            |}""".stripMargin.trim
    }

    private def genBashScript(applet: IR.Applet,
                              wdlCode: String,
                              linkInfo: Option[String],
                              dbInstance: Option[String]) : String = {
        val body:String = applet.kind match {
            case IR.AppletKindNative(_) =>
                throw new Exception("Sanity: generating a bash script for a native applet")
            case IR.AppletKindWfFragment(_) =>
                genBashScriptWfFragment()
            case IR.AppletKindTask =>
                applet.instanceType match {
                    case IR.InstanceTypeDefault | IR.InstanceTypeConst(_,_,_,_) =>
                        s"""|${dockerPreamble(applet.docker)}
                            |
                            |main() {
                            |${bashScriptTaskBody}
                            |}""".stripMargin
                    case IR.InstanceTypeRuntime =>
                        s"""|${dockerPreamble(applet.docker)}
                            |
                            |main() {
                            |    # check if this is the correct instance type
                            |    correctInstanceType=`java -jar $${DX_FS_ROOT}/dxWDL.jar internal taskCheckInstanceType $${DX_FS_ROOT}/source.wdl $${HOME} ${rtDebugLvl}`
                            |    if [[ $$correctInstanceType == "true" ]]; then
                            |        body
                            |    else
                            |       # evaluate the instance type, and launch a sub job on it
                            |       java -jar $${DX_FS_ROOT}/dxWDL.jar internal taskRelaunch $${DX_FS_ROOT}/source.wdl $${HOME} ${rtDebugLvl}
                            |    fi
                            |}
                            |
                            |# We are on the correct instance type, run the task
                            |body() {
                            |${bashScriptTaskBody}
                            |}""".stripMargin.trim
                }
            case IR.AppletKindWorkflowOutputReorg =>
                genBashScriptNonTask("workflowOutputReorg")
        }
        val setupFilesScript = genSourceFiles(wdlCode, linkInfo, dbInstance)
        s"""|#!/bin/bash -ex
            |${setupFilesScript}
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
                trace(verbose.on, s"Found existing version of ${name} in folder ${desc.getFolder}")
                return Some(dxObj)
        }

        val existingDxObjs = dxObjDir.lookup(name)
        val buildRequired:Boolean = existingDxObjs.size match {
            case 0 => true
            case 1 =>
                // Check if applet code has changed
                val dxObjInfo = existingDxObjs.head
                val dxClass:String = dxObjInfo.dxClass
                if (digest != dxObjInfo.digest) {
                    trace(verbose.on, s"${dxClass} ${name} has changed, rebuild required")
                    true
                } else {
                    trace(verbose.on, s"${dxClass} ${name} has not changed")
                    false
                }
            case _ =>
                val dxClass = existingDxObjs.head.dxClass
                warning(verbose, s"""|More than one ${dxClass} ${name} found in
                                     |path ${dxProject.getId()}:${folder}""".stripMargin)
                true
        }

        if (buildRequired) {
            if (existingDxObjs.size > 0) {
                if (cOpt.archive) {
                    // archive the applet/workflow(s)
                    existingDxObjs.foreach(x => dxObjDir.archiveDxObject(x))
                } else if (cOpt.force) {
                    // the dx:object exists, and needs to be removed. There
                    // may be several versions, all are removed.
                    val objs = existingDxObjs.map(_.dxObj)
                    trace(verbose.on, s"Removing old ${name} ${objs.map(_.getId)}")
                    dxProject.removeObjects(objs.asJava)
                } else {
                    val dxClass = existingDxObjs.head.dxClass
                    throw new Exception(s"""|${dxClass} ${name} already exists in
                                            |${dxProject.getId}:${folder}.
                                            |""".stripMargin.replaceAll("\n", " "))
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
            case CVar(name, wdlType, _, _) => (name -> wdlType)
        }.toMap
        val callOutputDefs: Map[String, WomType] = irCall.outputVars.map{
            case CVar(name, wdlType, _, _) => (name -> wdlType)
        }.toMap
        ExecLinkInfo(callInputDefs,
                     callOutputDefs,
                     dxObj)
    }

    // Bundle all the applet code into one bash script.
    //
    // For applets that call other applets, we pass a directory
    // of the callees, so they could be found a runtime. This is
    // equivalent to linking, in a standard C compiler.
    private def genAppletScript(applet: IR.Applet, aplLinks: ExecDict) : String = {
        // generate the wdl source file
        val wdlCode:String = WdlPrettyPrinter(false, None).apply(applet.ns, 0)
            .mkString("\n")

        // create linking information
        val linkInfo:Option[String] =
            if (aplLinks.isEmpty) {
                None
            } else {
                val linkInfo = JsObject(
                    aplLinks.map{ case (key, (irCall, dxObj)) =>
                        // Reduce the information to what will be needed for runtime linking.
                        val ali = genLinkInfo(irCall, dxObj)
                        key -> ExecLinkInfo.writeJson(ali)
                    }.toMap)
                Some(linkInfo.prettyPrint)
            }

        // Add the pricing model, and make the prices
        // opaque.
        val dbOpaque = InstanceTypeDB.opaquePrices(instanceTypeDB)
        val dbInstance = dbOpaque.toJson.prettyPrint

        // write the bash script
        genBashScript(applet,
                      wdlCode, linkInfo, Some(dbInstance))
    }

    private def apiParseReplyID(rep: JsonNode) : String = {
        val repJs:JsValue = jsValueOfJsonNode(rep)
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
            "release" -> JsString(UBUNTU_VERSION),
        )

        // Start with the default dx-attribute section, and override
        // any field that is specified in the individual task section.
        val extraRunSpec : Map[String, JsValue] = cOpt.extras match {
            case None => Map.empty
            case Some(ext) => ext.defaultTaskDxAttributes match {
                case None => Map.empty
                case Some(dta) => dta.toRunSpecJson
            }
        }
        val taskSpecificRunSpec : Map[String, JsValue] =
            if (applet.kind == IR.AppletKindTask) {
                // A task can override the default dx attributes
                cOpt.extras match {
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

        // - If the docker image is a platform asset, add it to the asset-depends.
        // - If the docker image is a tarball, add a link in the details field.
        val (dockerAsset: Option[JsValue], dockerFile: Option[DXFile]) = applet.docker match {
            case IR.DockerImageNone => (None, None)
            case IR.DockerImageNetwork => (None, None)
            case IR.DockerImageDxAsset(dxRecord) =>
                val desc = dxRecord.describe(DXDataObject.DescribeOptions.get.withDetails)

                // extract the archiveFileId field
                val details:JsValue = jsValueOfJsonNode(desc.getDetails(classOf[JsonNode]))
                val pkgFile:DXFile = details.asJsObject.fields.get("archiveFileId") match {
                    case Some(id) => dxFileFromJsValue(id)
                    case _ => throw new Exception(s"Badly formatted record ${dxRecord}")
                }
                val pkgName = pkgFile.describe.getName

                // Check if the asset points to a different
                // project. If so, make sure we have an asset clone
                // in -this- project.
                val rmtContainer = desc.getProject
                if (!rmtContainer.isInstanceOf[DXProject])
                    throw new Exception(s"remote asset is in container ${rmtContainer.getId}, not a project")
                val rmtProject = rmtContainer.asInstanceOf[DXProject]
                Utils.cloneAsset(dxRecord, dxProject, pkgName, rmtProject, verbose)
                val asset = JsObject("name" -> JsString(pkgName),
                                     "id" -> jsValueOfJsonNode(pkgFile.getLinkAsJson))
                (Some(asset), None)

            case IR.DockerImageDxFile(dxfile) =>
                // A docker image stored as a tar ball in a platform file
                (None, Some(dxfile))
        }
        val bundledDepends = dockerAsset match {
            case None => Vector(runtimeLibrary)
            case Some(img) => Vector(runtimeLibrary, img)
        }
        val runSpecEverything = JsObject(runSpecWithExtras +
                                   ("bundledDepends" -> JsArray(bundledDepends)))
        val details = dockerFile match {
            case None => JsNull
            case Some(dxfile) =>
                JsObject("details" ->
                             JsObject("docker-image" -> Utils.dxFileToJsValue(dxfile)))
        }
        (runSpecEverything, details)
    }

    def calcAccess(applet: IR.Applet) : JsValue = {
        val extraAccess: DxAccess = cOpt.extras match {
            case None => DxAccess.empty
            case Some(ext) => ext.getDefaultAccess
        }
        val taskSpecificAccess : DxAccess =
            if (applet.kind == IR.AppletKindTask) {
                // A task can override the default dx attributes
                cOpt.extras match {
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
            case IR.AppletKindTask =>
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
    private def appletNewReq(applet: IR.Applet,
                             bashScript: String,
                             folder : String) : (String, JsValue) = {
        trace(verbose2, s"Building /applet/new request for ${applet.name}")

        val inputSpec : Vector[JsValue] = applet.inputs.map(cVar =>
            cVarToSpec(cVar)
        ).flatten.toVector
        val outputSpec : Vector[JsValue] = applet.outputs.map(cVar =>
            cVarToSpec(cVar)
        ).flatten.toVector
        val (runSpec : JsValue, details: JsValue) = calcRunSpec(applet, bashScript)
        val access : JsValue = calcAccess(applet)

        // pack all the core arguments into a single request
        var reqCore = Map(
            "name" -> JsString(applet.name),
            "inputSpec" -> JsArray(inputSpec),
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
        trace(verbose2, s"Compiling applet ${applet.name}")

        // limit the applet dictionary, only to actual dependencies
        val calls:Map[String, String] = applet.kind match {
            case IR.AppletKindWfFragment(calls) => calls
            case _ => Map.empty
        }
        val aplLinks = calls.map{ case (_,tName) => tName -> execDict(tName) }.toMap

        // Build an applet script
        val bashScript = genAppletScript(applet, aplLinks)

        // Calculate a checksum of the inputs that went into the
        // making of the applet.
        val (digest,appletApiRequest) = appletNewReq(applet, bashScript, folder)
        if (verbose2) {
            val fName = s"${applet.name}_req.json"
            val trgPath = Utils.appCompileDirPath.resolve(fName)
            Utils.writeFileContent(trgPath, appletApiRequest.prettyPrint)
        }

        val buildRequired = isBuildRequired(applet.name, digest)
        buildRequired match {
            case None =>
                // Compile a WDL snippet into an applet.
                val rep = DXAPI.appletNew(jsonNodeOfJsValue(appletApiRequest), classOf[JsonNode])
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
    private def genStageInputs(inputs: Vector[(CVar, SArg)],
                               stageDict: Map[String, DXWorkflowStage]) : JsValue = {
        val jsInputs:Map[String, JsValue] = inputs.foldLeft(Map.empty[String, JsValue]){
            case (m, (cVar, sArg)) =>
                sArg match {
                    case IR.SArgEmpty =>
                        // We do not have a value for this input at compile time.
                        // For compulsory applet inputs, the user will have to fill
                        // in a value at runtime.
                        m
                    case IR.SArgConst(wValue) =>
                        val wvl = WdlVarLinks.importFromWDL(cVar.womType, cVar.attrs, wValue, IODirection.Zero)
                        val fields = WdlVarLinks.genFields(wvl, cVar.dxVarName)
                        m ++ fields.toMap
                    case IR.SArgLink(stageName, argName) =>
                        val dxStage = stageDict(stageName)
                        val wvl = WdlVarLinks(cVar.womType,
                                              cVar.attrs,
                                              DxlStage(dxStage, IORef.Output, argName.dxVarName))
                        val fields = WdlVarLinks.genFields(wvl, cVar.dxVarName)
                        m ++ fields.toMap
                    case IR.SArgWorkflowInput(argName) =>
                        val wvl = WdlVarLinks(cVar.womType,
                                              cVar.attrs,
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
                                       sArg:SArg,
                                       stageDict: Map[String, DXWorkflowStage])
            : Vector[JsValue] = {
        // deal with default values
        val attrs:DeclAttrs = sArg match {
            case IR.SArgWorkflowInput(_) =>
                // input is provided by the user
                DeclAttrs.empty
            case IR.SArgConst(wdlValue) =>
                DeclAttrs.empty.setDefault(wdlValue)
            case other =>
                throw new Exception(s"Bad value for sArg ${other}")
        }
        val allAttrs = attrs.merge(cVar.attrs)
        val cVarWithDflt = cVar.copy(attrs = allAttrs)
        cVarToSpec(cVarWithDflt)
    }

    // Note: a single WDL output can generate one or two JSON outputs.
    private def buildWorkflowOutputSpec(cVar:CVar,
                                        sArg:SArg,
                                        stageDict: Map[String, DXWorkflowStage])
            : Vector[JsValue] = {
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
            case IR.SArgLink(stageName, argName: CVar) =>
                // output is from an intermediate stage
                val dxStage = stageDict(stageName)
                val wvl = WdlVarLinks(cVar.womType,
                                      cVar.attrs,
                                      DxlStage(dxStage, IORef.Output, argName.dxVarName))
                WdlVarLinks.genFields(wvl, cVar.dxVarName)
            case IR.SArgWorkflowInput(argName: CVar) =>
                val wvl = WdlVarLinks(cVar.womType,
                                      cVar.attrs,
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
        val (stagesReq, stageDict) =
            wf.stages.foldLeft((Vector.empty[JsValue], Map.empty[String, DXWorkflowStage])) {
                case ((stagesReq, stageDict), stg) =>
                    val (irApplet,dxExec) = execDict(stg.calleeName)
                    val linkedInputs : Vector[(CVar, SArg)] = irApplet.inputVars zip stg.inputs
                    val inputs = genStageInputs(linkedInputs, stageDict)
                    // convert the per-stage metadata into JSON
                    val stageReqDesc = JsObject(
                        "id" -> JsString(stg.id.getId),
                        "executable" -> JsString(dxExec.getId),
                        "name" -> JsString(stg.description.getOrElse(stg.stageName)),
                        "input" -> inputs)
                    (stagesReq :+ stageReqDesc,
                     stageDict ++ Map(stg.stageName -> stg.id))
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
                    buildWorkflowInputSpec(cVar, sArg, stageDict)
                }.flatten
                val wfOutputSpec:Vector[JsValue] = wf.outputs.map{ case (cVar,sArg) =>
                    buildWorkflowOutputSpec(cVar, sArg, stageDict)
                }.flatten
                trace(verbose2, s"workflow input spec=${wfInputSpec}")
                trace(verbose2, s"workflow output spec=${wfOutputSpec}")

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
        if (!cOpt.leaveWorkflowsOpen)
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
            List(IR.yaml(wf).prettyPrint,
                 execDict.toString).mkString("\n")
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

    // Sort the executables according to dependencies. The lowest
    // ones are tasks, because they depend on nothing else. Higher up
    // are workflows and generated applets like scatters.
    private def sortByDependencies(allExecs: Vector[IR.Callable]) : Vector[IR.Callable] = {
        val allExecNames = allExecs.map(_.name).toSet

        // Figure out the immediate dependencies for this executable
        def immediateDeps(exec: IR.Callable) :Set[String] = {
            val depsRaw = exec match {
                case apl: IR.Applet =>
                    // A generated applet requires all the executables it calls
                    apl.kind match {
                        case IR.AppletKindWfFragment(calls) => calls.values
                        case _ => Vector.empty
                    }
                case wf: IR.Workflow =>
                    // A workflow depends on all the executables it calls
                    wf.stages.map{ _.calleeName}
            }
            val deps: Set[String] = depsRaw.toSet

            // Make sure all the dependencies are actually exist
            assert(deps.subsetOf(allExecNames))
            deps
        }

        // Find executables such that all of their dependencies are
        // satisfied. These can be compiled.
        def next(execs: Vector[IR.Callable],
                 ready: Vector[IR.Callable]) : Vector[IR.Callable] = {
            val readyNames = ready.map(_.name).toSet
            val satisfiedExecs = execs.filter{ e =>
                val deps = immediateDeps(e)
                deps.subsetOf(readyNames)
            }
            if (satisfiedExecs.isEmpty)
                throw new Exception("Sanity: cannot find the next executable to compile.")
            satisfiedExecs
        }

        var accu = Vector.empty[IR.Callable]
        var crnt = allExecs
        while (!crnt.isEmpty) {
            val execsToCompile = next(crnt, accu)
            accu = accu ++ execsToCompile
            val alreadyCompiled: Set[String] = accu.map(_.name).toSet
            crnt = crnt.filter{ exec => !(alreadyCompiled contains exec.name) }
        }
        assert(accu.length == allExecs.length)
        accu
    }

    def compile(ns: IR.Namespace) : CompilationResults = {
        // sort the applets and subworkflows by dependencies
        val executables = sortByDependencies((ns.applets ++ ns.subWorkflows).values.toVector)

        // build applets and workflows if they aren't on the platform already
        val execDict = executables.foldLeft(execDictEmpty) {
            case (accu, execIr) =>
                execIr match {
                    case irwf: IR.Workflow =>
                        val dxwfl = buildWorkflowIfNeeded(irwf, accu)
                        accu + (irwf.name -> (irwf, DxExec(dxwfl.getId)))
                    case irapl: IR.Applet =>
                        val id = irapl.kind match {
                            case IR.AppletKindNative(id) => id
                            case _ =>
                                val dxApplet = buildAppletIfNeeded(irapl, accu)
                                dxApplet.getId
                        }
                        accu + (irapl.name -> (irapl, DxExec(id)))
                }
        }

        // build the toplevel workflow, if it is defined
        val entrypoint = ns.entrypoint.map{ wf =>
            buildWorkflowIfNeeded(wf, execDict)
        }

        CompilationResults(entrypoint,
                           execDict.map{ case (name, (_,dxExec)) => name -> dxExec }.toMap)
    }
}

object Native {
    def apply(ns: IR.Namespace,
              dxWDLrtId: String,
              folder: String,
              dxProject: DXProject,
              instanceTypeDB: InstanceTypeDB,
              dxObjDir: DxObjectDirectory,
              cOpt: CompilerOptions) : CompilationResults = {
        val verbose = cOpt.verbose
        trace(verbose.on, "Native pass, generate dx:applets and dx:workflows")
        traceLevelInc()

        val ntv = new Native(dxWDLrtId, folder, dxProject, dxObjDir, instanceTypeDB,
                             cOpt)
        val retval = ntv.compile(ns)
        traceLevelDec()
        retval
    }
}
