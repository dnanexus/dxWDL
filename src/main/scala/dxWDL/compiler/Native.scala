/** Generate a dx:worflow and dx:applets from an intermediate representation.
  */
package dxWDL.compiler

// DX bindings
import com.fasterxml.jackson.databind.JsonNode
import com.dnanexus._
import dxWDL._
import dxWDL.Utils._
import java.security.MessageDigest
import IR.{CVar}
import scala.collection.JavaConverters._
import spray.json._
import wom.types._

case class Native(dxWDLrtId: String,
                  folder: String,
                  dxProject: DXProject,
                  dxObjDir: DxObjectDirectory,
                  instanceTypeDB: InstanceTypeDB,
                  extras: Option[Extras],
                  runtimeDebugLevel: Option[Int],
                  leaveWorkflowsOpen: Boolean,
                  force: Boolean,
                  archive: Boolean,
                  locked: Boolean,
                  verbose: Verbose) {
    type ExecDict = Map[String, (IR.Callable, DxExec)]
    val execDictEmpty = Map.empty[String, (IR.Callable, DxExec)]

    val verbose2:Boolean = verbose.keywords contains "native"
    val rtDebugLvl = runtimeDebugLevel.getOrElse(Utils.DEFAULT_RUNTIME_DEBUG_LEVEL)
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

    private def genBashScriptTaskBody(): String = {
        s"""|    # Keep track of streaming files. Each such file
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
    }

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

    private def genBashScript(appKind: IR.AppletKind,
                              instanceType: IR.InstanceType,
                              wdlCode: String,
                              linkInfo: Option[String],
                              dbInstance: Option[String]) : String = {
        val body:String = appKind match {
            case IR.AppletKindNative(_) =>
                throw new Exception("Sanity: generating a bash script for a native applet")
            case IR.AppletKindWfFragment(_) =>
                genBashScriptWfFragment()
            case IR.AppletKindTask =>
                instanceType match {
                    case IR.InstanceTypeDefault | IR.InstanceTypeConst(_,_,_,_) =>
                        s"""|main() {
                            |${genBashScriptTaskBody()}
                            |}""".stripMargin
                    case IR.InstanceTypeRuntime =>
                        s"""|main() {
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
                            |${genBashScriptTaskBody()}
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
                if (archive) {
                    // archive the applet/workflow(s)
                    existingDxObjs.foreach(x => dxObjDir.archiveDxObject(x))
                } else if (force) {
                    // the dx:object exists, and needs to be removed. There
                    // may be several versions, all are removed.
                    val objs = existingDxObjs.map(_.dxObj)
                    trace(verbose.on, s"Removing old ${name} ${objs.map(_.getId)}")
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
        genBashScript(applet.kind, applet.instanceType,
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
    private def calcRunSpec(bashScript: String,
                            iType: IR.InstanceType,
                            docker: IR.DockerImage) : JsValue = {
        // find the dxWDL asset
        val instanceType:String = iType match {
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
        val extraRunSpec : Map[String, JsValue] = extras match {
            case None => Map.empty
            case Some(ext) => ext.defaultTaskDxAttributes match {
                case None => Map.empty
                case Some(dta) => dta.toRunSpecJson
            }
        }
        val runSpecWithExtras = runSpec ++ extraRunSpec

        // If the docker image is a platform asset,
        // add it to the asset-depends.
        val dockerAssets: Option[JsValue] = docker match {
            case IR.DockerImageNone => None
            case IR.DockerImageNetwork => None
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
                Some(JsObject("name" -> JsString(pkgName),
                              "id" -> jsValueOfJsonNode(pkgFile.getLinkAsJson)))
        }
        val bundledDepends = dockerAssets match {
            case None => Vector(runtimeLibrary)
            case Some(img) => Vector(runtimeLibrary, img)
        }
        JsObject(runSpecWithExtras +
                     ("bundledDepends" -> JsArray(bundledDepends)))
    }

    def  calcAccess(applet: IR.Applet) : JsValue = {
        val extraAccess: DxAccess = extras match {
            case None => DxAccess.empty
            case Some(ext) => ext.defaultTaskDxAttributes match {
                case None => DxAccess.empty
                case Some(dta) => dta.access match {
                    case None => DxAccess.empty
                    case Some(access) => access
                }
            }
        }
        val access: DxAccess = applet.kind match {
            case IR.AppletKindTask =>
                if (applet.docker == IR.DockerImageNetwork) {
                    // docker requires network access, because we are downloading
                    // the image from the network
                    extraAccess.merge(DxAccess(Some(Vector("*")), None,  None,  None,  None))
                } else {
                    extraAccess
                }
            case IR.AppletKindWorkflowOutputReorg =>
                // The WorkflowOutput applet requires higher permissions
                // to organize the output directory.
                DxAccess(None, Some(AccessLevel.CONTRIBUTE), None, None, None)
            case _ =>
                // Even scatters need network access, because
                // they spawn subjobs that (may) use dx-docker.
                // We end up allowing all applets to use the network
                extraAccess.merge(DxAccess(Some(Vector("*")), None,  None,  None,  None))
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
        val runSpec : JsValue = calcRunSpec(bashScript, applet.instanceType, applet.docker)
        val access : JsValue = calcAccess(applet)

        // pack all the core arguments into a single request
        val reqCore = Map(
            "name" -> JsString(applet.name),
            "inputSpec" -> JsArray(inputSpec),
            "outputSpec" -> JsArray(outputSpec),
            "runSpec" -> runSpec,
            "dxapi" -> JsString("1.0.0"),
            "tags" -> JsArray(JsString("dxWDL"))
        )
        val reqWithAccess =
            if (access == JsNull)
                JsObject(reqCore)
            else
                JsObject(reqCore ++ Map("access" -> access))

        // Add a checksum
        val (digest, req) = checksumReq(reqWithAccess)

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

    def compile(bundle: IR.Bundle) : CompilationResults = {

        // build applets and workflows if they aren't on the platform already
        val execDict = bundle.allCallables.foldLeft(execDictEmpty) {
            case (accu, execIr) =>
                execIr match {
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
        /*val entrypoint = bundle.entrypoint.map{ wf =>
            buildWorkflowIfNeeded(wf, execDict)
        }*/

        CompilationResults(None,
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
              extras: Option[Extras],
              runtimeDebugLevel: Option[Int],
              leaveWorkflowsOpen: Boolean,
              force: Boolean,
              archive: Boolean,
              locked: Boolean,
              verbose: Verbose) : CompilationResults = {
        trace(verbose.on, "Native pass, generate dx:applets and dx:workflows")
        traceLevelInc()

        val ntv = new Native(dxWDLrtId, folder, dxProject, dxObjDir, instanceTypeDB,
                             extras, runtimeDebugLevel,
                             leaveWorkflowsOpen, force, archive, locked, verbose)
        val retval = ntv.compile(ns)
        traceLevelDec()
        retval
    }
}
