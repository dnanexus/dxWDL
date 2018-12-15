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
    type ExecDict = Map[String, (IR.Callable, DxExec)]
    val execDictEmpty = Map.empty[String, (IR.Callable, DxExec)]

    val verbose2:Boolean = verbose.keywords contains "native"
    val rtDebugLvl = runtimeDebugLevel.getOrElse(Utils.DEFAULT_RUNTIME_DEBUG_LEVEL)

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
        val name = Utils.encodeAppletVarName(cVar.dxVarName)
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

    private def genSourceFiles(wdlCode:String,
                               linkInfo: Option[String],
                               dbInstance: Option[String]): String = {
        // UU64 encode the WDL script to avoid characters that interact
        // badly with bash
        val wdlCodeUu64 = Utils.base64Encode(wdlCode)
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
                |    cat >$${DX_FS_ROOT}/${Utils.LINK_INFO_FILENAME} <<'EOL'
                |${info}
                |EOL
                |""".stripMargin.trim
        }

        val part3 = dbInstance.map { db =>
            s"""|    # write the instance type DB
                |    cat >$${DX_FS_ROOT}/${Utils.INSTANCE_TYPE_DB_FILENAME} <<'EOL'
                |${db}
                |EOL
                |""".stripMargin.trim
        }

        List(Some(part1), part2, part3).flatten.mkString("\n")
    }

    private def genBashScriptTaskBody(): String = {
        s"""|
            |    # evaluate input arguments, and download input files
            |    java -jar $${DX_FS_ROOT}/dxWDL.jar internal taskProlog $${DX_FS_ROOT}/source.wdl $${HOME} ${rtDebugLvl}
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
            |    #  check return code of the script
            |    rc=`cat ${dxPathConfig.rcPath}`
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
            case IR.AppletKindTask(_) =>
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
                Utils.trace(verbose.on, s"Found existing version of ${name} in folder ${desc.getFolder}")
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
                    Utils.trace(verbose.on, s"${dxClass} ${name} has changed, rebuild required")
                    true
                } else {
                    Utils.trace(verbose.on, s"${dxClass} ${name} has not changed")
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
                      applet.womSourceCode, linkInfo, Some(dbInstance))
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
            "release" -> JsString(Utils.UBUNTU_VERSION),
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
            case IR.DockerImageDxAsset(_, dxRecord) =>
                val desc = dxRecord.describe(DXDataObject.DescribeOptions.get.withDetails)

                // extract the archiveFileId field
                val details:JsValue = Utils.jsValueOfJsonNode(desc.getDetails(classOf[JsonNode]))
                val pkgFile:DXFile = details.asJsObject.fields.get("archiveFileId") match {
                    case Some(id) => Utils.dxFileFromJsValue(id)
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
                              "id" -> Utils.jsValueOfJsonNode(pkgFile.getLinkAsJson)))
        }
        runtimeLibrary match {
            case None =>
                // The runtime library is not provided. This is only for testing,
                // because the applet will not be able to run.
                JsObject(runSpecWithExtras)
            case Some(rtLib) =>
                val bundledDepends = dockerAssets match {
                    case None => Vector(rtLib)
                    case Some(img) => Vector(rtLib, img)
                }
                JsObject(runSpecWithExtras +
                             ("bundledDepends" -> JsArray(bundledDepends)))
        }
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
            case IR.AppletKindTask(_) =>
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
        Utils.trace(verbose2, s"Building /applet/new request for ${applet.name}")

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
        Utils.trace(verbose2, s"Compiling applet ${applet.name}")

        // limit the applet dictionary, only to actual dependencies
        val calls: Vector[String] = applet.kind match {
            case IR.AppletKindWfFragment(calls) => calls
            case _ => Vector.empty
        }
        val aplLinks = calls.map{ tName => tName -> execDict(tName) }.toMap

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
                        val wvl = WdlVarLinks.importFromWDL(cVar.womType, wValue)
                        val fields = WdlVarLinks.genFields(wvl, cVar.dxVarName)
                        m ++ fields.toMap
                    case IR.SArgLink(stageName, argName) =>
                        val dxStage = stageDict(stageName)
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
                                       sArg:SArg,
                                       stageDict: Map[String, DXWorkflowStage])
            : Vector[JsValue] = {
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
                        "name" -> JsString(stg.stageName),
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

    def compile(bundle: IR.Bundle) : CompilationResults = {
        // build applets and workflows if they aren't on the platform already

        val execDict = bundle.dependencies.foldLeft(execDictEmpty) {
            case (accu, cName) =>
                System.out.println(accu)
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

        CompilationResults(primary,
                           execDict.map{ case (name, (_,dxExec)) => name -> dxExec }.toMap)
    }
}

object Native {
    def apply(ns: IR.Bundle,
              dxWDLrtId: Option[String],
              folder: String,
              dxProject: DXProject,
              instanceTypeDB: InstanceTypeDB,
              dxPathConfig: DxPathConfig,
              dxObjDir: DxObjectDirectory,
              extras: Option[Extras],
              runtimeDebugLevel: Option[Int],
              leaveWorkflowsOpen: Boolean,
              force: Boolean,
              archive: Boolean,
              locked: Boolean,
              verbose: Verbose) : CompilationResults = {
        Utils.trace(verbose.on, "Native pass, generate dx:applets and dx:workflows")
        Utils.traceLevelInc()

        val ntv = new Native(dxWDLrtId, folder, dxProject, dxObjDir,
                             instanceTypeDB, dxPathConfig,
                             extras, runtimeDebugLevel,
                             leaveWorkflowsOpen, force, archive, locked, verbose)
        val retval = ntv.compile(ns)
        Utils.traceLevelDec()
        retval
    }
}
