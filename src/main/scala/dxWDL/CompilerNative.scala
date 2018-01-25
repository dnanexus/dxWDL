/** Generate a dx:worflow and dx:applets from an intermediate representation.
  */
package dxWDL

// DX bindings
import com.fasterxml.jackson.databind.JsonNode
import com.dnanexus.{DXApplet, DXAPI, DXDataObject, DXFile, DXProject, DXRecord, DXWorkflow}
import java.security.MessageDigest
import java.time.format.DateTimeFormatter
import IR.{CVar, SArg}
import scala.collection.JavaConverters._
import spray.json._
import Utils.{AppletLinkInfo, base64Encode, CHECKSUM_PROP, dxFileFromJsValue, DXWorkflowStage,
    FLAT_FILES_SUFFIX, INSTANCE_TYPE_DB_FILENAME, jsValueOfJsonNode, jsonNodeOfJsValue,
    LINK_INFO_FILENAME, trace, warning}
import wom.types._

case class CompilerNative(dxWDLrtId: String,
                          folder: String,
                          dxProject: DXProject,
                          instanceTypeDB: InstanceTypeDB,
                          force: Boolean,
                          archive: Boolean,
                          locked: Boolean,
                          verbose: Utils.Verbose) {
    val verbose2:Boolean = verbose.keywords contains "compilernative"
    lazy val runtimeLibrary:JsValue = getRuntimeLibrary()
    lazy val projName = dxProject.describe().getName()

    // Open the archive
    // Extract the archive from the details field
    def getRuntimeLibrary(): JsValue = {
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
    def cVarToSpec(cVar: CVar) : Vector[JsValue] = {
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
                case WomBooleanType => mkPrimitive("boolean", false || optional)
                case WomIntegerType => mkPrimitive("int", false || optional)
                case WomFloatType => mkPrimitive("float", false || optional)
                case WomStringType =>mkPrimitive("string", false || optional)
                case WomFileType => mkPrimitive("file", false || optional)

                // single dimension arrays of primitive types
                case WomNonEmptyArrayType(WomBooleanType) => mkPrimitiveArray("boolean", false || optional)
                case WomNonEmptyArrayType(WomIntegerType) => mkPrimitiveArray("int", false || optional)
                case WomNonEmptyArrayType(WomFloatType) => mkPrimitiveArray("float", false || optional)
                case WomNonEmptyArrayType(WomStringType) => mkPrimitiveArray("string", false || optional)
                case WomNonEmptyArrayType(WomFileType) => mkPrimitiveArray("file", false || optional)
                case WomMaybeEmptyArrayType(WomBooleanType) => mkPrimitiveArray("boolean", true)
                case WomMaybeEmptyArrayType(WomIntegerType) => mkPrimitiveArray("int", true)
                case WomMaybeEmptyArrayType(WomFloatType) => mkPrimitiveArray("float", true)
                case WomMaybeEmptyArrayType(WomStringType) => mkPrimitiveArray("string", true)
                case WomMaybeEmptyArrayType(WomFileType) => mkPrimitiveArray("file", true)

                // complex type, that may contains files
                case _ => mkComplex(optional)
            }
        }
        cVar.womType match {
            case WomOptionalType(t) => handleType(t, true)
            case t => handleType(t, false)
        }
    }

    def genSourceFiles(wdlCode:String,
                       linkInfo: Option[String],
                       dbInstance: Option[String]): String = {
        // UU64 encode the WDL script to avoid characters that interact
        // badly with bash
        val wdlCodeUu64 = base64Encode(wdlCode)
        val part1 =
            s"""|    echo "working directory =$${PWD}"
                |    echo "home dir =$${HOME}"
                |
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

    def genBashScriptTaskBody(): String = {
        s"""|    # Keep track of streaming files. Each such file
            |    # is converted into a fifo, and a 'dx cat' process
            |    # runs in the background.
            |    background_pids=()
            |
            |    # evaluate input arguments, and download input files
            |    java -jar $${DX_FS_ROOT}/dxWDL.jar internal taskProlog $${DX_FS_ROOT}/source.wdl $${HOME}
            |    # Debugging outputs
            |    ls -lR
            |    cat $${HOME}/execution/meta/script
            |
            |    # setup any file streams. Keep track of background
            |    # processes in the 'background_pids' array.
            |    # We 'source' the sub-script here, because we
            |    # need to wait for the pids. This can only be done
            |    # for child processes (not grand-children).
            |    if [[ -e $${HOME}/execution/meta/setup_streams ]]; then
            |       source $${HOME}/execution/meta/setup_streams > $${HOME}/execution/meta/background_pids.txt
            |
            |       # reads the file line by line, and converts into a bash array
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
            |    # See what the directory looks like after execution
            |    ls -lR
            |
            |    #  check return code of the script
            |    rc=`cat $${HOME}/execution/meta/rc`
            |    if [[ $$rc != 0 ]]; then
            |        exit $$rc
            |    fi
            |
            |    # evaluate applet outputs, and upload result files
            |    java -jar $${DX_FS_ROOT}/dxWDL.jar internal taskEpilog $${DX_FS_ROOT}/source.wdl $${HOME}
            |""".stripMargin.trim
    }

    def genBashScriptNonTask(miniCmd:String) : String = {
        s"""|main() {
            |    java -jar $${DX_FS_ROOT}/dxWDL.jar internal ${miniCmd} $${DX_FS_ROOT}/source.wdl $${HOME}
            |}""".stripMargin.trim
    }

    def genBashScriptScatterCollect() : String = {
        s"""|main() {
            |    java -jar $${DX_FS_ROOT}/dxWDL.jar internal scatterCollectSubjob $${DX_FS_ROOT}/source.wdl $${HOME}
            |}
            |
            |collect() {
            |    java -jar $${DX_FS_ROOT}/dxWDL.jar internal collect $${DX_FS_ROOT}/source.wdl $${HOME}
            |}""".stripMargin.trim
    }

    def genBashScript(appKind: IR.AppletKind,
                      instanceType: IR.InstanceType,
                      wdlCode: String,
                      linkInfo: Option[String],
                      dbInstance: Option[String]) : String = {
        val body:String = appKind match {
            case IR.AppletKindEval =>
                genBashScriptNonTask("eval")
            case (IR.AppletKindIf(_) | IR.AppletKindScatter(_)) =>
                genBashScriptNonTask("miniWorkflow")
            case (IR.AppletKindScatterCollect(_)) =>
                genBashScriptScatterCollect()
            case IR.AppletKindNative(_) =>
                throw new Exception("Sanity: generating a bash script for a native applet")
            case IR.AppletKindTask =>
                instanceType match {
                    case IR.InstanceTypeDefault | IR.InstanceTypeConst(_,_,_,_) =>
                        s"""|main() {
                            |${genBashScriptTaskBody()}
                            |}""".stripMargin
                    case IR.InstanceTypeRuntime =>
                        s"""|main() {
                            |    # evaluate the instance type, and launch a sub job on it
                            |    java -jar $${DX_FS_ROOT}/dxWDL.jar internal taskRelaunch $${DX_FS_ROOT}/source.wdl $${HOME}
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
    def chksum(s: String) : String = {
        val digest = MessageDigest.getInstance("MD5").digest(s.getBytes)
        digest.map("%02X" format _).mkString
    }

    // Add a checksum to a request
    def checksumReq(req: JsValue) : (String, JsValue) = {
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

    // Move an object into an archive directory. If the object
    // is an applet, for example /A/B/C/GLnexus, move it to
    //     /A/B/C/Applet_archive/GLnexus (Day Mon DD hh:mm:ss year)
    // If the object is a workflow, move it to
    //     /A/B/C/Workflow_archive/GLnexus (Day Mon DD hh:mm:ss year)
    //
    // Examples:
    //   GLnexus (Fri Aug 19 18:01:02 2016)
    //   GLnexus (Mon Mar  7 15:18:14 2016)
    //
    // Note: 'dx build' does not support workflow archiving at the moment.
    def archiveDxObject(objInfo:DxObjectInfo,
                        dxObjDir: DxObjectDirectory) : Unit = {
        trace(verbose.on, s"Archiving ${objInfo.name} ${objInfo.dxObj.getId}")
        val dxClass:String = objInfo.dxClass
        val destFolder = folder ++ "/." ++ dxClass + "_archive"

        // move the object to the new location
        dxObjDir.newFolder(destFolder)
        dxProject.move(List(objInfo.dxObj).asJava, List.empty[String].asJava, destFolder)

        // add the date to the object name
        val formatter = DateTimeFormatter.ofPattern("EE MMM dd kk:mm:ss yyyy")
        val crDateStr = objInfo.crDate.format(formatter)
        val req = JsObject(
            "project" -> JsString(dxProject.getId),
            "name" -> JsString(s"${objInfo.name} ${crDateStr}")
        )

        dxClass match {
            case "Workflow" =>
                DXAPI.workflowRename(objInfo.dxObj.getId,
                                     jsonNodeOfJsValue(req),
                                     classOf[JsonNode])
            case "Applet" =>
                DXAPI.appletRename(objInfo.dxObj.getId,
                                   jsonNodeOfJsValue(req),
                                   classOf[JsonNode])
            case other => throw new Exception(s"Unkown class ${other}")
        }
    }

    // Do we need to build this applet/workflow?
    //
    // Returns:
    //   None: build is required: None
    //   Some(dxobject) : the right object is already on the platform
    def isBuildRequired(name: String,
                        digest: String,
                        dxObjDir: DxObjectDirectory) : Option[DXDataObject] = {
        val existingDxObjs = dxObjDir.lookup(name)
        val buildRequired:Boolean = existingDxObjs.size match {
            case 0 => true
            case 1 =>
                // Check if applet code has changed
                val dxObjInfo = existingDxObjs.head
                val dxClass:String = dxObjInfo.dxClass
                if (digest != dxObjInfo.digest) {
                    trace(verbose.on, s"${dxClass} has changed, rebuild required")
                    true
                } else {
                    trace(verbose.on, s"${dxClass} has not changed")
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
                    existingDxObjs.foreach(x => archiveDxObject(x, dxObjDir))
                } else if (force) {
                    // the dx:object exists, and needs to be removed. There
                    // may be several versions, all are removed.
                    val objs = existingDxObjs.map(_.dxObj)
                    trace(verbose.on, s"Removing old ${name} ${objs.map(_.getId)}")
                    dxProject.removeObjects(objs.asJava)
                } else {
                    val dxClass = existingDxObjs.head.dxClass
                    throw new Exception(s"""|${dxClass} ${name} already exists in
                                            | ${projName}:${folder}""".stripMargin)
                }
            }
            None
        } else {
            assert(existingDxObjs.size == 1)
            Some(existingDxObjs.head.dxObj)
        }
    }

    // Bundle all the applet code into one bash script.
    //
    // For applets that call other applets, we pass a directory
    // of the callees, so they could be found a runtime. This is
    // equivalent to linking, in a standard C compiler.
    def genAppletScript(applet: IR.Applet,
                        aplLinks: Map[String, (IR.Applet, DXApplet)]) : String = {
        // generate the wdl source file
        val wdlCode:String = WdlPrettyPrinter(false, None).apply(applet.ns, 0)
            .mkString("\n")

        // create linking information
        val linkInfo:Option[String] =
            if (aplLinks.isEmpty) {
                None
            } else {
                val linkInfo = JsObject(
                    aplLinks.map{ case (key, (irApplet, dxApplet)) =>
                        // Reduce the information to what will be needed for runtime linking.
                        val appInputDefs: Map[String, WomType] = irApplet.inputs.map{
                            case CVar(name, wdlType, _, _, _) => (name -> wdlType)
                        }.toMap
                        val ali = AppletLinkInfo(appInputDefs, dxApplet)
                        key -> AppletLinkInfo.writeJson(ali)
                    }.toMap)
                Some(linkInfo.prettyPrint)
            }

        // Add the pricing model, if this will be needed. Make the prices
        // opaque.
        val dbInstance =
            if (applet.instanceType == IR.InstanceTypeRuntime) {
                val dbOpaque = InstanceTypeDB.opaquePrices(instanceTypeDB)
                Some(dbOpaque.toJson.prettyPrint)
            } else {
                None
            }

        // write the bash script
        genBashScript(applet.kind, applet.instanceType,
                      wdlCode, linkInfo, dbInstance)
    }

    // Set the run spec.
    //
    def calcRunSpec(bashScript: String,
                    iType: IR.InstanceType,
                    docker: IR.DockerImage) : JsValue = {
        // find the dxWDL asset
        val instanceType:String = iType match {
            case x : IR.InstanceTypeConst =>
                instanceTypeDB.apply(x)
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
            "release" -> JsString("14.04"),
        )

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
                Some(JsObject("name" -> JsString(pkgName),
                             "id" -> jsValueOfJsonNode(pkgFile.getLinkAsJson)))
        }
        val bundledDepends = dockerAssets match {
            case None => Vector(runtimeLibrary)
            case Some(img) => Vector(runtimeLibrary, img)
        }
        JsObject(runSpec +
                     ("bundledDepends" -> JsArray(bundledDepends)))
    }

    // Build an '/applet/new' request
    def appletNewReq(applet: IR.Applet,
                     bashScript: String,
                     folder : String) : JsValue = {
        trace(verbose.on, s"Building /applet/new request for ${applet.name}")

        val inputSpec : Vector[JsValue] = applet.inputs.map(cVar =>
            cVarToSpec(cVar)
        ).flatten.toVector
        val outputSpec : Vector[JsValue] = applet.outputs.map(cVar =>
            cVarToSpec(cVar)
        ).flatten.toVector
        val runSpec : JsValue = calcRunSpec(bashScript, applet.instanceType, applet.docker)

        // Even scatters need network access, because
        // they spawn subjobs that (may) use dx-docker.
        // We end up allowing all applets to use the network
        val network:Map[String, JsValue] = Map("network" -> JsArray(JsString("*")))

        // The WorkflowOutput applet requires higher permissions
        // to organize the output directory.
        val projAccess:Map[String, JsValue] = applet.kind match {
            case IR.AppletKindWorkflowOutputReorg => Map("project" -> JsString("CONTRIBUTE"))
            case _ => Map()
        }
        val access = JsObject(network ++ projAccess)

        // pack all the arguments into a single request
        JsObject(
            "project" -> JsString(dxProject.getId),
            "name" -> JsString(applet.name),
            "folder" -> JsString(folder),
            "parents" -> JsBoolean(true),
            "inputSpec" -> JsArray(inputSpec),
            "outputSpec" -> JsArray(outputSpec),
            "runSpec" -> runSpec,
            "dxapi" -> JsString("1.0.0"),
            "access" -> access,
            "tags" -> JsArray(JsString("dxWDL"))
        )
    }

    def apiParseReplyID(rep: JsonNode) : String = {
        val repJs:JsValue = jsValueOfJsonNode(rep)
        repJs.asJsObject.fields.get("id") match {
            case None => throw new Exception("API call did not returnd an ID")
            case Some(JsString(x)) => x
            case other => throw new Exception(s"API call returned invalid ID ${other}")
        }
    }

    // Rebuild the applet if needed.
    //
    // When [force] is true, always rebuild. Otherwise, rebuild only
    // if the WDL code has changed.
    def buildAppletIfNeeded(applet: IR.Applet,
                            appletDict: Map[String, (IR.Applet, DXApplet)],
                            dxObjDir: DxObjectDirectory) : DXApplet = {
        trace(verbose.on, s"Compiling applet ${applet.name}")

        // limit the applet dictionary, only to actual dependencies
        val calls:Map[String, String] = applet.kind match {
            case IR.AppletKindIf(calls) => calls
            case IR.AppletKindScatter(calls) => calls
            case IR.AppletKindScatterCollect(calls) => calls
            case _ => Map.empty
        }
        val aplLinks = calls.map{ case (_,tName) => tName -> appletDict(tName) }.toMap

        // Build an applet script
        val bashScript = genAppletScript(applet, aplLinks)

        // Calculate a checksum of the inputs that went into the
        // making of the applet.
        val req = appletNewReq(applet, bashScript, folder)
        val (digest,appletApiRequest) = checksumReq(req)
        if (verbose.on) {
            val fName = s"${applet.name}_req.json"
            val trgPath = Utils.appCompileDirPath.resolve(fName)
            Utils.writeFileContent(trgPath, req.prettyPrint)
        }

        val buildRequired = isBuildRequired(applet.name, digest, dxObjDir)
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
    def genStageInputs(inputs: Vector[(CVar, SArg)],
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
    def buildWorkflowInputSpec(cVar:CVar,
                               sArg:SArg,
                               stageDict: Map[String, DXWorkflowStage]): Vector[JsValue] = {
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
    def buildWorkflowOutputSpec(cVar:CVar,
                                sArg:SArg,
                                stageDict: Map[String, DXWorkflowStage]): Vector[JsValue] = {
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
    def buildWorkflow(ns: IR.Namespace,
                      wf: IR.Workflow,
                      digest: String,
                      appletDict: Map[String, (IR.Applet, DXApplet)]) : DXWorkflow = {
        val (stagesReq, stageDict) =
            wf.stages.foldLeft((Vector.empty[JsValue], Map.empty[String, DXWorkflowStage])) {
                case ((stagesReq, stageDict), stg) =>
                    val (irApplet,dxApplet) = appletDict(stg.appletName)
                    val linkedInputs : Vector[(CVar, SArg)] = irApplet.inputs zip stg.inputs
                    val inputs = genStageInputs(linkedInputs, stageDict)
                    // convert the per-stage metadata into JSON
                    val stageReqDesc = JsObject(
                        "id" -> JsString(stg.id.getId),
                        "executable" -> JsString(dxApplet.getId),
                        "name" -> JsString(stg.name),
                        "input" -> inputs)
                    (stagesReq :+ stageReqDesc,
                     stageDict ++ Map(stg.name -> stg.id))
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
        DXWorkflow.getInstance(id)
    }

    // Compile an entire workflow
    //
    // - Calculate the workflow checksum from the intermediate representation
    // - Do not rebuild the workflow if it has a correct checksum
    def buildWorkflowIfNeeded(ns: IR.Namespace,
                              wf: IR.Workflow,
                              appletDict: Map[String, (IR.Applet, DXApplet)],
                              dxObjDir: DxObjectDirectory) : DXWorkflow = {
        // the workflow digest depends on the IR and the applets
        val digest:String = chksum(
            List(IR.yaml(wf).prettyPrint,
                 appletDict.toString).mkString("\n")
        )
        val buildRequired = isBuildRequired(wf.name, digest, dxObjDir)
        buildRequired match {
            case None =>
                val dxWorkflow = buildWorkflow(ns, wf, digest, appletDict)
                dxObjDir.insert(wf.name, dxWorkflow, digest)
                dxWorkflow
            case Some(dxObj) =>
                // Old workflow exists, and it has not changed.
                dxObj.asInstanceOf[DXWorkflow]
        }
    }

    // Sort the applets according to dependencies. The lowest
    // ones are tasks, because they depend on nothing else. Last come
    // generated applets like scatters and if-blocks.
    def sortAppletsByDependencies(appletDict: Map[String, IR.Applet]) : Vector[IR.Applet] = {
        def immediateDeps(apl: IR.Applet) :Vector[IR.Applet] = {
            val calls:Map[String, String] = apl.kind match {
                case IR.AppletKindIf(calls) => calls
                case IR.AppletKindScatter(calls) => calls
                case IR.AppletKindScatterCollect(calls) => calls
                case _ => Map.empty
            }
            calls.map{ case (_, taskName) =>
                appletDict.get(taskName) match {
                    case None => throw new Exception(
                        s"Applet ${apl.name} depends on an unknown applet ${taskName}")
                    case Some(x) => x
                }
            }.toVector
        }
        def transitiveDeps(apl: IR.Applet) :Vector[IR.Applet] = {
            val nextLevel:Vector[IR.Applet] = immediateDeps(apl)
            if (nextLevel.isEmpty) {
                Vector(apl)
            } else {
                val lowerLevels:Vector[IR.Applet] =
                    nextLevel
                        .map(apl => transitiveDeps(apl))
                        .flatten
                lowerLevels ++ nextLevel ++ Vector(apl)
            }
        }

        val (_,sortedApplets:Vector[IR.Applet]) =
            appletDict.foldLeft(Set.empty[String], Vector.empty[IR.Applet]) {
                case ((sortedNames, sortedApplets), (_,apl)) =>
                    if (sortedNames contains apl.name) {
                        (sortedNames, sortedApplets)
                    } else {
                        val next:Vector[IR.Applet] = transitiveDeps(apl)
                            // prune applets we have already seen
                            .filter(x => !(sortedNames contains x.name))
                        val nextNames:Set[String] = next.map(_.name).toSet
                        (sortedNames ++ nextNames, sortedApplets ++ next)
                    }
            }
        sortedApplets
    }

    def apply(ns: IR.Namespace) : (Option[DXWorkflow], Vector[DXApplet]) = {
        trace(verbose.on, "Backend pass")

        // Efficiently build a directory of the currently existing applets.
        // We don't want to build them if we don't have to.
        val dxObjDir = DxObjectDirectory(ns, dxProject, folder, verbose)

        // Sort the applets according to dependencies.
        val applets = sortAppletsByDependencies(ns.applets)
        val appletNames = applets.map(_.name)
        trace(verbose.on, s"compilation order=${appletNames}")

        // Build the individual applets. We need to keep track of
        // the applets created, to be able to link calls. For example,
        // a scatter calls other applets; we need to pass the applet IDs
        // to the launcher at runtime.
        val appletDict = applets.foldLeft(Map.empty[String, (IR.Applet, DXApplet)]) {
            case (appletDict, apl) =>
                val dxApplet = apl.kind match {
                    case IR.AppletKindNative(id) => DXApplet.getInstance(id)
                    case _ => buildAppletIfNeeded(apl, appletDict, dxObjDir)
                }
                trace(verbose.on, s"Applet ${apl.name} = ${dxApplet.getId()}")
                appletDict + (apl.name -> (apl, dxApplet))
        }.toMap

        val dxApplets = appletDict.map{ case (_, (_,dxApl)) => dxApl }.toVector
        ns.workflow match {
            case None =>
                (None, dxApplets)
            case Some(wf) =>
                val dxwfl = buildWorkflowIfNeeded(ns, wf, appletDict, dxObjDir)
                (Some(dxwfl), dxApplets)
        }
    }
}
