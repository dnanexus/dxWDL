/** Generate a dx:worflow and dx:applets from an intermediate representation.
  */
package dxWDL

// DX bindings
import com.fasterxml.jackson.databind.JsonNode
import com.dnanexus.{DXApplet, DXAPI, DXDataObject, DXProject, DXRecord, DXWorkflow}
import java.security.MessageDigest
import java.time.format.DateTimeFormatter
import scala.collection.JavaConverters._
import spray.json._
import Utils.{AppletLinkInfo, base64Encode, CHECKSUM_PROP, dxFileOfJsValue, DXWorkflowStage,
    INSTANCE_TYPE_DB_FILENAME, jsValueOfJsonNode, jsonNodeOfJsValue, LINK_INFO_FILENAME, trace}
import wdl4s.wdl.types._

case class CompilerNative(dxWDLrtId: String,
                          dxProject: DXProject,
                          instanceTypeDB: InstanceTypeDB,
                          folder: String,
                          cef: CompilerErrorFormatter,
                          timeoutPolicy: Option[Int],
                          force: Boolean,
                          archive: Boolean,
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
        val dxFile = dxFileOfJsValue(dxLink)
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
    def cVarToSpec(cVar: IR.CVar) : Vector[JsValue] = {
        val name = Utils.encodeAppletVarName(cVar.dxVarName)
        val defaultVal:Map[String, JsValue] = cVar.attrs.getDefault match {
            case None => Map.empty
            case Some(v) => Map("default" -> v)
        }
        def mkPrimitive(dxType: String) : Vector[Map[String, JsValue]] = {
            Vector(Map("name" -> JsString(name),
                       "class" -> JsString(dxType))
                       ++ defaultVal)
        }
        def mkPrimitiveArray(dxType: String) : Vector[Map[String, JsValue]] = {
            Vector(Map("name" -> JsString(name),
                       "class" -> JsString("array:" ++ dxType),
                       "optional" -> JsBoolean(true))
                       ++ defaultVal)
        }
        def mkComplex() : Vector[Map[String,JsValue]] = {
            // A large JSON structure passed as a hash, and a
            // vector of platform files.
            //
            // Note: the help field for the file vector is empty,
            // so that the WdlVarLinks.loadJobInputsAsLinks method
            // will not interpret it.
            Vector(Map("name" -> JsString(name),
                       "class" -> JsString("hash"))
                       ++ defaultVal,
                   Map("name" -> JsString(name + Utils.FLAT_FILES_SUFFIX),
                       "class" -> JsString("array:file"),
                       "optional" -> JsBoolean(true)))
        }
        def nonOptional(t : WdlType) : Vector[Map[String, JsValue]] = {
            t match {
                // primitive types
                case WdlBooleanType => mkPrimitive("boolean")
                case WdlIntegerType => mkPrimitive("int")
                case WdlFloatType => mkPrimitive("float")
                case WdlStringType =>mkPrimitive("string")
                case WdlFileType => mkPrimitive("file")

                // single dimension arrays of primitive types
                case WdlArrayType(WdlBooleanType) => mkPrimitiveArray("boolean")
                case WdlArrayType(WdlIntegerType) => mkPrimitiveArray("int")
                case WdlArrayType(WdlFloatType) => mkPrimitiveArray("float")
                case WdlArrayType(WdlStringType) => mkPrimitiveArray("string")
                case WdlArrayType(WdlFileType) => mkPrimitiveArray("file")

                // complex types, that may contains files
                case _ => mkComplex()
            }
        }

        val vec: Vector[Map[String,JsValue]] = nonOptional(Utils.stripOptional(cVar.wdlType))
        cVar.wdlType match {
            case WdlOptionalType(t) =>
                // An optional variable, make it an optional dx input/output
                vec.map{ m => JsObject(m + ("optional" -> JsBoolean(true))) }
            case _ =>
                vec.map{ m => JsObject(m)}
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

    def genBashScriptNonTask(miniCmd:String,
                             setupFilesScript:String) : String = {
        s"""|#!/bin/bash -ex
            |${setupFilesScript}
            |
            |main() {
            |    java -jar $${DX_FS_ROOT}/dxWDL.jar internal ${miniCmd} $${DX_FS_ROOT}/source.wdl $${HOME}
            |}""".stripMargin.trim
    }

    def genBashScript(appKind: IR.AppletKind,
                      instanceType: IR.InstanceType,
                      wdlCode: String,
                      linkInfo: Option[String],
                      dbInstance: Option[String]) : String = {
        val setupFilesScript = genSourceFiles(wdlCode, linkInfo, dbInstance)
        appKind match {
            case IR.AppletKindEval =>
                genBashScriptNonTask("eval", setupFilesScript)
            case (IR.AppletKindIf(_) | IR.AppletKindScatter(_)) =>
                genBashScriptNonTask("miniWorkflow", setupFilesScript)
            case IR.AppletKindTask =>
                instanceType match {
                    case IR.InstanceTypeDefault | IR.InstanceTypeConst(_) =>
                        s"""|#!/bin/bash -ex
                            |${setupFilesScript}
                            |
                            |main() {
                            |${genBashScriptTaskBody()}
                            |}""".stripMargin
                    case IR.InstanceTypeRuntime =>
                        s"""|#!/bin/bash -ex
                            |${setupFilesScript}
                            |
                            |main() {
                            |    # evaluate the instance type, and launch a sub job on it
                            |    java -jar $${DX_FS_ROOT}/dxWDL.jar internal taskRelaunch $${DX_FS_ROOT}/source.wdl $${HOME}
                            |}
                            |
                            |# We are on the correct instance type, run the task
                            |body() {
                            |${genBashScriptTaskBody()}
                            |}""".stripMargin.trim
                }
            case IR.AppletKindWorkflowOutputs =>
                genBashScriptNonTask("workflowOutputs", setupFilesScript)
            case IR.AppletKindWorkflowOutputsAndReorg =>
                genBashScriptNonTask("workflowOutputsAndReorg", setupFilesScript)
        }
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
                System.err.println(s"""|More than one ${dxClass} ${name} found in
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
                        val appInputDefs: Map[String, WdlType] = irApplet.inputs.map{
                            case IR.CVar(name, wdlType, _, _) => (name -> wdlType)
                        }.toMap
                        val ali = AppletLinkInfo(appInputDefs, dxApplet)
                        key -> AppletLinkInfo.writeJson(ali)
                    }.toMap)
                Some(linkInfo.prettyPrint)
            }

        // Add the pricing model, if this will be needed
        val dbInstance =
            if (applet.instanceType == IR.InstanceTypeRuntime) {
                Some(instanceTypeDB.toJson.prettyPrint)
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
                    iType: IR.InstanceType) : JsValue = {
        // find the dxWDL asset
        val instanceType:String = iType match {
            case IR.InstanceTypeConst(x) => x
            case IR.InstanceTypeDefault | IR.InstanceTypeRuntime =>
                instanceTypeDB.getMinimalInstanceType
        }
        val timeoutSpec: Map[String, JsValue] =
            timeoutPolicy match {
                case None => Map()
                case Some(hours) =>
                    Map("timeoutPolicy" -> JsObject("*" ->
                                                        JsObject("hours" -> JsNumber(hours))
                        ))
            }

        val runSpec: Map[String, JsValue] = Map(
            "code" -> JsString(bashScript),
            "interpreter" -> JsString("bash"),
            "systemRequirements" ->
                JsObject("main" ->
                             JsObject("instanceType" -> JsString(instanceType))),
            "distribution" -> JsString("Ubuntu"),
            "release" -> JsString("14.04"),
            "bundledDepends" -> JsArray(runtimeLibrary)
        )
        JsObject(runSpec ++ timeoutSpec)
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
        val runSpec : JsValue = calcRunSpec(bashScript, applet.instanceType)

        // Even scatters need network access, because
        // they spawn subjobs that (may) use dx-docker.
        // We end up allowing all applets to use the network
        val network:Map[String, JsValue] = Map("network" -> JsArray(JsString("*")))

        // The WorkflowOutput applet requires higher permissions
        // to organize the output directory.
        val projAccess:Map[String, JsValue] = applet.kind match {
            case IR.AppletKindWorkflowOutputsAndReorg => Map("project" -> JsString("CONTRIBUTE"))
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
            "access" -> access
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
                            dxObjDir: DxObjectDirectory)
            : (DXApplet, Vector[IR.CVar]) = {
        trace(verbose.on, s"Compiling applet ${applet.name}")

        // limit the applet dictionary, only to actual dependencies
        val aplLinks = applet.kind match {
            case IR.AppletKindIf(calls) =>
                calls.map{ case (_,tName) => tName -> appletDict(tName) }.toMap
            case IR.AppletKindScatter(calls) =>
                calls.map{ case (_,tName) => tName -> appletDict(tName) }.toMap
            case _ =>
                Map.empty[String, (IR.Applet, DXApplet)]
        }

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
                (dxApplet, applet.outputs)
            case Some(dxObj) =>
                // Old applet exists, and it has not changed. Return the
                // applet-id.
                (dxObj.asInstanceOf[DXApplet], applet.outputs)
        }
    }

    // Link source values to targets. This is the same as
    // WdlVarLinks.genFields, but overcomes certain cases where the
    // source and target WDL types do not match. For example, if the
    // source is a File, and the target is an Array[File], we can
    // modify the JSON to get around this.
    def genFieldsCastIfRequired(wvl: WdlVarLinks,
                                rawSrcType: WdlType,
                                bindName: String) : List[(String, JsValue)] = {
//        System.err.println(s"genFieldsCastIfRequired(${bindName})  trgType=${wvl.wdlType.toWdlString} srcType=${srcType.toWdlString}")
        val srcType = Utils.stripOptional(rawSrcType)
        val trgType = Utils.stripOptional(wvl.wdlType)

        val l:List[(String, JsValue)] =
            if (trgType == srcType) {
                WdlVarLinks.genFields(wvl, bindName).map { case(key, jsv) =>
                    (key, jsv)
                }
            } else if (trgType == WdlArrayType(srcType)) {
                // Cast from T to Array[T]
                WdlVarLinks.genFields(wvl, bindName).map{ case(key, jsv) =>
                    (key, JsArray(jsv))
                }
            } else {
                throw new Exception(s"""|Linking error: source type=${rawSrcType.toWdlString}
                                        |target type=${wvl.wdlType.toWdlString}, bindName=${bindName}"""
                                        .stripMargin.replaceAll("\n", " "))
            }
        l.map{ case (key, json) => key -> json }
    }

    // Calculate the stage inputs from the call closure
    //
    // It comprises mappings from variable name to WdlType.
    def genStageInputs(inputs: Vector[(IR.CVar, IR.SArg)],
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
                        val wvl = WdlVarLinks.apply(cVar.wdlType, cVar.attrs, wValue)
                        val fields = genFieldsCastIfRequired(wvl,
                                                             wValue.wdlType,
                                                             cVar.dxVarName)
                        m ++ fields.toMap
                    case IR.SArgLink(stageName, argName) =>
                        val dxStage = stageDict(stageName)
                        val wvl = WdlVarLinks(cVar.wdlType,
                                              cVar.attrs,
                                              DxlStage(dxStage, IORef.Output, argName.dxVarName))
                        val fields = genFieldsCastIfRequired(wvl,
                                                             argName.wdlType,
                                                             cVar.dxVarName)
                        m ++ fields.toMap
                }
        }
        JsObject(jsInputs)
    }

    def dxClassOfWdlType(wdlType: WdlType) : String = {
        val t = Utils.stripOptional(wdlType)
        t match {
            // primitive types
            case WdlBooleanType => "boolean"
            case WdlIntegerType => "int"
            case WdlFloatType => "float"
            case WdlStringType =>"string"
            case WdlFileType => "file"

            // single dimension arrays of primitive types
            case WdlArrayType(WdlBooleanType) => "array:boolean"
            case WdlArrayType(WdlIntegerType) => "array:int"
            case WdlArrayType(WdlFloatType) => "array:float"
            case WdlArrayType(WdlStringType) => "array:string"
            case WdlArrayType(WdlFileType) => "array:file"

            // complex types, that may contains files
            case _ => "hash"
        }
    }

    // Create the workflow in a single API call.
    //
    // Prepare the list of stages, and the checksum in
    // advance. Previously we needed an API call for each stage.
    //
    // TODO: make use of capability to specify workflow level input/outputs
    def buildWorkflow(ns: IR.Namespace,
                      wf: IR.Workflow,
                      digest: String,
                      appletDict: Map[String, (IR.Applet, DXApplet)]) : DXWorkflow = {
        // Figure out the workflow inputs by looking at the first stage/applet
        assert(!wf.stages.isEmpty)
        val wfInputSpec:Vector[JsValue] = wf.inputs.map(cVar =>
            JsObject("name" -> JsString(cVar.name),
                     "class" -> JsString(dxClassOfWdlType(cVar.wdlType))
            ))
        trace(verbose2, s"workflow input spec=${wfInputSpec}")

        // Link the first stage inputs to the workflow inputs
        val stage0 = wf.stages.head
        val (_, dxApplet0) = appletDict(stage0.appletName)
        val stage0inputs:Map[String, JsObject] = wf.inputs.map{ cVar =>
            cVar.name -> JsObject("$dnanexus_link" -> JsObject(
                                               "workflowInputField" -> JsString(cVar.name)))
        }.toMap
        val stage0ReqDesc = JsObject(
            "id" -> JsString(stage0.id.getId),
            "executable" -> JsString(dxApplet0.getId),
            "name" -> JsString(stage0.name),
            "input" -> JsObject(stage0inputs))

        // Link all the other stages
        val (stagesReq,_) = wf.stages.tail.foldLeft(
            (Vector(stage0ReqDesc), Map(stage0.name -> stage0.id))) {
            case ((stagesReq, stageDict), stg) =>
                val (irApplet,dxApplet) = appletDict(stg.appletName)
                val linkedInputs : Vector[(IR.CVar, IR.SArg)] = irApplet.inputs zip stg.inputs
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

        // Figure out the workflow outputs
        val stageLast = wf.stages.last
        val wfOutputSpec:Vector[JsValue] = wf.outputs.map{ cVar =>
            JsObject("name" -> JsString(cVar.dxVarName),
                     "class" -> JsString(dxClassOfWdlType(cVar.wdlType)),
                     "outputSource" -> JsObject("$dnanexus_link" -> JsObject(
                                                    "stage" -> JsString(stageLast.id.getId) ,
                                                    "outputField" -> JsString(cVar.dxVarName))))
        }
        trace(verbose2, s"workflow output spec=${wfOutputSpec}")

        // pack all the arguments into a single API call
        val req = JsObject("project" -> JsString(dxProject.getId),
                           "name" -> JsString(wf.name),
                           "folder" -> JsString(folder),
                           "properties" -> JsObject(CHECKSUM_PROP -> JsString(digest)),
                           "stages" -> JsArray(stagesReq),
                           "workflowInputSpec" -> JsArray(wfInputSpec),
                           "workflowOutputSpec" -> JsArray(wfOutputSpec)
        )

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
            val calls:Seq[String] = apl.kind match {
                case IR.AppletKindIf(calls) => calls.map{ case (_,taskName) => taskName }.toSeq
                case IR.AppletKindScatter(calls) => calls.map{ case (_,taskName) => taskName }.toSeq
                case _ => Vector.empty
            }
            calls.map{ name =>
                appletDict.get(name) match {
                    case None => throw new Exception(
                        s"Applet ${apl.name} depends on an unknown applet ${name}")
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
                val (dxApplet, _) = buildAppletIfNeeded(apl, appletDict, dxObjDir)
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
