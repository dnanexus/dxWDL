/** Generate a dx:worflow and dx:applets from an intermediate representation.
  */
package dxWDL

// DX bindings
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.dnanexus.{DXApplet, DXAPI, DXDataObject,
    DXJSON, DXProject, DXRecord, DXSearch, DXWorkflow}
import java.security.MessageDigest
import scala.collection.JavaConverters._
import spray.json._
import Utils.{AppletLinkInfo, base64Encode, CHECKSUM_PROP, dxFileOfJsValue, DXWorkflowStage,
    INSTANCE_TYPE_DB_FILENAME, jsValueOfJsonNode, jsonNodeOfJsValue, LINK_INFO_FILENAME, trace}
import wdl4s.parser.WdlParser.Ast
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
    lazy val runtimeLibrary:JsValue = getRuntimeLibrary()

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
    def wdlVarToSpec(varName: String,
                     wdlType : WdlType,
                     ast: Ast) : Vector[JsValue] = {
        val name = Utils.encodeAppletVarName(varName)
        def mkPrimitive(dxType: String) : Vector[Map[String, JsValue]] = {
            Vector(Map("name" -> JsString(name),
                       "class" -> JsString(dxType)))
        }
        def mkPrimitiveArray(dxType: String) : Vector[Map[String, JsValue]] = {
            Vector(Map("name" -> JsString(name),
                       "class" -> JsString("array:" ++ dxType),
                       "optional" -> JsBoolean(true)))
        }
        def mkComplex() : Vector[Map[String,JsValue]] = {
            // A large JSON structure passed as a hash, and a
            // vector of platform files.
            //
            // Note: the help field for the file vector is empty,
            // so that the WdlVarLinks.loadJobInputsAsLinks method
            // will not interpret it.
            Vector(Map("name" -> JsString(name),
                       "class" -> JsString("hash")),
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

        val vec: Vector[Map[String,JsValue]] = nonOptional(Utils.stripOptional(wdlType))
        wdlType match {
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

    // Build an applet by calling /applet/new
    def appletGenApiCall(applet: IR.Applet,
                         bashScript: String,
                         folder : String) : JsValue = {
        trace(verbose.on, s"Building /applet/new request for ${applet.name}")

        // We need to implement the archiving option
        assert(!archive)

        val inputSpec : Vector[JsValue] = applet.inputs.map(cVar =>
            wdlVarToSpec(cVar.dxVarName, cVar.wdlType, cVar.ast)
        ).flatten.toVector
        val outputSpec : Vector[JsValue] = applet.outputs.map(cVar =>
            wdlVarToSpec(cVar.dxVarName, cVar.wdlType, cVar.ast)
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
        val access = JsObject("access" -> JsObject(network ++ projAccess))

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
                            aplDir: AppletDirectory)
            : (DXApplet, Vector[IR.CVar]) = {
        trace(verbose.on, s"Compiling applet ${applet.name}")
        val existingApl = aplDir.lookup(applet.name)

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
        val req = appletGenApiCall(applet, bashScript, folder)
        val (digest,appletApiRequest) = checksumReq(req)
        if (verbose.on) {
            val fName = s"${applet.name}_req_${digest}.json"
            val trgPath = Utils.appCompileDirPath.resolve(fName)
            Utils.writeFileContent(trgPath, req.prettyPrint)
        }

        val buildRequired = existingApl.size match {
            case 0 => true
            case 1 =>
                // Check if applet code has changed
                val dxAplInfo = existingApl.head
                if (digest != dxAplInfo.digest) {
                    trace(verbose.on, s"Applet has changed, rebuild required")
                    true
                } else {
                    trace(verbose.on, "Applet has not changed")
                    false
                }
            case _ =>
                System.err.println(s"""|More than one applet ${applet.name} found in
                                       |path ${dxProject.getId()}:${folder}""".stripMargin)
                true
        }

        if (buildRequired) {
            if (existingApl.size > 0) {
                // applet exists, and needs to be removed
                if (!force) {
                    val projName = dxProject.describe().getName()
                    throw new Exception(s"""|Applet ${applet.name} already exists in
                                            | ${projName}:${folder}""".stripMargin)
                }
                trace(verbose.on, "[Force] Removing old applets")
                val dxObjects:Vector[DXApplet] = existingApl.map(_.applet).toVector
                dxProject.removeObjects(dxObjects.asJava)
            }
            // Compile a WDL snippet into an applet.
            val rep = DXAPI.appletNew(jsonNodeOfJsValue(appletApiRequest), classOf[JsonNode])
            val id = apiParseReplyID(rep)
            val dxApplet = DXApplet.getInstance(id)
            aplDir.insert(applet.name, dxApplet, digest)
            (dxApplet, applet.outputs)
        } else {
            // Old applet exists, and it has not changed. Return the
            // applet-id.
            assert(existingApl.size > 0)
            (existingApl.head.applet, applet.outputs)
        }
    }

    // Link source values to targets. This is the same as
    // WdlVarLinks.genFields, but overcomes certain cases where the
    // source and target WDL types do not match. For example, if the
    // source is a File, and the target is an Array[File], we can
    // modify the JSON to get around this.
    def genFieldsCastIfRequired(wvl: WdlVarLinks,
                                rawSrcType: WdlType,
                                bindName: String) : List[(String, JsonNode)] = {
//        System.err.println(s"genFieldsCastIfRequired(${bindName})  trgType=${wvl.wdlType.toWdlString} srcType=${srcType.toWdlString}")
        val srcType = Utils.stripOptional(rawSrcType)
        val trgType = Utils.stripOptional(wvl.wdlType)

        if (trgType == srcType) {
            WdlVarLinks.genFields(wvl, bindName)
        } else if (trgType == WdlArrayType(srcType)) {
            // Cast from T to Array[T]
            WdlVarLinks.genFields(wvl, bindName).map{ case(key, jsonNode) =>
                val jsonArr = DXJSON.getArrayBuilder().add(jsonNode).build()
                (key, jsonArr)
            }.toList
        } else {
            throw new Exception(s"""|Linking error: source type=${rawSrcType.toWdlString}
                                    |target type=${wvl.wdlType.toWdlString}, bindName=${bindName}"""
                                    .stripMargin.replaceAll("\n", " "))
        }
    }

    // Calculate the stage inputs from the call closure
    //
    // It comprises mappings from variable name to WdlType.
    def genStageInputs(inputs: Vector[(IR.CVar, IR.SArg)],
                       irApplet: IR.Applet,
                       stageDict: Map[String, DXWorkflowStage]) : JsonNode = {
        val dxBuilder = inputs.foldLeft(DXJSON.getObjectBuilder()) {
            case (dxBuilder, (cVar, sArg)) =>
                sArg match {
                    case IR.SArgEmpty =>
                        // We do not have a value for this input at compile time.
                        // For compulsory applet inputs, the user will have to fill
                        // in a value at runtime.
                        dxBuilder
                    case IR.SArgConst(wValue) =>
                        val wvl = WdlVarLinks.apply(cVar.wdlType, cVar.attrs, wValue)
                        val fields = genFieldsCastIfRequired(wvl,
                                                             wValue.wdlType,
                                                             cVar.dxVarName)
                        fields.foldLeft(dxBuilder) { case (b, (fieldName, jsonNode)) =>
                            b.put(fieldName, jsonNode)
                        }
                    case IR.SArgLink(stageName, argName) =>
                        val dxStage = stageDict(stageName)
                        val wvl = WdlVarLinks(cVar.wdlType,
                                              cVar.attrs,
                                              DxlStage(dxStage, IORef.Output, argName.dxVarName))
                        val fields = genFieldsCastIfRequired(wvl,
                                                             argName.wdlType,
                                                             cVar.dxVarName)
                        fields.foldLeft(dxBuilder) { case (b, (fieldName, jsonNode)) =>
                            b.put(fieldName, jsonNode)
                        }
                }
        }
        dxBuilder.build()
    }

    // Create the workflow in a single API call.
    //
    // Prepare the list of stages, and the checksum in
    // advance. Previously we needed an API call for each stage.
    //
    // TODO: make use of capability to specify workflow level input/outputs
    def buildWorkflow(wf: IR.Workflow,
                      wfDigest: String,
                      appletDict: Map[String, (IR.Applet, DXApplet)]) : DXWorkflow = {
        val stageDictInit = Map.empty[String, DXWorkflowStage]
        val stagesReqEmpty = DXJSON.getArrayBuilder()
        val (_,stagesReq,_) = wf.stages.foldLeft((0, stagesReqEmpty, stageDictInit)) {
            case ((version, stagesReq, stageDict), stg) =>
                val (irApplet,dxApplet) = appletDict(stg.appletName)
                val linkedInputs : Vector[(IR.CVar, IR.SArg)] = irApplet.inputs zip stg.inputs
                val inputs: JsonNode = genStageInputs(linkedInputs, irApplet, stageDict)
                val stgId = DXWorkflowStage(s"stage_${version}")
                // convert the per-stage metadata into JSON
                val stageReqDesc = DXJSON.getObjectBuilder()
                    .put("id", stgId.getId)
                    .put("executable", dxApplet.getId)
                    .put("name", stg.name)
                    .put("input", inputs)
                    .build()

                (version + 1,
                 stagesReq.add(stageReqDesc),
                 stageDict ++ Map(stg.name -> stgId))
        }

        // pack all the arguments into a single API call
        val req: ObjectNode = DXJSON.getObjectBuilder()
            .put("project", dxProject.getId)
            .put("name", wf.name)
            .put("folder", folder)
            .put("properties", DXJSON.getObjectBuilder()
                     .put(CHECKSUM_PROP, wfDigest)
                     .build())
            .put("stages", stagesReq.build())
            .build()
        val rep = DXAPI.workflowNew(req, classOf[JsonNode])
        val id = apiParseReplyID(rep)
        DXWorkflow.getInstance(id)
    }

    // Compile an entire workflow
    //
    // - Calculate the workflow checksum from the intermediate representation
    // - Do not rebuild the workflow if it has a correct checksum
    def buildWorkflowIfNeeded(ns: IR.Namespace,
                              wf: IR.Workflow,
                              appletDict: Map[String, (IR.Applet, DXApplet)]) : DXWorkflow = {
        // the workflow digest depends on the IR and the applets
        val wfDigest:String = chksum(
            List(IR.yaml(wf).prettyPrint,
                 appletDict.toString).mkString("\n")
        )

        // Search for existing workflows on the platform, in the same path
        val existingWfl = DXSearch.findDataObjects().nameMatchesExactly(wf.name)
            .inFolder(dxProject, folder).withClassWorkflow().execute().asList()
            .asScala.toList

        val buildRequired = existingWfl.size match {
            case 0 => true
            case 1 =>
                // Check if workflow code has changed
                val dxWfl = existingWfl.head
                val desc: DXWorkflow.Describe = dxWfl.describe(
                    DXDataObject.DescribeOptions.get().withProperties())
                val props: Map[String, String] = desc.getProperties().asScala.toMap
                val oldWfDigest:String = props.get(CHECKSUM_PROP) match {
                    case None =>
                        System.err.println(
                            s"""|Workflow ${wf.name} has no checksum, and is invalid. It was probably
                                |not created with dxWDL. Please remove it with:
                                |dx rm ${dxProject}:${folder}/${wf.name}
                                |""".stripMargin.trim)
                        throw new Exception("Encountered invalid workflow, not created with dxWDL")
                    case Some(x) => x
                }
                if (wfDigest != oldWfDigest) {
                    trace(verbose.on, "Workflow has changed, rebuild required")
                    true
                } else {
                    trace(verbose.on, "Workflow has not changed")
                    false
                }
            case _ => true
        }

        if (buildRequired) {
            if (existingWfl.size > 0) {
                // workflow exists, and needs to be removed
                if (!force) {
                    val projName = dxProject.describe().getName()
                    throw new Exception(s"""|Workflow ${wf.name} already exists in
                                            | ${projName}:${folder}""".stripMargin)
                }
                trace(verbose.on, "[Force] Removing old workflow")
                dxProject.removeObjects(existingWfl.asJava)
            }
            buildWorkflow(wf, wfDigest, appletDict)
        } else {
            // Old workflow exists, and it has not changed.
            assert(existingWfl.size > 0)
            existingWfl.head
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
        val aplDir = AppletDirectory(ns, dxProject, folder, verbose)

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
                val (dxApplet, _) = buildAppletIfNeeded(apl, appletDict, aplDir)
                trace(verbose.on, s"Applet ${apl.name} = ${dxApplet.getId()}")
                appletDict + (apl.name -> (apl, dxApplet))
        }.toMap

        val dxApplets = appletDict.map{ case (_, (_,dxApl)) => dxApl }.toVector
        ns.workflow match {
            case None =>
                (None, dxApplets)
            case Some(wf) =>
                val dxwfl = buildWorkflowIfNeeded(ns, wf, appletDict)
                (Some(dxwfl), dxApplets)
        }
    }
}
