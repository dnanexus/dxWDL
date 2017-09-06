/** Generate a dx:worflow and dx:applets from an intermediate representation.
  */
package dxWDL

// DX bindings
import com.fasterxml.jackson.databind.JsonNode
import com.dnanexus.{DXApplet, DXDataObject, DXJSON, DXProject, DXSearch, DXWorkflow}
import java.nio.file.{Files, Paths, Path}
import java.security.MessageDigest
import scala.collection.JavaConverters._
import spray.json._
import Utils.{AppletLinkInfo, CHECKSUM_PROP, WDL_SNIPPET_FILENAME}
import wdl4s.parser.WdlParser.Ast
import wdl4s.wdl.types._

// Keep all the information about an applet in packaged form
case class AppletInfo(name:String,
                      applet:DXApplet,
                      props: Map[String, String])

// Take a snapshot of the platform target path before the build starts.
// Make an efficient directory of all the applets that exist there.
case class AppletDirectory(dxProject:DXProject,
                           folder: String,
                           verbose: Utils.Verbose) {
    lazy val appletDir : Map[String, Vector[AppletInfo]] = bulkAppletLookup()

    // Instead of looking applets one by one, perform a bulk lookup, and
    // find all the applets in the target directory. Setup an easy to
    // use map with information on each applet name.
    private def bulkAppletLookup() : Map[String, Vector[AppletInfo]] = {
        val dxApplets: List[DXApplet] = DXSearch.findDataObjects()
            .inFolder(dxProject, folder)
            .withClassApplet()
            .includeDescribeOutput(DXDataObject.DescribeOptions.get().withProperties())
            .execute().asList().asScala.toList

        // discard applet that have no checksum property
        val aplInfoList: List[AppletInfo] = dxApplets.map{ dxApl =>
            val desc = dxApl.getCachedDescribe()
            val name = desc.getName()
            val props: Map[String, String] = desc.getProperties().asScala.toMap
            AppletInfo(name, dxApl, props)
        }

        // There could be multiple versions of the same applet, collect their
        // information in vectors
        aplInfoList.foldLeft(Map.empty[String, Vector[AppletInfo]]) {
            case (accu, aplInfo) =>
                val name = aplInfo.name
                accu.get(name) match {
                    case None =>
                        // first time we have seen this applet
                        accu + (name -> Vector(aplInfo))
                    case Some(vec) =>
                        // there is already at least one applet by this name
                        accu + (name -> (vec :+ aplInfo))
                }
        }
    }

    def lookup(aplName: String) : Vector[AppletInfo] = {
        appletDir.get(aplName) match {
            case None => Vector.empty
            case Some(v) => v
        }
    }
}

case class CompilerNative(dxWDLrtId: String,
                          dxProject: DXProject,
                          instanceTypeDB: InstanceTypeDB,
                          folder: String,
                          cef: CompilerErrorFormatter,
                          timeoutPolicy: Option[Int],
                          force: Boolean,
                          archive: Boolean,
                          verbose: Utils.Verbose) {
    val DX_COMPILE_TIMEOUT = 30
    val MAX_NUM_RETRIES = 5
    val MIN_SLEEP_SEC = 5
    val MAX_SLEEP_SEC = 30

    val aplDir = AppletDirectory(dxProject, folder, verbose)

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
                       "help" -> JsString(wdlType.toWdlString),
                       "class" -> JsString(dxType)))
        }
        def mkPrimitiveArray(dxType: String) : Vector[Map[String, JsValue]] = {
            Vector(Map("name" -> JsString(name),
                       "help" -> JsString(wdlType.toWdlString),
                       "class" -> JsString("array:" ++ dxType),
                       "optional" -> JsBoolean(true)))
        }
        def mkComplex() : Vector[Map[String,JsValue]] = {
            // A large JSON structure passed as a file, and a
            // vector of platform files.
            //
            // Note: the help field for the file vector is empty,
            // so that the WdlVarLinks.loadJobInputsAsLinks method
            // will not interpret it.
            Vector(Map("name" -> JsString(name),
                       "help" -> JsString(wdlType.toWdlString),
                       "class" -> JsString("file")),
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

    def genBashScriptTaskBody(): String = {
        s"""|    echo "working directory =$${PWD}"
            |    echo "home dir =$${HOME}"
            |    echo "user= $${USER}"
            |
            |    # Keep track of streaming files. Each such file
            |    # is converted into a fifo, and a 'dx cat' process
            |    # runs in the background.
            |    background_pids=()
            |
            |    # evaluate input arguments, and download input files
            |    java -jar $${DX_FS_ROOT}/dxWDL.jar internal taskProlog $${DX_FS_ROOT}/${WDL_SNIPPET_FILENAME} $${HOME}
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
            |    java -jar $${DX_FS_ROOT}/dxWDL.jar internal taskEpilog $${DX_FS_ROOT}/${WDL_SNIPPET_FILENAME} $${HOME}
            |""".stripMargin.trim
    }

    def genBashScript(appKind: IR.AppletKind, instanceType: IR.InstanceType) : String = {
        appKind match {
            case IR.AppletKindEval =>
                s"""|#!/bin/bash -ex
                    |main() {
                    |    echo "working directory =$${PWD}"
                    |    echo "home dir =$${HOME}"
                    |    echo "user= $${USER}"
                    |    java -jar $${DX_FS_ROOT}/dxWDL.jar internal eval $${DX_FS_ROOT}/${WDL_SNIPPET_FILENAME} $${HOME}
                    |}""".stripMargin.trim

            case (IR.AppletKindIf(_) | IR.AppletKindScatter(_)) =>
                s"""|#!/bin/bash -ex
                    |main() {
                    |    echo "working directory =$${PWD}"
                    |    echo "home dir =$${HOME}"
                    |    echo "user= $${USER}"
                    |    java -jar $${DX_FS_ROOT}/dxWDL.jar internal miniWorkflow $${DX_FS_ROOT}/${WDL_SNIPPET_FILENAME} $${HOME}
                    |}""".stripMargin.trim

            case IR.AppletKindTask =>
                instanceType match {
                    case IR.InstanceTypeDefault | IR.InstanceTypeConst(_) =>
                        s"""|#!/bin/bash -ex
                            |main() {
                            |${genBashScriptTaskBody()}
                            |}""".stripMargin
                    case IR.InstanceTypeRuntime =>
                        s"""|#!/bin/bash -ex
                            |main() {
                            |    # evaluate the instance type, and launch a sub job on it
                            |    java -jar $${DX_FS_ROOT}/dxWDL.jar internal taskRelaunch $${DX_FS_ROOT}/${WDL_SNIPPET_FILENAME} $${HOME}
                            |}
                            |
                            |# We are on the correct instance type, run the task
                            |body() {
                            |${genBashScriptTaskBody()}
                            |}""".stripMargin.trim
                }

            case IR.AppletKindWorkflowOutputs =>
                s"""|#!/bin/bash -ex
                    |main() {
                    |    echo "working directory =$${PWD}"
                    |    echo "home dir =$${HOME}"
                    |    echo "user= $${USER}"
                    |    java -jar $${DX_FS_ROOT}/dxWDL.jar internal workflowOutputs $${DX_FS_ROOT}/${WDL_SNIPPET_FILENAME} $${HOME}
                    |}""".stripMargin.trim
            case IR.AppletKindWorkflowOutputsAndReorg =>
                s"""|#!/bin/bash -ex
                    |main() {
                    |    echo "working directory =$${PWD}"
                    |    echo "home dir =$${HOME}"
                    |    echo "user= $${USER}"
                    |    java -jar $${DX_FS_ROOT}/dxWDL.jar internal workflowOutputsAndReorg $${DX_FS_ROOT}/${WDL_SNIPPET_FILENAME} $${HOME}
                    |}""".stripMargin.trim
        }
    }

    // create a directory structure for this applet
    // applets/
    //        task-name/
    //                  dxapp.json
    //                  resources/
    //                      workflowFile.wdl
    //
    // For applets that call other applets, we pass a directory
    // of the callees, so they could be found a runtime. This is
    // equivalent to linking, in a standard C compiler.
    //
    // Calculate a checksum of the inputs that went into the making of the applet.
    // This helps
    def createAppletDirStruct(applet: IR.Applet,
                              aplLinks: Map[String, (IR.Applet, DXApplet)],
                              appJson : JsObject) : Path = {
        // create temporary directory
        val appletDir : Path = Utils.appCompileDirPath.resolve(applet.name)
        Utils.safeMkdir(appletDir)

        // Clean up the task subdirectory, if it it exists
        Utils.deleteRecursive(appletDir.toFile())
        Files.createDirectory(appletDir)

        val srcDir = Files.createDirectory(Paths.get(appletDir.toString(), "src"))
        val resourcesDir = Files.createDirectory(Paths.get(appletDir.toString(), "resources"))

        // write the bash script
        val bashScript = genBashScript(applet.kind, applet.instanceType)
        Utils.writeFileContent(srcDir.resolve("code.sh"), bashScript)

        // Copy the WDL file.
        //
        val wdlCode:String = WdlPrettyPrinter(false, None).apply(applet.ns, 0)
            .mkString("\n")
        Utils.writeFileContent(resourcesDir.resolve(Utils.WDL_SNIPPET_FILENAME),
                               wdlCode)

        // write linking information
        if (!aplLinks.isEmpty) {
            val linkInfo = JsObject(
                aplLinks.map{ case (key, (irApplet, dxApplet)) =>
                    // Reduce the information to what will be needed for runtime linking.
                    val appInputDefs: Map[String, WdlType] = irApplet.inputs.map{
                        case IR.CVar(name, wdlType, _, _) => (name -> wdlType)
                    }.toMap
                    val ali = AppletLinkInfo(appInputDefs, dxApplet)
                    key -> AppletLinkInfo.writeJson(ali)
                }.toMap
            )
            Utils.writeFileContent(resourcesDir.resolve(Utils.LINK_INFO_FILENAME),
                                   linkInfo.prettyPrint)
        }

        // Add the pricing model, if this will be needed
        if (applet.instanceType == IR.InstanceTypeRuntime) {
            Utils.writeFileContent(resourcesDir.resolve(Utils.INSTANCE_TYPE_DB_FILENAME),
                                   instanceTypeDB.toJson.prettyPrint)
        }

        // write the dxapp.json
        Utils.writeFileContent(appletDir.resolve("dxapp.json"), appJson.prettyPrint)
        appletDir
    }

    // Set the run spec.
    //
    def calcRunSpec(iType: IR.InstanceType) : JsValue = {
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
            "interpreter" -> JsString("bash"),
            "file" -> JsString("src/code.sh"),
            "distribution" -> JsString("Ubuntu"),
            "release" -> JsString("14.04"),
            "assetDepends" -> JsArray(
                JsObject("id" -> JsString(dxWDLrtId))
            ),
            "systemRequirements" ->
                JsObject("main" ->
                             JsObject("instanceType" -> JsString(instanceType)))
        )
        JsObject(runSpec ++ timeoutSpec)
    }

    // Sending a string to the command line shell may require quoting it.
    // Quoting is needed if the string contains white spaces.
    private def quoteIfNeeded(buf: String) : String = {
        if (!buf.matches("\\S+")) s"""'${buf}'"""
        else buf
    }

    // Perform a "dx build" on a local directory representing an applet
    //
    def dxBuildApp(appletDir : Path,
                   appletName : String,
                   folder : String) : DXApplet = {
        Utils.trace(verbose.on, s"Building applet ${appletName}")
        val path =
            if (folder.endsWith("/")) (folder ++ appletName)
            else (folder ++ "/" ++ appletName)
        val pId = dxProject.getId()
        val dest =
            if (path.startsWith("/")) pId ++ ":" ++ path
            else pId ++ ":/" ++ path
        var buildCmd = List("dx",
                            "build",
                            quoteIfNeeded(appletDir.toString()),
                            "--destination",
                            quoteIfNeeded(dest))
        if (force)
            buildCmd = buildCmd :+ "-f"
        if (archive)
            buildCmd = buildCmd :+ "-a"
        val commandStr = buildCmd.mkString(" ")

        def build() : Option[DXApplet] = {
            try {
                // Run the dx-build command
                Utils.trace(verbose.on, commandStr)
                val (outstr, _) = Utils.execCommand(commandStr, Some(DX_COMPILE_TIMEOUT))

                // extract the appID from the output
                val app : JsObject = outstr.parseJson.asJsObject
                app.fields("id") match {
                    case JsString(appId) => Some(DXApplet.getInstance(appId, dxProject))
                    case _ => None
                }
            } catch {
                case e : Throwable =>
                    // Triage the error, according to the exception message.
                    // Is this a retriable error?
                    val msg = e.getMessage
                    if ((msg contains "The folder could not be found") ||
                            (msg contains "No value found")) {
                        throw new Exception(s"build command ${commandStr} failed")
                    }
                    None
            }
        }

        // Retry the build operation with exponential backoff
        var retrySleepSec = MIN_SLEEP_SEC
        for (i <- 1 to MAX_NUM_RETRIES) {
            build() match {
                case None => ()
                case Some(dxApp) => return dxApp
            }
            System.err.println(s"Build attempt ${i} failed")
            if (i < MAX_NUM_RETRIES) {
                System.err.println(s"Sleeping for ${retrySleepSec} seconds")
                Thread.sleep(retrySleepSec * 1000)
                retrySleepSec = Math.min(MAX_SLEEP_SEC, retrySleepSec*2)
            }
        }
        throw new Exception(s"Failed to build applet ${appletName}")
    }


    // Calculate the MD5 checksum of a string
    def chksum(s: String) : String = {
        val digest = MessageDigest.getInstance("MD5").digest(s.getBytes)
        digest.map("%02X" format _).mkString
    }

    // Concatenate all the files in the directory into a string, and
    // checksum the string. We assume the files are small enough to
    // fit in memory, and that the directory structure does not
    // matter, only file content. This is sufficient for our purposes.
    def checksumDirectory(dir: Path) : String = {
        val content = Files.walk(dir).iterator().asScala
            .filter(Files.isRegularFile(_))
            .map(path => Utils.readFileContent(path))
        chksum(content.mkString("\n"))
    }


    // Write the WDL code to a file, and generate a bash applet to run
    // it on the platform.
    def localBuildApplet(applet: IR.Applet,
                         appletDict: Map[String, (IR.Applet, DXApplet)]) : Path = {
        Utils.trace(verbose.on, s"Compiling applet ${applet.name}")
        val inputSpec : Seq[JsValue] = applet.inputs.map(cVar =>
            wdlVarToSpec(cVar.dxVarName, cVar.wdlType, cVar.ast)
        ).flatten
        val outputDecls : Seq[JsValue] = applet.outputs.map(cVar =>
            wdlVarToSpec(cVar.dxVarName, cVar.wdlType, cVar.ast)
        ).flatten
        val runSpec : JsValue = calcRunSpec(applet.instanceType)
        val attrs = Map(
            "name" -> JsString(applet.name),
            "inputSpec" -> JsArray(inputSpec.toVector),
            "outputSpec" -> JsArray(outputDecls.toVector),
            "runSpec" -> runSpec
        )

        // Even scatters need network access, because
        // they spawn subjobs that (may) use dx-docker.
        // We end up allowing all applets to use the network
        val network:Map[String, JsValue] = Map("network" -> JsArray(JsString("*")))

        // The WorkflowOutput applet requires higher permissions
        // to organize the output directory.
        val proj:Map[String, JsValue] = applet.kind match {
            case IR.AppletKindWorkflowOutputsAndReorg => Map("project" -> JsString("CONTRIBUTE"))
            case _ => Map()
        }
        val access = Map("access" -> JsObject(network ++ proj))
        val json = JsObject(attrs ++ access)

        val aplLinks = applet.kind match {
            case IR.AppletKindIf(_) => appletDict
            case IR.AppletKindScatter(_) => appletDict
            case _ => Map.empty[String, (IR.Applet, DXApplet)]
        }
        // create a directory structure for this applet
        createAppletDirStruct(applet, aplLinks, json)
    }

    // Rebuild the applet if needed.
    //
    // When [force] is true, always rebuild. Otherwise, rebuild only
    // if the WDL code has changed.
    def buildAppletIfNeeded(applet: IR.Applet,
                            appletDict: Map[String, (IR.Applet, DXApplet)])
            : (DXApplet, Vector[IR.CVar]) = {
        val existingApl = aplDir.lookup(applet.name)

        // Build an applet structure locally
        val appletDir = localBuildApplet(applet, appletDict)
        val digest = checksumDirectory(appletDir)

        val buildRequired =
            if (existingApl.size == 0) {
                true
            } else if (existingApl.size == 1) {
                // Check if applet code has changed
                val dxAplInfo = existingApl.head
                val props = dxAplInfo.props
                props.get(CHECKSUM_PROP) match {
                    case None =>
                        System.err.println(
                            s"""|No checksum found for applet ${applet.name}
                                |${dxAplInfo.applet.getId()}, rebuilding""".stripMargin)
                        true
                    case Some(dxAplChksum) =>
                        if (digest != dxAplChksum) {
                            Utils.trace(verbose.on, "Applet has changed, rebuild required")
                            true
                        } else {
                            Utils.trace(verbose.on, "Applet has not changed")
                            false
                        }
                }
            } else {
                throw new Exception(s"""|More than one applet ${applet.name} found in
                                        |path ${dxProject.getId()}:${folder}""".stripMargin)
            }

        if (buildRequired) {
            // Compile a WDL snippet into an applet.
            val dxApplet = dxBuildApp(appletDir, applet.name, folder)

            // Add a checksum for the WDL code as a property of the applet.
            // This allows to quickly check if anything has changed, saving
            // unnecessary builds.
            dxApplet.putProperty(CHECKSUM_PROP, digest)
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
                       stageDict: Map[String, DXWorkflow.Stage]) : JsonNode = {
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

    def buildWorkflow(wf: IR.Workflow,
                      wfDigest: String,
                      appletDict: Map[String, (IR.Applet, DXApplet)]) : DXWorkflow = {
        // create fresh workflow
        val dxwfl = DXWorkflow.newWorkflow()
            .setProject(dxProject)
            .setFolder(folder)
            .setName(wf.name).build()

        // Create a dx:workflow stage for each stage in the IR.
        // The accumulator state holds:
        // - the workflow version, which gets incremented per stage
        // - a dictionary of stages, mapping name to stage. This is used
        //   to locate variable references.
        //
        val stageDictInit = Map.empty[String, DXWorkflow.Stage]
        val _ = wf.stages.foldLeft((0, stageDictInit)) {
            case ((version, stageDict), stg) =>
                val (irApplet,dxApplet) = appletDict(stg.appletName)
                val linkedInputs : Vector[(IR.CVar, IR.SArg)] = irApplet.inputs zip stg.inputs
                val inputs: JsonNode = genStageInputs(linkedInputs, irApplet, stageDict)
                val modif: DXWorkflow.Modification[DXWorkflow.Stage] =
                    dxwfl.addStage(dxApplet, stg.name, inputs, version)
                val nextVersion = modif.getEditVersion()
                val dxStage : DXWorkflow.Stage = modif.getValue()
                Utils.trace(verbose.on, s"Stage ${stg.name} = ${dxStage.getId()}")
                (nextVersion,
                 stageDict ++ Map(stg.name -> dxStage))
        }
        dxwfl.close()

        // Add a checksum for the WDL code as a property of the applet.
        // This allows to quickly check if anything has changed, saving
        // unnecessary builds.
        dxwfl.putProperty(CHECKSUM_PROP, wfDigest)
        dxwfl
    }

    // Compile an entire workflow
    //
    // - Calculate the workflow checksum from the intermediate representation
    // - Do not rebuild the workflow if it has a correct checksum
    def buildWorkflowIfNeeded(wf: IR.Workflow) : DXWorkflow = {
        // Build the individual applets. We need to keep track of
        // the applets created, to be able to link calls. For example,
        // a scatter calls other applets; we need to pass the applet IDs
        // to the launcher at runtime.
        val initAppletDict = Map.empty[String, (IR.Applet, DXApplet)]
        val appletDict = wf.applets.foldLeft(initAppletDict) {
            case (appletDict, a) =>
                val (dxApplet, _) = buildAppletIfNeeded(a, appletDict)
                Utils.trace(verbose.on, s"Applet ${a.name} = ${dxApplet.getId()}")
                appletDict + (a.name -> (a, dxApplet))
        }.toMap

        // the workflow digest depends on the IR and the applets
        val wfDigest:String = chksum(
            List(IR.yaml(wf).prettyPrint,
                 appletDict.toString).mkString("\n")
        )

        // Search for existing workflows on the platform, in the same path
        val existingWfl = DXSearch.findDataObjects().nameMatchesExactly(wf.name)
            .inFolder(dxProject, folder).withClassWorkflow().execute().asList()
            .asScala.toList

        val buildRequired =
            if (existingWfl.size == 0) {
                true
            } else if (existingWfl.size == 1) {
                // Check if workflow code has changed
                val dxWfl = existingWfl.head
                val desc: DXWorkflow.Describe = dxWfl.describe(
                    DXDataObject.DescribeOptions.get().withProperties())
                val props: Map[String, String] = desc.getProperties().asScala.toMap
                props.get(CHECKSUM_PROP) match {
                    case None =>
                        System.err.println(s"""|No checksum found for workflow ${wf.name}
                                               |${dxWfl.getId()}, rebuilding""".stripMargin)
                        true
                    case Some(dxWflChksum) =>
                        if (wfDigest != dxWflChksum) {
                            Utils.trace(verbose.on, "Workflow has changed, rebuild required")
                            true
                        } else {
                            Utils.trace(verbose.on, "Workflow has not changed")
                            false
                        }
                }
            } else {
                throw new Exception(s"""|More than one workflow ${wf.name} found in
                                        |path ${dxProject.getId()}:${folder}""".stripMargin)
            }

        if (buildRequired) {
            if (existingWfl.size > 0) {
                // workflow exists, and needs to be removed
                if (!force) {
                    val projName = dxProject.describe().getName()
                    throw new Exception(s"""|Workflow ${wf.name} already exists in
                                            | ${projName}:${folder}""".stripMargin)
                }
                Utils.trace(verbose.on, "[Force] Removing old workflow")
                dxProject.removeObjects(existingWfl.asJava)
            }
            buildWorkflow(wf, wfDigest, appletDict)
        } else {
            // Old workflow exists, and it has not changed.
            assert(existingWfl.size > 0)
            existingWfl.head
        }
    }

    def apply(ns: IR.Namespace) : (Option[DXWorkflow], Vector[DXApplet]) = {
        Utils.trace(verbose.on, "Backend pass")
        ns.workflow match {
            case None =>
                val dxApplets = ns.applets.map{ applet =>
                    val (dxApplet, _) = buildAppletIfNeeded(applet, Map.empty)
                    dxApplet
                }
                (None, dxApplets.toVector)

            case Some(iRepWf) =>
                val dxwfl = buildWorkflowIfNeeded(iRepWf)
                (Some(dxwfl), Vector.empty)
        }
    }
}
