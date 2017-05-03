/** Generate a dx:worflow and dx:applets from an intermediate representation.
  */
package dxWDL

// DX bindings
import com.fasterxml.jackson.databind.JsonNode
import com.dnanexus.{DXApplet, DXDataObject, DXJSON, DXProject, DXSearch, DXUtil, DXWorkflow}
import java.nio.file.{Files, Paths, Path}
import java.security.MessageDigest
import scala.collection.JavaConverters._
import spray.json._
import spray.json.DefaultJsonProtocol
import Utils.{CHECKSUM_PROP, WDL_SNIPPET_FILENAME}
import wdl4s.expression.{NoFunctions, WdlStandardLibraryFunctionsType}
import wdl4s.parser.WdlParser.Ast
import wdl4s.types._
import wdl4s.values._
import WdlVarLinks._

object CompilerBackend {
    val MAX_NUM_RETRIES = 5
    val MAX_SLEEP_SEC = 30

    // Compiler state.
    // Packs common arguments passed between methods.
    case class State(dxWDLrtId: String,
                     dxProject: DXProject,
                     folder: String,
                     cef: CompilerErrorFormatter,
                     force: Boolean,
                     verbose: Boolean)

    // For primitive types, and arrays of such types, we can map directly
    // to the equivalent dx types. For example,
    //   Int  -> int
    //   Array[String] -> array:string
    //
    // Ragged arrays, maps, and objects, cannot be mapped in such a trivial way.
    // Currently, we handle only ragged arrays. All cases aside from Array[Array[File]
    // are encoded as a json string. Ragged file arrays are passed as two fields: a
    // json string, and a flat file array.
    def wdlVarToSpec(varName: String,
                     wdlType : WdlType,
                     ast: Ast,
                     cState: State) : Vector[JsValue] = {
        val name = Utils.encodeAppletVarName(varName)
        def mkPrimitive(dxType: String) : Vector[Map[String, JsValue]] = {
            Vector(Map("name" -> JsString(name),
                     "help" -> JsString(wdlType.toWdlString),
                     "class" -> JsString(dxType)))
        }
        def mkPrimitiveArray(dxType: String) : Vector[Map[String, JsValue]] = {
            Vector(Map("name" -> JsString(name),
                     "help" -> JsString(wdlType.toWdlString),
                     "class" -> JsString("array:" ++ dxType)))
        }
        def mkRaggedArray() : Vector[Map[String,JsValue]] = {
            Vector(Map("name" -> JsString(name),
                     "help" -> JsString(wdlType.toWdlString),
                     "class" -> JsString("file")))
        }

        def nonOptional(t : WdlType) = t  match {
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

                // ragged arrays
            case WdlArrayType(WdlArrayType(WdlBooleanType)) => mkRaggedArray()
            case WdlArrayType(WdlArrayType(WdlIntegerType)) => mkRaggedArray()
            case WdlArrayType(WdlArrayType(WdlFloatType)) => mkRaggedArray()
            case WdlArrayType(WdlArrayType(WdlStringType)) => mkRaggedArray()

            case _ =>
                throw new Exception(cState.cef.notCurrentlySupported(ast, s"type ${wdlType}"))
        }

        wdlType match {
            case WdlOptionalType(t) =>
                // An optional variable, make it an optional dx input/output
                val l : Vector[Map[String,JsValue]] = nonOptional(t)
                l.map{ m => JsObject(m + ("optional" -> JsBoolean(true))) }
            case t =>
                val l : Vector[Map[String,JsValue]] = nonOptional(t)
                l.map{ m => JsObject(m)}
        }
    }

    def genBashScriptTaskBody(submitShellCommand: String): String = {
        s"""|    echo "working directory =$${PWD}"
            |    echo "home dir =$${HOME}"
            |    echo "user= $${USER}"
            |
            |    # evaluate input arguments, and download input files
            |    java -cp $${DX_FS_ROOT}/dnanexus-api-0.1.0-SNAPSHOT-jar-with-dependencies.jar:$${DX_FS_ROOT}/dxWDL.jar:$${CLASSPATH} dxWDL.Main taskProlog $${DX_FS_ROOT}/${Utils.WDL_SNIPPET_FILENAME} $${HOME}
            |    # Debugging outputs
            |    ls -lR
            |    cat $${HOME}/execution/meta/script
            |
            |    # Run the shell script generated by the prolog.
            |    # Capture the stderr/stdout in files
            |    ${submitShellCommand} $${HOME}/execution/meta/script
            |
            |    #  check return code of the script
            |    rc=`cat $${HOME}/execution/meta/rc`
            |    if [[ $$rc != 0 ]]; then
            |        exit $$rc
            |    fi
            |
            |    # evaluate applet outputs, and upload result files
            |    java -cp $${DX_FS_ROOT}/dnanexus-api-0.1.0-SNAPSHOT-jar-with-dependencies.jar:$${DX_FS_ROOT}/dxWDL.jar:$${CLASSPATH} dxWDL.Main taskEpilog $${DX_FS_ROOT}/${Utils.WDL_SNIPPET_FILENAME} $${HOME}
            |""".stripMargin.trim
    }

    def genBashScript(appKind: IR.AppletKind.Value,
                      instanceType: IR.InstanceTypeSpec,
                      docker: Option[String]) : String = {
        appKind match {
            case IR.AppletKind.Eval =>
                s"""|#!/bin/bash -ex
                    |main() {
                    |    echo "working directory =$${PWD}"
                    |    echo "home dir =$${HOME}"
                    |    echo "user= $${USER}"
                    |    java -cp $${DX_FS_ROOT}/dnanexus-api-0.1.0-SNAPSHOT-jar-with-dependencies.jar:$${DX_FS_ROOT}/dxWDL.jar:$${CLASSPATH} dxWDL.Main eval $${DX_FS_ROOT}/${WDL_SNIPPET_FILENAME} $${HOME}
                    |}""".stripMargin.trim

            case IR.AppletKind.Scatter =>
                s"""|#!/bin/bash -ex
                    |main() {
                    |    echo "working directory =$${PWD}"
                    |    echo "home dir =$${HOME}"
                    |    echo "user= $${USER}"
                    |    java -cp $${DX_FS_ROOT}/dnanexus-api-0.1.0-SNAPSHOT-jar-with-dependencies.jar:$${DX_FS_ROOT}/dxWDL.jar:$${CLASSPATH} dxWDL.Main launchScatter $${DX_FS_ROOT}/${WDL_SNIPPET_FILENAME} $${HOME}
                    |}""".stripMargin.trim

            case IR.AppletKind.Task =>
                val submitShellCommand = docker match {
                    case None =>
                        "/bin/bash"
                    case Some(imgName) =>
                        // The user wants to use a docker container with the
                        // image [imgName]. We implement this with dx-docker.
                        // There may be corner cases where the image will run
                        // into permission limitations due to security.
                        //
                        // Map the home directory into the container, so that
                        // we can reach the result files, and upload them to
                        // the platform.
                        val DX_HOME = Utils.DX_HOME
                        s"""dx-docker run -v ${DX_HOME}:${DX_HOME} ${imgName} /bin/bash"""
                }
                instanceType match {
                    case IR.InstTypeDefault | IR.InstTypeConst(_) =>
                        s"""|#!/bin/bash -ex
                            |main() {
                            |${genBashScriptTaskBody(submitShellCommand)}
                            |}""".stripMargin
                    case IR.InstTypeRuntime =>
                        s"""|#!/bin/bash -ex
                            |main() {
                            |    # evaluate the instance type, and launch a sub job on it
                            |    java -cp $${DX_FS_ROOT}/dnanexus-api-0.1.0-SNAPSHOT-jar-with-dependencies.jar:$${DX_FS_ROOT}/dxWDL.jar:$${CLASSPATH} dxWDL.Main taskRelaunch $${DX_FS_ROOT}/${Utils.WDL_SNIPPET_FILENAME} $${HOME}
                            |}
                            |
                            |# We are on the correct instance type, run the task
                            |body() {
                            |${genBashScriptTaskBody(submitShellCommand)}
                            |}""".stripMargin.trim
                }
        }
    }

    // remove old workflow
    def removeOldWorkflow(wfName: String, dxProject: DXProject, folder: String) = {
        val oldWf = DXSearch.findDataObjects().nameMatchesExactly(wfName)
            .inFolder(dxProject, folder).withClassWorkflow().execute().asList()
        dxProject.removeObjects(oldWf)
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
                              aplLinks: Map[String, DXApplet],
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
        val bashScript = genBashScript(applet.kind, applet.instanceType, applet.docker)
        Utils.writeFileContent(srcDir.resolve("code.sh"), bashScript)

        // Copy the WDL file.
        //
        Utils.writeFileContent(resourcesDir.resolve(Utils.WDL_SNIPPET_FILENAME), applet.wdlCode)

        // write linking information
        if (!aplLinks.isEmpty) {
            val linkInfo = JsObject(
                aplLinks.map{ case (key, dxApplet) =>
                    key -> JsString(dxApplet.getId())
                }.toMap
            )
            Utils.writeFileContent(resourcesDir.resolve(Utils.LINK_INFO_FILENAME),
                                   linkInfo.prettyPrint)
        }

        // write the dxapp.json
        Utils.writeFileContent(appletDir.resolve("dxapp.json"), appJson.prettyPrint)
        appletDir
    }

    // Set the run spec.
    //
    def calcRunSpec(iType: IR.InstanceTypeSpec, dxWDLrtId: String) : JsValue = {
        // find the dxWDL asset
        val runSpec: Map[String, JsValue] = Map(
            "interpreter" -> JsString("bash"),
            "file" -> JsString("src/code.sh"),
            "distribution" -> JsString("Ubuntu"),
            "release" -> JsString("14.04"),
            "assetDepends" -> JsArray(
                JsObject("id" -> JsString(dxWDLrtId))
            )
        )

        val instanceType: Map[String, JsValue] = iType match {
            case IR.InstTypeDefault => Map()
            case IR.InstTypeConst(x) =>
                Map("systemRequirements" ->
                        JsObject("main" ->
                                     JsObject("instanceType" -> JsString(x))))
            case IR.InstTypeRuntime => Map()
        }
        JsObject(runSpec ++ instanceType)
    }

    // Perform a "dx build" on a local directory representing an applet
    //
    def dxBuildApp(appletDir : Path,
                   appletName : String,
                   folder : String,
                   cState: State) : DXApplet = {
        Utils.trace(cState.verbose, s"Building applet ${appletName}")
        val path =
            if (folder.endsWith("/")) (folder ++ appletName)
            else (folder ++ "/" ++ appletName)
        val pId = cState.dxProject.getId()
        val dest =
            if (path.startsWith("/")) pId ++ ":" ++ path
            else pId ++ ":/" ++ path
        val buildCmd = List("dx", "build", "-f", appletDir.toString(), "--destination", dest)
        def build() : Option[DXApplet] = {
            try {
                // Run the dx-build command
                val commandStr = buildCmd.mkString(" ")
                Utils.trace(cState.verbose, commandStr)
                val (outstr, _) = Utils.execCommand(commandStr)

                // extract the appID from the output
                val app : JsObject = outstr.parseJson.asJsObject
                app.fields("id") match {
                    case JsString(appId) => Some(DXApplet.getInstance(appId, cState.dxProject))
                    case _ => None
                }
            } catch {
                case e : Throwable => None
            }
        }

        // Retry the build operation with exponential backoff
        var retrySleepSec = 2
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
    def checksumDirectory(path: Path, cState: State) : String = {
        val dir = path.toFile
        val content = dir.listFiles.map{ file =>
            if (file.isFile)
                Utils.readFileContent(file.toPath)
            else
                ""
        }
        chksum(content.mkString(""))
    }


    // Write the WDL code to a file, and generate a bash applet to run
    // it on the platform.
    def localBuildApplet(applet: IR.Applet,
                         appletDict: Map[String, DXApplet],
                         cState: State) : Path = {
        Utils.trace(cState.verbose, s"Compiling applet ${applet.name}")
        val inputSpec : Seq[JsValue] = applet.inputs.map(cVar =>
            wdlVarToSpec(cVar.dxVarName, cVar.wdlType, cVar.ast, cState)
        ).flatten
        val outputDecls : Seq[JsValue] = applet.outputs.map(cVar =>
            wdlVarToSpec(cVar.dxVarName, cVar.wdlType, cVar.ast, cState)
        ).flatten
        val runSpec : JsValue = calcRunSpec(applet.instanceType, cState.dxWDLrtId)
        val attrs = Map(
            "name" -> JsString(applet.name),
            "inputSpec" -> JsArray(inputSpec.toVector),
            "outputSpec" -> JsArray(outputDecls.toVector),
            "runSpec" -> runSpec
        )

        // Even scatters need network access, because
        // they spawn subjobs that use dx-docker.
        // We end up allowing all applets to use the network
        val networkAccess: Map[String, JsValue] =
            Map("access" -> JsObject(Map("network" -> JsArray(JsString("*")))))
        val json = JsObject(attrs ++ networkAccess)

        val aplLinks = applet.kind match {
            case IR.AppletKind.Scatter => appletDict
            case _ => Map.empty[String, DXApplet]
        }
        // create a directory structure for this applet
        createAppletDirStruct(applet, aplLinks, json)
    }

    // Rebuild the applet if needed.
    //
    // When [force] is true, always rebuild. Otherwise, rebuild only
    // if the WDL code has changed.
    def buildAppletIfNeeded(applet: IR.Applet,
                            appletDict: Map[String, DXApplet],
                            cState: State) : (DXApplet, Vector[IR.CVar]) = {
        // Search for existing applets on the platform, in the same path
        var existingApl: List[DXApplet] = DXSearch.findDataObjects().nameMatchesExactly(applet.name)
            .inFolder(cState.dxProject, cState.folder).withClassApplet().execute().asList()
            .asScala.toList
        if (cState.force && existingApl.size > 0) {
            // Remove old applet
            Utils.trace(cState.verbose,
                        s"[Force] Removing old applet ${applet.name} ${existingApl}")
            cState.dxProject.removeObjects(existingApl.asJava)
            existingApl = List.empty[DXApplet]
        }

        // Build an applet structure locally
        val appletDir = localBuildApplet(applet, appletDict, cState)
        val digest = checksumDirectory(appletDir, cState)

        val buildRequired =
            if (existingApl.size == 0) {
                if (!cState.force) {
                    Utils.trace(cState.verbose,
                                s"No previous version of applet ${applet.name} exists")
                }
                true
            } else if (existingApl.size == 1) {
                // Check if applet code has changed
                val dxApl = existingApl.head
                val desc: DXApplet.Describe = dxApl.describe(
                    DXDataObject.DescribeOptions.get().withProperties())
                val props: Map[String, String] = desc.getProperties().asScala.toMap
                props.get(CHECKSUM_PROP) match {
                    case None =>
                        System.err.println(s"No checksum found for applet ${applet.name} ${dxApl.getId()}, rebuilding")
                        true
                    case Some(dxAplChksum) =>
                        if (digest != dxAplChksum) {
                            Utils.trace(cState.verbose, "Applet has changed, rebuild required")
                            true
                        } else {
                            Utils.trace(cState.verbose, "Applet has not changed")
                            false
                        }
                }
            } else {
                throw new Exception(s"""|More than one applet ${applet.name} found in
                                        | path ${cState.dxProject.getId()}:${cState.folder}""")
            }

        if (buildRequired) {
            // Compile a WDL snippet into an applet.
            val dxApplet = dxBuildApp(appletDir, applet.name, cState.folder, cState)

            // Add a checksum for the WDL code as a property of the applet.
            // This allows to quickly check if anything has changed, saving
            // unnecessary builds.
            dxApplet.putProperty(CHECKSUM_PROP, digest)
            (dxApplet, applet.outputs)
        } else {
            // Old applet exists, and it has not changed. Return the
            // applet-id.
            (existingApl.head, applet.outputs)
        }
    }

    // Calculate the stage inputs from the call closure
    //
    // It comprises mappings from variable name to WdlType.
    def genStageInputs(inputs: Vector[(IR.CVar, IR.SArg)],
                       irApplet: IR.Applet,
                       stageDict: Map[String, DXWorkflow.Stage],
                       cState: State) : JsonNode = {
        val dxBuilder = inputs.foldLeft(DXJSON.getObjectBuilder()) {
            case (dxBuilder, (cVar, sArg)) =>
                sArg match {
                    case IR.SArgEmpty =>
                        // We do not have a value for this input at compile time.
                        // For compulsory applet inputs, the user will have to fill
                        // in a value at runtime.
                        dxBuilder
                    case IR.SArgConst(wValue) =>
                        val wvl = WdlVarLinks.apply(wValue.wdlType, wValue)
                        val fields = WdlVarLinks.genFields(wvl, cVar.dxVarName)
                        fields.foldLeft(dxBuilder) { case (b, (fieldName, jsonNode)) =>
                            b.put(fieldName, jsonNode)
                        }
                    case IR.SArgLink(stageName, argName) =>
                        val dxStage = stageDict(stageName)
                        val wvl = WdlVarLinks(cVar.wdlType, DxlStage(dxStage, IORef.Output, argName.dxVarName))
                        val fields = WdlVarLinks.genFields(wvl, cVar.dxVarName)
                        fields.foldLeft(dxBuilder) { case (b, (fieldName, jsonNode)) =>
                            b.put(fieldName, jsonNode)
                        }
                }
        }
        dxBuilder.build()
    }

    // Compile a single applet
    def apply(applet: IR.Applet,
              dxProject: DXProject,
              dxWDLrtId: String,
              folder: String,
              cef: CompilerErrorFormatter,
              force: Boolean,
              verbose: Boolean) : DXApplet = {
        Utils.trace(verbose, "Backend pass, single applet")
        val cState = State(dxWDLrtId, dxProject, folder, cef, force, verbose)
        val (dxApplet, _) = buildAppletIfNeeded(applet, Map.empty, cState)
        dxApplet
    }

    // Compile an entire workflow
    def apply(wf: IR.Workflow,
              dxProject: DXProject,
              dxWDLrtId: String,
              folder: String,
              cef: CompilerErrorFormatter,
              force: Boolean,
              verbose: Boolean) : DXWorkflow = {
        Utils.trace(verbose, "Backend pass")
        val cState = State(dxWDLrtId, dxProject, folder, cef, force, verbose)

        // create fresh workflow
        removeOldWorkflow(wf.name, dxProject, folder)
        val dxwfl = DXWorkflow.newWorkflow().setProject(dxProject).setFolder(folder)
            .setName(wf.name).build()

        // Build the individual applets. We need to keep track of
        // the applets created, to be able to link calls. For example,
        // a scatter calls other applets; we need to pass the applet IDs
        // to the launcher at runtime.
        val initAppletDict = Map.empty[String, (IR.Applet, DXApplet)]
        val appletDict = wf.applets.foldLeft(initAppletDict) {
            case (appletDict, a) =>
                val aplDir = appletDict.map{ case (key, (irApl, apl)) => (key, apl) }
                val (dxApplet, _) = buildAppletIfNeeded(a, aplDir, cState)
                Utils.trace(cState.verbose, s"Applet ${a.name} = ${dxApplet.getId()}")
                appletDict + (a.name -> (a, dxApplet))
        }.toMap

        // Create a dx:workflow stage for each stage in the IR.
        //
        // The accumulator state holds:
        // - the workflow version, which gets incremented per stage
        // - a dictionary of stages, mapping name to stage. This is used
        //   to locate variable references.
        val stageDictInit = Map.empty[String, DXWorkflow.Stage]
        wf.stages.foldLeft((0,stageDictInit)) {
            case ((version,stageDict), stg) =>
                val (irApplet,dxApplet) = appletDict(stg.appletName)
                val linkedInputs : Vector[(IR.CVar, IR.SArg)] = irApplet.inputs zip stg.inputs
                val inputs: JsonNode = genStageInputs(linkedInputs, irApplet, stageDict, cState)
                val modif: DXWorkflow.Modification[DXWorkflow.Stage] =
                    dxwfl.addStage(dxApplet, stg.name, inputs, version)
                val nextVersion = modif.getEditVersion()
                val dxStage : DXWorkflow.Stage = modif.getValue()
                Utils.trace(cState.verbose, s"Stage ${stg.name} = ${dxStage.getId()}")
                (nextVersion,
                 stageDict ++ Map(stg.name -> dxStage))
        }
        dxwfl.close()
        dxwfl
    }
}
