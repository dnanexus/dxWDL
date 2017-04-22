/** Generate a dx:worflow and dx:applets from an intermediate representation.
  */
package dxWDL

// DX bindings
import com.fasterxml.jackson.databind.JsonNode
import com.dnanexus.{DXWorkflow, DXApplet, DXProject, DXJSON, DXUtil}
import java.nio.file.{Files, Paths, Path}
import scala.util.{Failure, Success, Try}
import wdl4s.expression.{NoFunctions, WdlStandardLibraryFunctionsType}
import wdl4s.parser.WdlParser.{Ast, AstNode, Terminal}
import wdl4s.types._
import wdl4s.values._
import wdl4s.WdlExpression.AstForExpressions
import WdlVarLinks._

// Json support
import spray.json._
import DefaultJsonProtocol._

object CompilerBackend {
    val WDL_SNIPPET_FILENAME = Utils.WDL_SNIPPET_FILENAME

    // Compiler state.
    // Packs common arguments passed between methods.
    case class State(dxWDLrtId: String,
                     dxProject: DXProject,
                     folder: String,
                     cef: CompilerErrorFormatter,
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
        def mkRaggedFileArray() : Vector[Map[String, JsValue]] = {
            Vector(Map("name" -> JsString(name),
                     "help" -> JsString(wdlType.toWdlString),
                     "class" -> JsString("file")),
                 Map("name" -> JsString( name ++ Utils.FLAT_FILE_ARRAY_SUFFIX),
                     "class" -> JsString("array:file")))
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

    def genBashScript(appKind: IR.AppletKind.Value, docker: Option[String]) : String = {
        appKind match {
            case IR.AppletKind.Eval =>
                s"""|#!/bin/bash -ex
                    |main() {
                    |    echo "working directory =$${PWD}"
                    |    echo "home dir =$${HOME}"
                    |    echo "user= $${USER}"
                    |    java -cp $${DX_FS_ROOT}/dxWDL.jar:$${CLASSPATH} dxWDL.Main eval $${DX_FS_ROOT}/${WDL_SNIPPET_FILENAME} $${HOME}
                    |}""".stripMargin.trim

            case IR.AppletKind.Scatter =>
                s"""|#!/bin/bash -ex
                    |main() {
                    |    echo "working directory =$${PWD}"
                    |    echo "home dir =$${HOME}"
                    |    echo "user= $${USER}"
                    |    java -cp $${DX_FS_ROOT}/dxWDL.jar:$${CLASSPATH} dxWDL.Main launchScatter $${DX_FS_ROOT}/${WDL_SNIPPET_FILENAME} $${HOME}
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

                s"""|#!/bin/bash
                    |main() {
                    |    set -ex
                    |    echo "working directory =$${PWD}"
                    |    echo "home dir =$${HOME}"
                    |    echo "user= $${USER}"
                    |
                    |    # evaluate input arguments, and download input files
                    |    java -cp $${DX_FS_ROOT}/dxWDL.jar:$${CLASSPATH} dxWDL.Main taskProlog $${DX_FS_ROOT}/${Utils.WDL_SNIPPET_FILENAME} $${HOME}
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
                    |    java -cp $${DX_FS_ROOT}/dxWDL.jar:$${CLASSPATH} dxWDL.Main taskEpilog $${DX_FS_ROOT}/${Utils.WDL_SNIPPET_FILENAME} $${HOME}
                    |}""".stripMargin.trim
        }
    }

    // create a directory structure for this applet
    // applets/
    //        task-name/
    //                  dxapp.json
    //                  resources/
    //                      workflowFile.wdl
    def createAppletDirStruct(applet: IR.Applet, appJson : JsObject) : Path = {
        // create temporary directory
        val appletDir : Path = Utils.appCompileDirPath.resolve(applet.name)
        Utils.safeMkdir(appletDir)

        // Clean up the task subdirectory, if it it exists
        Utils.deleteRecursive(appletDir.toFile())
        Files.createDirectory(appletDir)

        val srcDir = Files.createDirectory(Paths.get(appletDir.toString(), "src"))
        val resourcesDir = Files.createDirectory(Paths.get(appletDir.toString(), "resources"))

        // write the bash script
        val bashScript = genBashScript(applet.kind, applet.docker)
        Utils.writeFileContent(srcDir.resolve("code.sh"), bashScript)

        // Copy the WDL file.
        //
        // The runner figures out which applet to run, by querying the
        // platform.
        Utils.writeFileContent(resourcesDir.resolve(Utils.WDL_SNIPPET_FILENAME), applet.wdlCode)

        // write the dxapp.json
        Utils.writeFileContent(appletDir.resolve("dxapp.json"), appJson.prettyPrint)
        appletDir
    }

    // Set the run spec.
    //
    def calcRunSpec(iType : String, dxWDLrtId: String) : JsValue = {
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
        val instanceType: Map[String, JsValue] =
            Map("systemRequirements" ->
                    JsObject("main" ->
                                 JsObject("instanceType" -> JsString(iType))))
        JsObject(runSpec ++ instanceType)
    }

    // Perform a "dx build" on a local directory representing an applet
    //
    def dxBuildApp(appletDir : Path,
                   appletFqn : String,
                   folder : String,
                   cState: State) : DXApplet = {
        Utils.trace(cState.verbose, s"Building applet ${appletFqn}")
        val path =
            if (folder.endsWith("/")) (folder ++ appletFqn)
            else (folder ++ "/" ++ appletFqn)
        val pId = cState.dxProject.getId()
        val dest =
            if (path.startsWith("/")) pId ++ ":" ++ path
            else pId ++ ":/" ++ path
        val buildCmd = List("dx", "build", "-f", appletDir.toString(), "--destination", dest)
        def build() : Option[DXApplet] = {
            try {
                // Run the dx-build command
                val (outstr, _) = Utils.execCommand(buildCmd.mkString(" "))

                // extract the appID from the output
                val app : JsObject = outstr.parseJson.asJsObject
                app.fields("id") match {
                    case JsString(appId) => Some(DXApplet.getInstance(appId))
                    case _ => None
                }
            } catch {
                case e : Throwable => None
            }
        }

        // Try several times to do the build
        for (i <- 1 to 5) {
            build() match {
                case None => ()
                case Some(dxApp) => return dxApp
            }
            System.err.println(s"Build attempt ${i} failed")
            System.err.println(s"Sleeping for 10 seconds")
            Thread.sleep(10000)
        }
        throw new Exception(s"Failed to build applet ${appletFqn}")
    }

    // Compile a WDL snippet into an applet.
    //
    // Write the WDL code to a file, and generate a bash applet to
    // run it on the platform.
    def buildApplet(applet: IR.Applet, cState: State) : (DXApplet, Vector[IR.CVar]) = {
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

        // create a directory structure for this applet
        val appletDir = createAppletDirStruct(applet, json)
        val dxapp = dxBuildApp(appletDir, applet.name, cState.folder, cState)
        (dxapp, applet.outputs)
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
                        val wvl = WdlVarLinks.outputFieldOfWdlValue(cVar.dxVarName, wValue.wdlType, wValue)
                        val fields = WdlVarLinks.genFields(wvl, cVar.dxVarName)
                        fields.foldLeft(dxBuilder) { case (b, (fieldName, jsonNode)) =>
                            b.put(fieldName, jsonNode)
                        }
                    case IR.SArgLink(stageName, argName) =>
                        val dxStage = stageDict(stageName)
                        val wvl = WdlVarLinks(argName.dxVarName,
                                              cVar.wdlType,
                                              Some(IORef.Output, DxlStage(dxStage)))
                        val fields = WdlVarLinks.genFields(wvl, cVar.dxVarName)
                        fields.foldLeft(dxBuilder) { case (b, (fieldName, jsonNode)) =>
                            b.put(fieldName, jsonNode)
                        }
                }
        }
        dxBuilder.build()
    }

    def apply(wf: IR.Workflow,
              dxProject: DXProject,
              dxWDLrtId: String,
              folder: String,
              cef: CompilerErrorFormatter,
              verbose: Boolean) : DXWorkflow = {
        Utils.trace(verbose, "Backend pass")
        val cState = State(dxWDLrtId, dxProject, folder, cef, verbose)

        // build the individual applets
        val appletDict : Map[String, (IR.Applet, DXApplet)] =
            wf.applets.map{ a =>
                val (dxApplet, _) = buildApplet(a, cState)
                Utils.trace(cState.verbose, s"Applet ${a.name} = ${dxApplet.getId()}")
                a.name -> (a, dxApplet)
            }.toMap

        // create workflow
        val dxwfl = DXWorkflow.newWorkflow().setProject(dxProject).setFolder(folder)
            .setName(wf.name).build()

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
