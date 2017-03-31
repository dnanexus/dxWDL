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
    // Compiler state.
    // Packs common arguments passed between methods.
    case class State(dxWDLrtId: String,
                     wdlSourceFile: Path,
                     destination: String,
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
                     cState: State) : List[JsValue] = {
        val name = Utils.encodeAppletVarName(varName)
        def mkPrimitive(dxType: String) : List[Map[String, JsValue]] = {
            List(Map("name" -> JsString(name),
                     "help" -> JsString(wdlType.toWdlString),
                     "class" -> JsString(dxType)))
        }
        def mkPrimitiveArray(dxType: String) : List[Map[String, JsValue]] = {
            List(Map("name" -> JsString(name),
                     "help" -> JsString(wdlType.toWdlString),
                     "class" -> JsString("array:" ++ dxType)))
        }
        def mkRaggedArray() : List[Map[String,JsValue]] = {
            List(Map("name" -> JsString(name),
                     "help" -> JsString(wdlType.toWdlString),
                     "class" -> JsString("file")))
        }
        def mkRaggedFileArray() : List[Map[String, JsValue]] = {
            List(Map("name" -> JsString(name),
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
                val l : List[Map[String,JsValue]] = nonOptional(t)
                l.map{ m => JsObject(m + ("optional" -> JsBoolean(true))) }
            case t =>
                val l : List[Map[String,JsValue]] = nonOptional(t)
                l.map{ m => JsObject(m)}
        }
    }

    // create a directory structure for this applet
    // applets/
    //        task-name/
    //                  dxapp.json
    //                  resources/
    //                      workflowFile.wdl
    def createAppletDirStruct(appletFqnName : String,
                              wdlSourceFile : Path,
                              appJson : JsObject,
                              bashScript: String) : Path = {
        // create temporary directory
        val appletDir : Path = Utils.appCompileDirPath.resolve(appletFqnName)
        Utils.safeMkdir(appletDir)

        // Clean up the task subdirectory, if it it exists
        Utils.deleteRecursive(appletDir.toFile())
        Files.createDirectory(appletDir)

        val srcDir = Files.createDirectory(Paths.get(appletDir.toString(), "src"))
        val resourcesDir = Files.createDirectory(Paths.get(appletDir.toString(), "resources"))

        // write the bash script
        Utils.writeFileContent(srcDir.resolve("code.sh"), bashScript)

        // Copy the WDL file.
        //
        // The runner figures out which applet to run, by querying the
        // platform.
        val wdlFileBaseName : String = wdlSourceFile.getFileName().toString()
        Files.copy(wdlSourceFile,
                   Paths.get(resourcesDir.toString(), wdlFileBaseName))

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
                   destination : String,
                   verbose: Boolean) : DXApplet = {
        Utils.trace(verbose, s"Building applet ${appletFqn}")
        val dest =
            if (destination.endsWith("/")) (destination ++ appletFqn)
            else (destination ++ "/" ++ appletFqn)
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

    // Compile a WDL call into an applet.
    //
    // We need to support calculations done in the workflow, inside a
    // call. For example:
    //
    //  call Add {
    //       input: a = 3, b = 4, c = Read.sum+8
    //  }
    //
    // There is no context to run such a calculation in the workflow, and we need
    // to push it to the applet. Therefore, we figure out the closure needed, and
    // pass that as applet inputs. The applet opens the WDL file, finds the call,
    // and executes with the provided closure.
    //
    def buildApplet(applet: IR.Applet, cState: State) : (DXApplet, List[IR.CVar]) = {
        Utils.trace(cState.verbose, s"Compiling call ${applet.name}")
        val inputSpec : Seq[JsValue] = applet.inputs.map(cVar =>
            wdlVarToSpec(cVar.name, cVar.wdlType, cVar.ast, cState)
        ).flatten
        val outputDecls : Seq[JsValue] = applet.outputs.map(cVar =>
            wdlVarToSpec(cVar.name, cVar.wdlType, cVar.ast, cState)
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
        val appletDir = createAppletDirStruct(applet.name, cState.wdlSourceFile, json,
                                              applet.code)
        val dxapp = dxBuildApp(appletDir, applet.name, cState.destination, cState.verbose)
        (dxapp, applet.outputs)
    }

    // Calculate the stage inputs from the call closure
    //
    // It comprises mappings from variable name to WdlType.
    def genStageInputs(inputs: List[(IR.CVar, IR.SArg)],
                       irApplet: IR.Applet,
                       stageDict: Map[String, DXWorkflow.Stage],
                       cState: State) : JsonNode = {
        val dxBuilder = inputs.foldLeft(DXJSON.getObjectBuilder()) {
            case (dxBuilder, (cVar, sArg)) =>
                sArg match {
                    case IR.SArgEmpty =>
                        // We do not have a value for this input at compile time.
                        // Currently, we throw an exception. But is this actually legal?
                        throw new Exception(cState.cef.missingVarRefException(irApplet.ast))
                    case IR.SArgLink(stageName, argName) =>
                        val dxStage = stageDict(stageName)
                        val wvl = WdlVarLinks(argName, cVar.wdlType,
                                              Some(IORef.Output, DxlStage(dxStage)))
                        val fields = WdlVarLinks.genFields(wvl, cVar.name)
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
              wdlSourceFile: Path,
              destination: String,
              cef: CompilerErrorFormatter,
              verbose: Boolean) : DXWorkflow = {
        Utils.trace(verbose, "Backend phase")
        val cState = State(dxWDLrtId, wdlSourceFile, destination, cef, verbose)

        // build the individual applets
        val appDict : Map[String, (IR.Applet, DXApplet)] =
            wf.applets.map{ a =>
                val (dxApplet, _) = buildApplet(a, cState)
                Utils.trace(cState.verbose, s"Applet ${a.name} = ${dxApplet.getId()}")
                a.name -> (a, dxApplet)
            }.toMap

        // create workflow
        val dxwfl = DXWorkflow.newWorkflow().setProject(dxProject).setFolder(destination)
            .setName(wf.name).build()

        // Create a dx:workflow stage for each stage in the IR.
        val stageDictInit = Map.empty[String, DXWorkflow.Stage]
        wf.stages.foldLeft((0,stageDictInit)) {
            case ((version,stageDict), stg) =>
                val (irApplet,dxApplet) = appDict(stg.appletName)
                val linkedInputs : List[(IR.CVar, IR.SArg)] = irApplet.inputs.zip(stg.inputs)
                val inputs = genStageInputs(linkedInputs, irApplet, stageDict, cState)
                val modif : DXWorkflow.Modification[DXWorkflow.Stage] =
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
