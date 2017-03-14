package dxWDL

// DX bindings
import com.fasterxml.jackson.databind.JsonNode
import com.dnanexus.{DXWorkflow, DXApplet, DXProject, DXJSON, DXUtil, DXContainer, DXSearch, DXDataObject}
import java.nio.file.{Files, Paths, Path}
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}
import wdl4s.AstTools
import wdl4s.AstTools.EnhancedAstNode
import wdl4s.{Call, Declaration, Scatter, Scope, Task, TaskOutput, WdlExpression,
    WdlNamespace, WdlNamespaceWithWorkflow, WdlSource, Workflow}
import wdl4s.expression.{NoFunctions, WdlStandardLibraryFunctionsType}
//import wdl4s.parser.WdlParser._
import wdl4s.parser.WdlParser.{Ast, AstNode, Terminal}
import wdl4s.types._
import wdl4s.values._
import wdl4s.WdlExpression.AstForExpressions
import WdlVarLinks._

// Json stuff
import spray.json._
import DefaultJsonProtocol._

/** Compile a WDL document into a DNAnexus workflow
  *
  * Notes: One of the problems is that there is no context for
  * calculating expressions in the workflow. Therefore, we push
  * expression calculation into the called applet. We do this
  * by passing the closure for a call, and generating an applet that
  * can take this closure and run the task. This results in an applet per
  * workflow stage.
  *
  * A scatter block is compiled into a workflow stage. The inputs for
  * it are the closure. The outputs are the union of all calls. The AppletRunner
  * is called with the scatter name, and it needs to
  *
  * REFERENCES:
  *    wdl4s/src/main/scala/wdl4s/expression/ValueEvaluator.scala
  */

// There are several kinds of applets
//   Common:    beginning of a workflow
//   Scatter:   utility block for scatter/gather
//   Command:   call a task, execute a shell command (usually)
object AppletKind extends Enumeration {
    val Common, Scatter, Command = Value
}

class DynamicInstanceTypesException private(ex: Exception) extends RuntimeException(ex) {
    def this() = this(new RuntimeException(
                          "Only runtime constants are supported, currently, instance types should be known at compile time."))
}

// At compile time, we need to keep track of the syntax-tree, for error
// reporting purposes.
case class CompileVar(wvl: WdlVarLinks, ast: Ast)

object Compile {
    // Environment (scope) where a call is made
    type CallEnv = Map[String, CompileVar]

    // The minimal environment needed to execute a call
    type Closure =  CallEnv

    case class VarOutput(unqualifiedName: String, wdlType: WdlType, ast: Ast)

    // Packs common arguments passed between methods.
    case class State(wdlSourceFile : Path, destination : String, dxWDLrtId: String,
                     verbose: Boolean, errFmt: CompileErrorFormatter)

    def trace(verbose: Boolean, msg: String) : Unit = {
        if (!verbose)
            return
        System.err.println(msg)
    }

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
                     cState: State,
                     mkDxOpt: Boolean=false) : List[JsValue] = {
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
                throw new Exception(cState.errFmt.notCurrentlySupported(ast, s"type ${wdlType}"))
        }

        wdlType match {
            case WdlOptionalType(t) =>
                // An optional variable, make it an optional dx input/output
                val l : List[Map[String,JsValue]] = nonOptional(t)
                l.map{ m => JsObject(m + ("optional" -> JsBoolean(true))) }
            case t =>
                val l : List[Map[String,JsValue]] = nonOptional(t)
                if (mkDxOpt) {
                    l.map{ m => JsObject(m + ("optional" -> JsBoolean(true))) }
                } else {
                    l.map{ m => JsObject(m)}
                }
        }
    }

    def callUniqueName(call : Call, cState: State) = {
        val nm = call.alias match {
            case Some(x) => x
            case None => Utils.taskOfCall(call).name
        }
        Utils.reservedAppletPrefixes.foreach{ prefix =>
            if (nm.startsWith(prefix))
                throw new Exception(cState.errFmt.illegalCallName(call))
        }
        Utils.reservedSubstrings.foreach{ sb =>
            if (nm contains sb)
                throw new Exception(cState.errFmt.illegalCallName(call))
        }
        nm
    }

    def genBashScript(appKind: AppletKind.Value, docker: Option[String]) : String = {
        appKind match {
            case AppletKind.Common =>
                s"""|#!/bin/bash -ex
                    |main() {
                    |    echo "working directory =$${PWD}"
                    |    echo "home dir =$${HOME}"
                    |    echo "user= $${USER}"
                    |    java -cp $${DX_FS_ROOT}/dxWDL.jar:$${CLASSPATH} dxWDL.Main workflowCommon $${DX_FS_ROOT}/*.wdl $${HOME}
                    |}""".stripMargin.trim

            case AppletKind.Scatter =>
                s"""|#!/bin/bash -ex
                    |main() {
                    |    echo "working directory =$${PWD}"
                    |    echo "home dir =$${HOME}"
                    |    echo "user= $${USER}"
                    |    java -cp $${DX_FS_ROOT}/dxWDL.jar:$${CLASSPATH} dxWDL.Main launchScatter $${DX_FS_ROOT}/*.wdl $${HOME}
                    |}""".stripMargin.trim

            case AppletKind.Command =>
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

                //# TODO: make sure there is exactly one WDL file
                s"""|#!/bin/bash
                    |main() {
                    |    set -ex
                    |    echo "working directory =$${PWD}"
                    |    echo "home dir =$${HOME}"
                    |    echo "user= $${USER}"
                    |
                    |    # evaluate input arguments, and download input files
                    |    java -cp $${DX_FS_ROOT}/dxWDL.jar:$${CLASSPATH} dxWDL.Main appletProlog $${DX_FS_ROOT}/*.wdl $${HOME}
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
                    |    java -cp $${DX_FS_ROOT}/dxWDL.jar:$${CLASSPATH} dxWDL.Main appletEpilog $${DX_FS_ROOT}/*.wdl $${HOME}
                    |}""".stripMargin.trim
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
                              appKind: AppletKind.Value,
                              docker: Option[String]) : Path = {
        // create temporary directory
        val appletDir : Path = Utils.appCompileDirPath.resolve(appletFqnName)
        Utils.safeMkdir(appletDir)

        // Clean up the task subdirectory, if it it exists
        Utils.deleteRecursive(appletDir.toFile())
        Files.createDirectory(appletDir)

        val srcDir = Files.createDirectory(Paths.get(appletDir.toString(), "src"))
        val resourcesDir = Files.createDirectory(Paths.get(appletDir.toString(), "resources"))

        val bashScript : String = genBashScript(appKind, docker)

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


    // Figure out which instance to use.
    //
    //   Extract three fields from the task:
    // RAM, disk space, and number of cores. These are WDL expressions
    // that, in the general case, we could calculate them only at runtime.
    // Currently, we support only constants. If a runtime expression is used,
    // we convert it to a moderatly high constant.
    def calcInstanceType(task: Task) : String = {
        def lookup(varName : String) : WdlValue = {
            throw new DynamicInstanceTypesException()
        }
        def evalAttr(attrName: String, defaultValue: WdlValue) : Option[WdlValue] = {
            task.runtimeAttributes.attrs.get(attrName) match {
                case None => None
                case Some(expr) =>
                    try {
                        Some(expr.evaluate(lookup, NoFunctions).get)
                    } catch {
                        case e : DynamicInstanceTypesException =>
                            val v = Some(defaultValue)
                            System.err.println(
                                s"""|Warning: runtime expressions are used in
                                    |task ${task.name}, but are currently not supported.
                                    |Default ${defaultValue} substituted for
                                    |attribute ${attrName}."""
                                    .stripMargin.trim)
                            Some(defaultValue)
                    }
            }
        }

        val memory = evalAttr("memory", WdlString("60 GB"))
        val diskSpace = evalAttr("disks", WdlString("local-disk 400 HDD"))
        val cores = evalAttr("cpu", WdlInteger(8))
        InstanceTypes.apply(memory, diskSpace, cores)
    }

    // Set the run spec.
    //
    def calcRunSpec(task : Option[Task], destination : String, dxWDLrtId: String) : JsValue = {
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
        val iType = task match {
            case None =>
                // A utility calculation, that requires minimal computing resources.
                // For example, the top level of a scatter block. We use
                // the default instance type, because one will probably be available,
                // and it will probably be inexpensive.
                InstanceTypes.getMinimalInstanceType()
            case Some(task) =>
                calcInstanceType(task)
        }
        val instanceType: Map[String, JsValue] =
            Map("systemRequirements" ->
                    JsObject("main" ->
                                 JsObject("instanceType" -> JsString(iType))))
        JsObject(runSpec ++ instanceType)
    }

    // Update a closure with all the variables required
    // for an expression.
    //
    // @param  closure   call closure
    // @param  env       mapping from fully qualified WDL name to a dxlink
    // @param  expr      expression as it appears in source WDL
    def updateClosure(closure : Closure,
                      env : CallEnv,
                      expr : WdlExpression,
                      strict : Boolean,
                      cState: State) : Closure = {
        expr.ast match {
            case t: Terminal =>
                val srcStr = t.getSourceString
                t.getTerminalStr match {
                    case "identifier" =>
                        val cVar : CompileVar = env.get(srcStr) match {
                            case Some(x) => x
                            case None => throw new Exception(cState.errFmt.missingVarRefException(t))
                        }
                        val fqn = WdlExpression.toString(t)
                        closure + (fqn -> cVar)
                    case _ => closure
                }

            case a: Ast if a.isMemberAccess =>
                // This is a case of accessing something like A.B.C
                // The RHS is C, and the LHS is A.B
                val rhs : Terminal = a.getAttribute("rhs") match {
                    case rhs:Terminal if rhs.getTerminalStr == "identifier" => rhs
                    case _ => throw new Exception(cState.errFmt.rightSideMustBeIdentifer(a))
                }

                // The FQN is "A.B.C"
                val fqn = WdlExpression.toString(a)
                env.get(fqn) match {
                    case Some(cVar) =>
                        closure + (fqn -> cVar)
                    case None =>
                        if (strict)
                            throw new Exception(cState.errFmt.missingVarRefException(a))
                        closure
                }
            case a: Ast if a.isFunctionCall || a.isUnaryOperator || a.isBinaryOperator
                  || a.isTupleLiteral || a.isArrayOrMapLookup =>
                // Figure out which variables are needed to calculate this expression,
                // and add bindings for them
                val memberAccesses = a.findTopLevelMemberAccesses().map(
                    // This is an expression like A.B.C
                    varRef => WdlExpression.toString(varRef)
                )
                val variables = AstTools.findVariableReferences(a).map{ case t:Terminal =>
                    WdlExpression.toString(t)
                }
                val allDeps = (memberAccesses ++ variables).map{ fqn =>
                    env.get(fqn) match {
                        case Some(cVar) => Some(fqn -> cVar)

                        // There are cases where
                        // [findVariableReferences] gives us previous
                        // call names. We still don't know how to get rid of those cases.
                        case None => None
                        //case None => throw new Exception(s"Variable ${fqn} is not found in the scope expr=${expr.toWdlString}")
                    }
                }.flatten
                closure ++ allDeps

            case a: Ast =>
                throw new Exception(cState.errFmt.notCurrentlySupported(
                                        a,
                                        s"The expression ${expr.toString} is currently not handled"))
        }
    }


    // Calculate the applet input specification from the call closure
    def closureToAppletInputSpec(closure : Closure, cState: State) : Seq[JsValue] = {
        closure.map {
            case (varName, cVar) => wdlVarToSpec(varName, cVar.wvl.wdlType, cVar.ast, cState)
        }.toList.flatten
    }

    // Calculate the stage inputs from the call closure
    //
    // It comprises mappings from variable name to WdlType.
    def closureToStageInputs(closure : Closure, cState: State) : JsonNode = {
        var builder : DXJSON.ObjectBuilder = DXJSON.getObjectBuilder()
        closure.foreach {
            case (varName, cVar) =>
                cVar.wvl.dxlink match {
                    case Some(l) =>
                        WdlVarLinks.genFields(cVar.wvl, varName).foreach{
                            case(fieldName, jsonNode) =>
                                builder = builder.put(fieldName, jsonNode)
                        }
                    case None =>
                        // We do not have a value for this input at compile time.
                        //
                        // Is this legal? Could the user provide it at runtime?
                        throw new Exception(cState.errFmt.missingVarRefException(cVar.ast))
                }
        }
        builder.build()
    }


    // Perform a "dx build" on a local directory representing an applet
    //
    def dxBuildApp(appletDir : Path,
                   appletFqn : String,
                   destination : String,
                   verbose: Boolean) : DXApplet = {
        trace(verbose, s"Building applet ${appletFqn}")
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

    // Create a preliminary stage to handle workflow inputs, top-level
    // expressions and constants.
    //
    // For example, a workflow could start like:
    //
    // workflow PairedEndSingleSampleWorkflow {
    //   String tag = "v1.56"
    //   String gvcf_suffix
    //   File ref_fasta
    //   String cmdline="perf mem -K 100000000 -p -v 3 $tag"
    //   ...
    // }
    //
    //  The first varible [tag] has a default value, and does not have to be
    //  defined on the command line. The next two ([gvcf_suffix, ref_fasta])
    //  must be defined on the command line. [cmdline] is a calculation that
    //  uses constants and variables.
    //
    def compileCommon(wf : Workflow,
                        topDeclarations: Seq[Declaration],
                        env : CallEnv,
                        cState: State) : (DXApplet, Closure, String, Seq[VarOutput]) = {
        trace(cState.verbose, s"Compiling workflow initialization sequence")
        // We need minimal compute resources, use the default instance type
        val runSpec : JsValue = calcRunSpec(None, cState.destination, cState.dxWDLrtId)
        val appletFqn : String = wf.unqualifiedName ++ "." ++ Utils.COMMON

        // Only workflow declarations that do not have an expression,
        // needs to be provide by the user.
        //
        // Examples:
        //   File x
        //   String y = "abc"
        //   Float pi = 3 + .14
        //
        // x - must be provided as an applet input
        // y, pi -- calculated, non inputs
        val inputSpec : Seq[JsValue] =  topDeclarations.map{ decl =>
            decl.expression match {
                case None => wdlVarToSpec(decl.unqualifiedName, decl.wdlType, decl.ast, cState)
                case Some(_) => List()
            }
        }.flatten
        val outputSpec : Seq[JsValue] = topDeclarations.map{ decl =>
            wdlVarToSpec(decl.unqualifiedName, decl.wdlType, decl.ast, cState)
        }.flatten

        val attrs = Map(
            "name" -> JsString(appletFqn),
            "inputSpec" -> JsArray(inputSpec.toVector),
            "outputSpec" -> JsArray(outputSpec.toVector),
            "runSpec" -> runSpec
        )

        // create a directory structure for this applet
        val appletDir = createAppletDirStruct(appletFqn, cState.wdlSourceFile,
                                              JsObject(attrs), AppletKind.Common, None)
        val dxapp = dxBuildApp(appletDir, appletFqn, cState.destination, cState.verbose)
        val closure = Map.empty[String, CompileVar]
        val outputs = topDeclarations.map(decl =>
            VarOutput(decl.unqualifiedName, decl.wdlType, decl.ast)
        )
        (dxapp, closure, Utils.COMMON, outputs)
    }

    // Figure out which required inputs are unbound. Print out a warning,
    // and return a list.
    def unboundRequiredTaskInputs(call: Call, cState: State) : Seq[CompileVar] = {
        val task = Utils.taskOfCall(call)
        val provided = call.inputMappings.map{ case (varName, _) => varName }.toSet
        val required = task.declarations.map{ decl =>
            decl.expression match {
                case Some(_) =>
                    // Variable calculation
                    None
                case None =>
                    // An input, filter out optionals
                    if (Utils.isOptional(decl.wdlType)) {
                        None
                    } else {
                        Some((decl.unqualifiedName, decl))
                    }
            }
        }.flatten
        // Check for each required variable, if it is provided
        required.map{ case (varName, decl) =>
            if (provided contains varName) {
                None
            } else {
                val callName = callUniqueName(call, cState)
                System.err.println(s"""|Note: workflow doesn't supply required input
                                       |${varName} to call ${callName};
                                       |leaving corresponding DNAnexus workflow input unbound"""
                                       .stripMargin.replaceAll("\n", " "))
                Some(CompileVar(WdlVarLinks(varName, decl.wdlType, None), decl.ast))
            }
        }.flatten
    }

    // List task optional inputs, there were NOT provided in the call.
    def unboundOptionalTaskInputs(call: Call) : Seq[CompileVar] = {
        val task = Utils.taskOfCall(call)
        val provided = call.inputMappings.map{ case (varName, _) => varName }.toSet
        val optionals = task.declarations.map{ decl =>
            decl.expression match {
                case Some(_) =>
                    // Variable calculation
                    None
                case None =>
                    // An input, filter out optionals
                    if (Utils.isOptional(decl.wdlType)) {
                        Some((decl.unqualifiedName, decl))
                    } else {
                        None
                    }
            }
        }.flatten
        // Check for each optional variable, if it is provided
        optionals.map{ case (varName, decl) =>
            if (provided contains varName) {
                None
            } else {
                Some(CompileVar(WdlVarLinks(varName, decl.wdlType, None),
                                decl.ast))
            }
        }.flatten
    }

    def unboundOrOptionalInputsSpec(call: Call, cState: State) : Seq[JsValue] = {
        // Allow the user to provide required inputs, that the workflow
        // does not give. Export them as optional arguments on the platform.
        val unboundRequiredInputs :Seq[CompileVar] = unboundRequiredTaskInputs(call, cState)
        val unboundSpec = unboundRequiredInputs.map(x =>
            wdlVarToSpec(x.wvl.varName, x.wvl.wdlType, x.ast, cState)
        )

        // Allow the user to provide optionals, not provided in the
        // workflow, through the platform.
        val unboundOptionalInputs :Seq[CompileVar] = unboundOptionalTaskInputs(call)
        val optsSpec = unboundOptionalInputs.map(cVar =>
            wdlVarToSpec(cVar.wvl.varName, cVar.wvl.wdlType, cVar.ast, cState, mkDxOpt=true)
        )
        unboundSpec.flatten ++ optsSpec.flatten
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
    def compileCall(wf : Workflow,
                    call: Call,
                    env : CallEnv,
                    cState: State) : (DXApplet, Closure, String, Seq[VarOutput]) = {
        val callUName = callUniqueName(call, cState)
        trace(cState.verbose, s"Compiling call ${callUName}")
        val task = Utils.taskOfCall(call)
        val outputDecls : Seq[JsValue] =
            task.outputs.map(tso =>
                wdlVarToSpec(tso.unqualifiedName, tso.wdlType, tso.ast, cState)
            ).flatten
        val runSpec : JsValue = calcRunSpec(Some(task), cState.destination, cState.dxWDLrtId)

        var closure = Map.empty[String, CompileVar]
        call.inputMappings.foreach { case (_, expr) =>
            closure = updateClosure(closure, env, expr, true, cState)
        }
        val inputSpec : Seq[JsValue] = closureToAppletInputSpec(closure, cState)
        val unboundOrOptionalSpec : Seq[JsValue] = unboundOrOptionalInputsSpec(call, cState)
        val appletFqn : String = wf.unqualifiedName ++ "." ++ callUName

        val attrs = Map(
            "name" -> JsString(appletFqn),
            "inputSpec" -> JsArray(inputSpec.toVector ++ unboundOrOptionalSpec.toVector),
            "outputSpec" -> JsArray(outputDecls.toVector),
            "runSpec" -> runSpec
        )

        // Figure out if we need to use docker
        val docker = task.runtimeAttributes.attrs.get("docker") match {
            case None => None
            case Some(wdlExpression) =>
                var buf = wdlExpression.toWdlString
                buf = buf.replaceAll("\"","")
                Some(buf)
        }
        // Even scatters need network access, because
        // they spawn subjobs that use dx-docker.
        // We end up allowing all applets to use the network
        val networkAccess: Map[String, JsValue] =
            Map("access" -> JsObject(Map("network" -> JsArray(JsString("*")))))
        val json = JsObject(attrs ++ networkAccess)

        // create a directory structure for this applet
        val appletDir = createAppletDirStruct(appletFqn, cState.wdlSourceFile, json,
                                              AppletKind.Command, docker)
        val dxapp = dxBuildApp(appletDir, appletFqn, cState.destination, cState.verbose)
        val outputs = task.outputs.map(tso => VarOutput(tso.unqualifiedName, tso.wdlType, tso.ast))
        (dxapp, closure, callUName, outputs)
    }

    // Compile a scatter block
    def compileScatter(wf : Workflow,
                       scatter: Scatter,
                       env : CallEnv,
                       cState: State) : (DXApplet, Closure, String, Seq[VarOutput]) = {
        def unsupported(ast: Ast, msg: String) : String = {
            cState.errFmt.notCurrentlySupported(ast, msg)
        }
        val (topDecls, rest) = Utils.splitBlockDeclarations(scatter.children.toList)
        val calls : Seq[Call] = rest.map {
            case call: Call => call
            case decl: Declaration =>
                throw new Exception(unsupported(decl.ast, "Declarations in the middle of a scatter block"))
            case ssc: Scatter =>
                throw new Exception(unsupported(ssc.ast, "Nested scatters"))
            case swf: Workflow =>
                throw new Exception(unsupported(swf.ast, "Nested workflow inside scatter"))
        }
        if (calls.isEmpty)
            throw new Exception(unsupported(scatter.ast, "Scatters with no calls"))

        // Construct a unique stage name by adding "scatter" to the
        // first call name. This is guaranteed to be unique within a
        // workflow, because call names must be unique (or aliased)
        val stageName = Utils.SCATTER ++ "___" ++ callUniqueName(calls(0), cState)
        trace(cState.verbose, "compiling scatter ${stageName}")

        // Construct the block output by unifying individual call outputs.
        // Each applet output becomes an array of that type. For example,
        // an Int becomes an Array[Int].
        val outputDecls : Seq[VarOutput] = calls.map { call =>
            val task = Utils.taskOfCall(call)
            task.outputs.map { tso =>
                VarOutput(callUniqueName(call, cState) ++ "." ++ tso.unqualifiedName,
                          WdlArrayType(tso.wdlType), tso.ast)
            }
        }.flatten

        // We need minimal compute resources to execute a scatter
        val runSpec : JsValue = calcRunSpec(None, cState.destination, cState.dxWDLrtId)

        // Figure out the closure, we need the expression being looped over.
        var closure = Map.empty[String, CompileVar]
        closure = updateClosure(closure, env, scatter.collection, true, cState)

        // Figure out the type of the iteration variable,
        // and ignore it for closure purposes
        val typeEnv : Map[String, WdlType] = env.map { case (x,cVar) => x -> cVar.wvl.wdlType }.toMap
        val iterVarType : WdlType = Utils.calcIterWdlType(scatter, typeEnv)
        val cVar = CompileVar(WdlVarLinks(scatter.item, iterVarType, None), scatter.ast)
        var innerEnv : CallEnv = env + (scatter.item -> cVar)

        // Add the declarations at the top of the block
        val localDecls = topDecls.map( decl =>
            decl.unqualifiedName -> CompileVar(WdlVarLinks(decl.unqualifiedName, decl.wdlType, None),
                                               decl.ast)
        ).toMap
        innerEnv = innerEnv ++ localDecls

        // Get closure dependencies from the top declarations
        topDecls.foreach { decl =>
            decl.expression match {
                case Some(expr) =>
                    closure = updateClosure(closure, innerEnv, expr, false, cState)
                case None => ()
            }
        }
        calls.foreach { call =>
            call.inputMappings.foreach { case (_, expr) =>
                closure = updateClosure(closure, innerEnv, expr, false, cState)
            }
        }

        // remove the iteration variable from the closure
        closure -= scatter.item
        // remove the local variables from the closure
        topDecls.foreach( decl =>
            closure -= decl.unqualifiedName
        )
        trace(cState.verbose, s"scatter closure=${closure}")
        val inputSpec : Seq[JsValue] = closureToAppletInputSpec(closure, cState)
        val outputSpec : Seq[JsValue] = outputDecls.map(tso =>
            wdlVarToSpec(tso.unqualifiedName, tso.wdlType, tso.ast, cState)
        ).flatten
        val scatterFqn = wf.unqualifiedName ++ "." ++ stageName
        val json = JsObject(
            "name" -> JsString(scatterFqn),
            "inputSpec" -> JsArray(inputSpec.toVector),
            "outputSpec" -> JsArray(outputSpec.toVector),
            "runSpec" -> runSpec,
            "access" -> JsObject(Map("network" -> JsArray(JsString("*"))))
        )

        // create a directory structure for this applet
        val appletDir = createAppletDirStruct(scatterFqn, cState.wdlSourceFile, json,
                                              AppletKind.Scatter, None)
        val dxapp = dxBuildApp(appletDir, scatterFqn, cState.destination, cState.verbose)

        // Compile each of the calls in the scatter block into an applet.
        // These will be called from the scatter stage at runtime.
        calls.foreach { call =>
            val (_, _, _, outputs) =
                compileCall(wf, call, innerEnv, cState)
            // Add bindings for all call output variables. This allows later calls to refer
            // to these results. Links to their values are unknown at compile time, which
            // is why they are set to None.
            for (tso <- outputs) {
                val fqVarName = callUniqueName(call, cState) ++ "." ++ tso.unqualifiedName
                innerEnv = innerEnv + (fqVarName ->
                                           CompileVar(WdlVarLinks(fqVarName, tso.wdlType, None),
                                                      call.ast))
            }
        }

        (dxapp, closure, stageName, outputDecls)
    }

    def compileWorkflow(wf: Workflow,
                        dxwfl: DXWorkflow,
                        cState: State) : DXWorkflow = {
        def unsupported(ast: Ast, msg: String) : String = {
            cState.errFmt.notCurrentlySupported(ast, msg)
        }
        // An environment where variables are defined
        var env : CallEnv = Map.empty[String, CompileVar]

        // Deal with the toplevel declarations, and leave only
        // children we can deal with. Lift all declarations that can
        // be evaluated at the top of the block. Nested calls to
        // workflows are not supported, neither are any remaining
        // declarations in the middle of the workflow.
        val (topDecls, wfBody) = Utils.splitBlockDeclarations(wf.children.toList)
        val (liftedDecls, wfCore) = Utils.liftDeclarations(topDecls, wfBody)
        val calls : Seq[Scope] = wfCore.map {
            case call: Call => call
            case decl: Declaration =>
                throw new Exception(unsupported(decl.ast,
                                                "Declaration in the middle of workflow could not be lifted"))
            case ssc: Scatter => ssc
            case swf: Workflow =>
                throw new Exception(unsupported(swf.ast,
                                                "Nested workflow inside scatter"))
        }

        // - Create a preliminary stage to handle workflow inputs, and top-level
        //   declarations.
        // - Create a stage per call/scatter-block
        var version : Int = 0
        (wf :: calls.toList).foreach { child =>
            val (applet, closure, stageName, outputs) = child match {
                case _ : Workflow =>
                    compileCommon(wf, topDecls ++ liftedDecls, env, cState)
                case call: Call =>
                    compileCall(wf, call, env, cState)
                case scatter : Scatter =>
                    compileScatter(wf, scatter, env, cState)
            }

            val inputs = closureToStageInputs(closure, cState)
            //System.err.println(s"inputs=${Utils.jsValueOfJsonNode(inputs).prettyPrint}")
            val modif : DXWorkflow.Modification[DXWorkflow.Stage] =
                dxwfl.addStage(applet, stageName, inputs, version)
            version = modif.getEditVersion()
            val dxStage : DXWorkflow.Stage = modif.getValue()

            // Add bindings for all call output variables. This allows later calls to refer
            // to these results. In case of scatters, there is no block name to reference.
            for (tso <- outputs) {
                val fqVarName : String = child match {
                    case call : Call => stageName ++ "." ++ tso.unqualifiedName
                    case scatter : Scatter => tso.unqualifiedName
                    case _ : Workflow => tso.unqualifiedName
                    case _ => throw new Exception("Sanity")
                }
                env = env + (fqVarName ->
                                 CompileVar(WdlVarLinks(tso.unqualifiedName, tso.wdlType,
                                                        Some(IORef.Output, DxlStage(dxStage))),
                                            tso.ast))
            }
            //System.err.println(s"env(${stageName}) = ${env}")
        }
        dxwfl
    }

    def apply(ns : WdlNamespace, wdlSourceFile : Path, options: Map[String, String]) : String = {
        // extract the workflow
        val wf = ns match {
            case nswf : WdlNamespaceWithWorkflow => nswf.workflow
            case _ => throw new Exception("WDL does not have a workflow")
        }

        // verify version ID
        options.get("expectedVersion") match {
            case Some(vid) if vid != Utils.VERSION =>
                throw new Exception(s"""|Version mismatch, library is ${Utils.VERSION},
                                        |expected version is ${vid}"""
                                        .stripMargin.replaceAll("\n", " "))
            case _ => ()
        }

        val verbose = options.get("verbose") match {
            case None => false
            case Some(_) => true
        }

        // deal with the various options
        val destination : String = options.get("destination") match {
            case None => ""
            case Some(d) => d
        }

        // There are three possible syntaxes:
        //    project-id:/folder
        //    project-id:
        //    /folder
        val vec = destination.split(":")
        val (project, folder) = vec.length match {
            case 0 => (None, "/")
            case 1 =>
                if (destination.endsWith(":"))
                    (Some(vec(0)), "/")
                else
                    (None, vec(0))
            case 2 => (Some(vec(0)), vec(1))
            case _ => throw new Exception(s"Invalid path syntex ${destination}")
        }

        val dxProject : DXProject = project match {
            case None =>
                // get the default project
                val dxEnv = com.dnanexus.DXEnvironment.create()
                dxEnv.getProjectContext()
            case Some(p) => DXProject.getInstance(p)
        }

        // remove old workflow and applets
        val oldWf = DXSearch.findDataObjects().nameMatchesExactly(wf.unqualifiedName)
            .inFolder(dxProject, folder).withClassWorkflow().execute().asList()
        dxProject.removeObjects(oldWf)
        val oldApplets = DXSearch.findDataObjects().nameMatchesGlob(wf.unqualifiedName + ".*")
            .inFolder(dxProject, folder).withClassWorkflow().execute().asList()
        dxProject.removeObjects(oldApplets)

        val dxWDLrtId: String = options.get("dxWDLrtId") match {
            case None => throw new Exception("dxWDLrt asset ID not specified")
            case Some(id) => id
        }

        // create workflow
        val dxwfl = DXWorkflow.newWorkflow().setProject(dxProject).setFolder(folder)
            .setName(wf.unqualifiedName).build()

        // Wire everything together to create a workflow
        val cState = new State(wdlSourceFile, destination, dxWDLrtId, verbose,
                               new CompileErrorFormatter(wf.wdlSyntaxErrorFormatter.terminalMap))
        compileWorkflow(wf, dxwfl, cState)
        dxwfl.close()
        dxwfl.getId()
    }
}
