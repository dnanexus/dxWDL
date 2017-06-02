/** Execute a scatter block on the platform

The canonical example for what is happening here, is the workflow below.
The scatter block has two calls, and it iterates over the "integers"
array.

workflow scatter {
    Array[Int] integers

    scatter (i in integers) {
        call inc as inc1 {input: i=i}
        call inc as inc2 {input: i=inc1.incremented}
    }

  output {
    Array[Int] inc1_result = inc1.result
    Array[Int] inc2_result = inc2.result
  }
}
  */

package dxWDL

// DX bindings
import com.dnanexus._
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import java.nio.file.{Path, Paths, Files}
import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import spray.json._
import spray.json.DefaultJsonProtocol
import Utils.AppletLinkInfo
import wdl4s._
import wdl4s.expression._
import wdl4s.parser.WdlParser.{Ast, AstNode, Terminal}
import wdl4s.types._
import wdl4s.values._
import wdl4s.WdlExpression.AstForExpressions
import WdlVarLinks._

object RunnerScatter {
    // Environment where a scatter is executed
    type ScatterEnv = Map[String, WdlVarLinks]

    // Runtime state.
    // Packs common arguments passed between methods.
    case class State(cef: CompilerErrorFormatter,
                     verbose: Boolean)

    // Find the scatter block.
    def findScatter(wf : Workflow) : Scatter = {
        val scatters: Set[Scatter] = wf.scatters
        if (scatters.size != 1)
            throw new Exception("Workflow has more than one scatter block")
        scatters.head
    }

    def callUniqueName(call : Call) : String = {
        call.alias match {
            case Some(x) => x
            case None => Utils.taskOfCall(call).name
        }
    }

    // find the sequence of applets inside the scatter block. In the example,
    // these are: [inc1, inc2]
    def findAppletsInScatterBlock(scatter : Scatter,
                                  linkInfo: Map[String, AppletLinkInfo]) : Seq[(Call, AppletLinkInfo)] = {
        // Match each call with its dx:applet
        scatter.children.map {
            case call: TaskCall =>
                val dxAppletName = call.task.name
                linkInfo.get(dxAppletName) match {
                    case Some(x) => Some((call, x))
                    case None =>
                        throw new AppInternalException(
                            s"Could not find linking information for ${dxAppletName}")
                }
            case call: WorkflowCall => throw new Exception("Calling workflows not supported")
            case _ => None
        }.flatten
    }

    // Evaluate a call input expression, and return a WdlVarLink structure. It
    // is passed on to another dx:job.
    //
    // The front end pass makes sure that the range of expressions is
    // limited: constants, variables, and member accesses.
    def wvlEvalExpression(expr: WdlExpression,
                          env : ScatterEnv,
                          rState: State) : WdlVarLinks = {
        expr.ast match {
            case t: Terminal =>
                val srcStr = t.getSourceString
                t.getTerminalStr match {
                    case "identifier" =>
                        env.get(srcStr) match {
                            case Some(x) => x
                            case None => throw new Exception(rState.cef.missingVarRefException(t))
                        }
                    case _ =>
                        // a constant
                        def nullLookup(varName : String) : WdlValue = {
                            throw new Exception(rState.cef.expressionMustBeConstOrVar(expr))
                        }
                        val wdlValue = expr.evaluate(nullLookup, NoFunctions).get
                        WdlVarLinks.apply(wdlValue.wdlType, wdlValue)
                }

            case a: Ast if a.isMemberAccess =>
                // Accessing something like A.B.C
                val fqn = WdlExpression.toString(a)
                env.get(fqn) match {
                    case Some(x)=> x
                    case None =>
                        System.err.println(s"Cannot find ${fqn} in env=${env}")
                        throw new Exception(rState.cef.undefinedMemberAccess(a))
                }
            case _:Ast =>
                throw new Exception(rState.cef.expressionMustBeConstOrVar(expr))
        }
    }


    // remove persistent resources used by this variable
    private def cleanup(wdlValue: WdlValue) : Unit = {
        wdlValue match {
            case WdlSingleFile(path) =>
                Files.delete(Paths.get(path))
            case WdlArray(WdlArrayType(WdlFileType), files) =>
                files.map{ case x : WdlSingleFile =>
                    val path = x.value
                    Files.delete(Paths.get(path))
                }
            // TODO: there may be other structures that have files as
            // members, we need to delete those files as well.
            case _ => ()
        }
    }

    /**
      In the workflow below, we want to correctly pass the [k] value
      to each [inc] Task invocation.

    scatter (k in integers) {
        call inc as inc {input: i=k}
    }
      */
    def buildAppletInputs(call: Call,
                          apLinkInfo: AppletLinkInfo,
                          env : ScatterEnv,
                          rState: State) : ObjectNode = {
        val callName = callUniqueName(call)
        val appInputs: Map[String, Option[WdlVarLinks]] = apLinkInfo.inputs.map{ case (varName, wdlType) =>
            // The lhs is [k], the varName is [i]
            val lhs: Option[(String,WdlExpression)] =
                call.inputMappings.find{ case(key, expr) => key == varName }
            val wvl:Option[WdlVarLinks] = lhs match {
                case None =>
                    // A value for [i] is not provided in the call.
                    // Check if it was passed in the environment
                    env.get(s"${callName}_${varName}") match {
                        case None =>
                            if (Utils.isOptional(wdlType)) {
                                None
                            } else {
                                val provided = call.inputMappings.map{ case (key, expr) => key}.toVector
                                throw new AppInternalException(
                                    s"""|Could not find binding for required variable ${varName}.
                                        |The call bindings are ${provided}""".stripMargin.trim)
                            }
                        case Some(wvl) => Some(wvl)
                    }
                case Some((_, expr)) =>
                    Some(wvlEvalExpression(expr, env, rState))
            }
            varName -> wvl
        }

        var builder : DXJSON.ObjectBuilder = DXJSON.getObjectBuilder()
        appInputs.foreach{
            case (varName, Some(wvl)) =>
                WdlVarLinks.genFields(wvl, varName).foreach{ case (fieldName, jsNode) =>
                    builder = builder.put(fieldName, jsNode)
                }
            case _ => ()
        }
        builder.build()
    }

    // Create a mapping from the job output variables to json values. These
    // are variables that can be referenced by other calls.
    def jobOutputEnv(call: Call, dxJob: DXJob) : ScatterEnv = {
        val prefix = callUniqueName(call)
        val task = Utils.taskOfCall(call)
        task.outputs.map { tso =>
            val fqn = prefix ++ "." ++ tso.unqualifiedName
            fqn -> WdlVarLinks(tso.wdlType, DxlJob(dxJob, IORef.Output, tso.unqualifiedName))
        }.toMap
    }

    // Gather outputs from calls in a scatter block into arrays. Essentially,
    // take a list of WDL values, and convert them into a wdl array. The complexity
    // comes in, because we need to carry around Json links to platform data objects.
    //
    // Note: in case the phase list is empty, we need to provide a sensible output
    def gatherOutputs(scOutputs : List[ScatterEnv]) : Map[String, JsValue] = {
        if (scOutputs.length == 0) {
            return Map.empty[String, JsValue]
        }

        // Map each individual variable to a list of output fields
        val outputs : List[(String, JsonNode)] =
            scOutputs.map{ scEnv =>
                scEnv.map{ case (varName, wvl) =>
                    WdlVarLinks.genFields(wvl, varName)
                }.toList.flatten
            }.flatten

        // collect values for each key into lists
        val m : HashMap[String, List[JsonNode]] = HashMap.empty
        outputs.foreach{ case (key, jsNode) =>
            m(key) = m.get(key) match {
                case None => List(jsNode)
                case Some(l) => l :+ jsNode
            }
        }
        m.map { case (key, jsl) =>
            key ->  JsArray(jsl.map(Utils.jsValueOfJsonNode).toVector)
        }.toMap
    }

    // Launch a job for each call, and link them with JBORs. Do not
    // wait for the jobs to complete, because that would
    // require leaving auxiliary instance up for the duration of the subjob executions.
    // Return the variables calculated.
    def evalScatter(scatter : Scatter,
                    collection : WdlVarLinks,
                    calls : Seq[(Call, AppletLinkInfo)],
                    outerScopeEnv : ScatterEnv,
                    rState: State) : Map[String, JsValue] = {
        System.err.println(s"evalScatter")

        // add the top declarations in the scatter block to the
        // environment
        val (topDecls,_) = Utils.splitBlockDeclarations(scatter.children.toList)

        val collElements : Seq[WdlVarLinks] = WdlVarLinks.unpackWdlArray(collection)
        var scOutputs : List[ScatterEnv] = List()
        collElements.foreach { case elem =>
            // Bind the iteration variable inside the loop
            val envWithIterItem = outerScopeEnv + (scatter.item -> elem)
            System.err.println(s"envWithIterItem= ${envWithIterItem}")

            // calculate declarations at the top of the block
            val bValues = RunnerEval.evalDeclarations(topDecls, envWithIterItem).toMap
            var innerEnv = bValues.map{ case(key, bVal) => key -> bVal.wvl }.toMap
            innerEnv = innerEnv ++ envWithIterItem

            calls.foreach { case (call,apLinkInfo) =>
                val inputs : ObjectNode = buildAppletInputs(call, apLinkInfo, innerEnv, rState)
                System.err.println(s"call=${callUniqueName(call)} inputs=${inputs}")
                val dxJob : DXJob = apLinkInfo.dxApplet.newRun().setRawInput(inputs).run()
                val jobOutputs : ScatterEnv = jobOutputEnv(call, dxJob)

                // add the job outputs to the environment. This makes them available to the applets
                // that come next.
                innerEnv = innerEnv ++ jobOutputs
                scOutputs = scOutputs :+ jobOutputs
            }

            // Cleanup the index variable; it could be a file holding persistent resources.
            // In particular, the file name is an important resource, if it is not removed,
            // the name cannot be reused in the next loop iteration.
            bValues.get(scatter.item) match {
                case Some(iterElem: BValue) =>
                    cleanup(iterElem.wdlValue)
                case None => ()
            }
        }

        // Gather phase. Collect call outputs in arrays, do not wait
        // for the jobs to complete.
        gatherOutputs(scOutputs)
    }

    // Load from disk a mapping of applet name to id. We
    // need this in order to call the right version of other
    // applets.
    def loadLinkInfo(dxProject: DXProject) : Map[String, AppletLinkInfo]= {
        System.err.println(s"Loading link information")
        val linkSourceFile: Path = Paths.get("/" + Utils.LINK_INFO_FILENAME)
        if (!Files.exists(linkSourceFile))
            Map.empty

        val info: String = Utils.readFileContent(linkSourceFile)
        try {
            info.parseJson.asJsObject.fields.map {
                case (key:String, jso) =>
                    key -> AppletLinkInfo.readJson(jso, dxProject)
                case _ =>
                    throw new AppInternalException(s"Bad JSON")
            }.toMap
        } catch {
            case e : Throwable =>
                throw new AppInternalException(s"Link JSON information is badly formatted ${info}")
        }
    }

    def apply(wf: Workflow,
              jobInputPath : Path,
              jobOutputPath : Path,
              jobInfoPath: Path) : Unit = {
        val cef = new CompilerErrorFormatter(wf.wdlSyntaxErrorFormatter.terminalMap)
        val rState = new State(cef, false)

        // Extract types for closure inputs
        val (closureTypes,_) = Utils.loadExecInfo(Utils.readFileContent(jobInfoPath))
        System.err.println(s"WdlType mapping =${closureTypes}")

        // Parse the inputs, do not download files from the platform.
        // They will be passed as links to the tasks.
        val inputLines : String = Utils.readFileContent(jobInputPath)
        val outScopeEnv : ScatterEnv = WdlVarLinks.loadJobInputsAsLinks(inputLines, closureTypes)
        val scatter : Scatter = findScatter(wf)

        // Lookup the array we are looping on.
        // Note: it could be an expression, which requires calculation
        val collElements = outScopeEnv.get(scatter.collection.toWdlString) match {
            case None => throw new AppInternalException(
                s"Could not find the collection array ${scatter.collection.toWdlString} in the job inputs")
            case Some(wvl) => wvl
        }

        val dxEnv = DXEnvironment.create()
        val dxProject = dxEnv.getProjectContext()
        val linkInfo = loadLinkInfo(dxProject)
        System.err.println(s"link info=${linkInfo}")

        val applets : Seq[(Call, AppletLinkInfo)] = findAppletsInScatterBlock(scatter, linkInfo)
        val outputs : Map[String, JsValue] = evalScatter(scatter, collElements, applets, outScopeEnv, rState)

        // write the outputs to the job_output.json file
        val json = JsObject(outputs)
        val ast_pp = json.prettyPrint
        System.err.println(s"outputs = ${ast_pp}")
        Utils.writeFileContent(jobOutputPath, ast_pp)
    }
}
