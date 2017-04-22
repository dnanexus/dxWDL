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
import spray.json.JsString
import wdl4s._
import wdl4s.types._
import wdl4s.values._
import WdlVarLinks._

object RunnerScatter {
    // Environment where a scatter is executed
    type ScatterEnv = Map[String, WdlVarLinks]

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

    // find the sequence of applets inside the scatter block. In this example,
    // these are: [inc]
    def findAppletsInScatterBlock(wf : Workflow,
                                  scatter : Scatter,
                                  project : DXProject) : Seq[(Call, DXApplet)] = {
        val children : Seq[TaskCall] = scatter.children.map {
            case call: TaskCall => Some(call)
            case call: WorkflowCall => throw new Exception("Calling workflows not supported")
            case _ => None
        }.flatten

        // Match each call with its dx:applet
        children.map { call =>
            val dxAppletName = call.task.name
            val applets : List[DXApplet] = DXSearch.findDataObjects().inProject(project).
                nameMatchesExactly(dxAppletName).withClassApplet().execute().asList().asScala.toList
            if (applets.size == 0)
                throw new Exception(s"Could not find applet ${dxAppletName}")
            if (applets.size > 1)
                throw new Exception(s"More than one applet called ${dxAppletName} in project ${project.getId()}")
            (call, applets.head)
        }
    }

    // evaluate call input expressions
    def evalExpression(expr: WdlExpression, env: ScatterEnv) : WdlValue= {
        def lookup(varName : String) : WdlValue =
            env.get(varName) match {
                case Some(wvl) => wdlValueOfInputField(wvl)
                case None =>
                    System.err.println(s"Could not find variable ${varName} in environment ${env}")
                    throw new AppInternalException(s"Evaluating ${expr.toWdlString}, variable ${varName} unbound")
            }
        expr.evaluate(lookup, DxFunctions).get
    }

    /**
      In the workflow below, we want to correctly pass the [k] value
      to each [inc] Task invocation.

    scatter (k in integers) {
        call inc as inc {input: i=k}
    }
      */
    def buildAppletInputs(call: Call,
                          inputSpec : List[InputParameter],
                          env : ScatterEnv) : ObjectNode = {
        // Figure out which wdl fields this applet needs
        val appInputVars = inputSpec.map{ spec =>
            val name = spec.getName()
            if (name.endsWith(Utils.FLAT_FILE_ARRAY_SUFFIX))
                None
            else
                Some((Utils.appletVarNameStripSuffix(name), spec))
        }.flatten

        val allEnvVars: Vector[IR.CVar] = env.map{ case (varName, wvl) =>
            val wvl2 = WdlVarLinks(varName, wvl.wdlType, wvl.dxlink)
            wvl2.cVar
        }.toVector

        var builder : DXJSON.ObjectBuilder = DXJSON.getObjectBuilder()
        appInputVars.foreach{ case (varName, spec) =>
            // The lhs is [k], the varName is [i]
            val lhs: Option[(String,WdlExpression)] =
                call.inputMappings.find{ case(key, expr) => key == varName}
            val callerVar = lhs match {
                case None =>
                    // A value for [i] is not provided
                    if (!spec.isOptional()) {
                        val provided = call.inputMappings.map{ case (key, expr) => key}.toVector
                        throw new AppInternalException(
                            s"""|Could not find binding for required variable ${varName}.
                                |The call bindings are ${provided}""".stripMargin.trim)
                    }
                case Some((_, expr)) =>
                    // Member accesses require caution. For example [inc1.incremented] needs
                    // to be replaced with [inc1_incremented]
                    val rExpr = IR.exprRenameVars(expr, allEnvVars)
                    val wValue = evalExpression(rExpr, env)
                    val wvl = WdlVarLinks.outputFieldOfWdlValue(varName, wValue.wdlType, wValue)
                    WdlVarLinks.genFields(wvl, varName).foreach{ case (fieldName, jsNode) =>
                        builder = builder.put(fieldName, jsNode)
                    }
            }
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
            fqn -> WdlVarLinks(tso.unqualifiedName, tso.wdlType, Some(IORef.Output, DxlJob(dxJob)))
        }.toMap
    }

    // Gather outputs from calls in a scatter block into arrays. Essentially,
    // take a list of WDL values, and convert them into a wdl array. The complexity
    // comes in, because we need to carry around Json links to platform data objects.
    //
    // Note: in case the phase list is empty, we need to provide a sensible output
    def gatherOutputs(scOutputs : List[ScatterEnv]) : Map[String, JsValue] = {
        if (scOutputs.length == 0) {
            // TODO deal with this better
            throw new AppInternalException("gather list is empty")
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
                    calls : Seq[(Call, DXApplet)],
                    outerScopeEnv : ScatterEnv) : Map[String, JsValue] = {
        // add the top declarations in the scatter block to the
        // environment
        val (topDecls,_) = Utils.splitBlockDeclarations(scatter.children.toList)
        val env: ScatterEnv = RunnerEval.evalDeclarations(topDecls, outerScopeEnv).toMap

        // Figure out the input/output specs for each applet.
        // Do this once per applet in the loop.
        val phases = calls.map { case (call, dxApplet) =>
            val d = dxApplet.describe()
            val inputSpec : List[InputParameter] = d.getInputSpecification().asScala.toList
            (call, dxApplet, inputSpec)
        }
        val collElements : Seq[WdlVarLinks] = WdlVarLinks.unpackWdlArray(collection)
        var scOutputs : List[ScatterEnv] = List()
        collElements.foreach { case elem =>
            // Bind the iteration variable inside the loop
            var innerEnv = env + (scatter.item -> elem)

            phases.foreach { case (call,dxApplet,inputSpec) =>
                val inputs : ObjectNode = buildAppletInputs(call, inputSpec, innerEnv)
                System.err.println(s"call=${callUniqueName(call)} inputs=${inputs}")
                val dxJob : DXJob = dxApplet.newRun().setRawInput(inputs).run()
                val jobOutputs : ScatterEnv = jobOutputEnv(call, dxJob)

                // add the job outputs to the environment. This makes them available to the applets
                // that come next.
                innerEnv = innerEnv ++ jobOutputs
                scOutputs = scOutputs :+ jobOutputs
            }
        }

        // Gather phase. Collect call outputs in arrays, do not wait
        // for the jobs to complete.
        gatherOutputs(scOutputs)
    }

    def apply(wf: Workflow,
              jobInputPath : Path,
              jobOutputPath : Path,
              jobInfoPath: Path) : Unit = {
        // Extract types for closure inputs
        val (closureTypes,_) = Utils.loadExecInfo(Utils.readFileContent(jobInfoPath))
        System.err.println(s"WdlType mapping =${closureTypes}")

        // Parse the inputs, do not download files from the platform.
        // They will be passed as links to the tasks.
        val inputLines : String = Utils.readFileContent(jobInputPath)
        val outScopeEnv : ScatterEnv = WdlVarLinks.loadJobInputsAsLinks(inputLines, closureTypes)
        val scatter : Scatter = findScatter(wf)

        // We need only the array we are looping on. Note that
        // we *do not* want to download the files, if we are looping on
        // a file array.
        //
        // In the future, we will also need to calculate the expressions
        // at the top of the block.
        val collElements = outScopeEnv.get(scatter.collection.toWdlString) match {
            case None => throw new AppInternalException(
                s"Could not find the collection array ${scatter.collection.toWdlString} in the job inputs")
            case Some(wvl) => wvl
        }

        val dxEnv = DXEnvironment.create()
        val project = dxEnv.getProjectContext()
        val applets : Seq[(Call, DXApplet)] = findAppletsInScatterBlock(wf, scatter, project)
        val outputs : Map[String, JsValue] = evalScatter(scatter, collElements, applets, outScopeEnv)

        // write the outputs to the job_output.json file
        val json = JsObject(outputs)
        val ast_pp = json.prettyPrint
        System.err.println(s"outputs = ${ast_pp}")
        Utils.writeFileContent(jobOutputPath, ast_pp)
    }
}
