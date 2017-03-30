package dxWDL

// DX bindings
import com.dnanexus.{DXApplet, DXEnvironment, DXJob, DXJSON, DXProject, DXSearch, InputParameter, OutputParameter}
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import java.nio.charset.StandardCharsets
import java.nio.file.{Path, Paths, Files}
import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import scala.util.{Try, Success, Failure}
import spray.json._
import spray.json.DefaultJsonProtocol
import spray.json.JsString
import wdl4s.types._
import wdl4s.values._
import wdl4s.{Call, Scatter, Workflow, WdlNamespaceWithWorkflow}
import wdl4s.WdlExpression.AstForExpressions
import WdlVarLinks._

/** Execute a scatter block on the platform

The canonical example for what is happening here, is the workflow below.
The scatter block has a single call, and it iterates over the "integers"
array.

task inc {
    Int i

    command <<<
        python -c "print(${i} + 1)"
    >>>
    output {
        Int incremented = read_int(stdout())
    }
}

task sum {
    Array[Int] ints

    command <<<
        python -c "print(${sep="+" ints})"
    >>>
    output {
        Int sum = read_int(stdout())
    }
}

workflow sg_sum {
    Array[Int] integers

    scatter (i in integers) {
        call inc as inc1 {input: i=i}
        call inc as inc2 {input: i=inc1.incremented}
    }
    call sum {input: ints = inc2.incremented}
}
  */

object ScatterRunner {
    // Environment where a scatter is executed
    type ScatterEnv = Map[String, WdlVarLinks]

    // Find the scatter block.
    // In the example, scatter___inc is the scatter block name.
    def findScatter(wf : Workflow, appName : String) : Scatter = {
        // Find the scatter block with the first call name in the block, which
        // must be unique.
        val components : Array[String] = appName.split("___")
        assert(components(0) == "scatter")
        val firstCallName = components(1)
        val call : Call = wf.findCallByName(firstCallName) match {
            case None => throw new AppInternalException(s"Call ${appName} not found in WDL file")
            case Some(call) => call
        }

        // The parent block of the call must be a scatter
        call.parent match {
            case (Some(scatter : Scatter)) => scatter
            case (Some(_)) =>
                throw new AppInternalException(s"Parent scope of call ${firstCallName} is not a scatter block")
            case None =>
                throw new AppInternalException(s"No parent scope found for call ${firstCallName}")
        }
    }

    def callUniqueName(call : Call) : String = {
        call.alias match {
            case Some(x) => x
            case None => Utils.taskOfCall(call).name
        }
    }

    def callUniqueNameInWorkflow(wf : Workflow, call : Call) : String = {
        wf.unqualifiedName ++ "." + callUniqueName(call)
    }

    // find the sequence of applets inside the scatter block. In this example,
    // these are: [sg_sum.inc, sg_sum.inc2]
    def findAppletsInScatterBlock(wf : Workflow,
                                  scatter : Scatter,
                                  project : DXProject) : Seq[(Call, DXApplet)] = {
        val children : Seq[Call] = scatter.children.map {
            case call: Call => Some(call)
            case _ => None
        }.flatten

        // Find all the applets for the workflow. Performs a single database
        // query on the backend, and should be efficient.
        val pattern = wf.unqualifiedName ++ ".*"
        val applets : List[DXApplet] = DXSearch.findDataObjects().inProject(project).
            nameMatchesGlob(pattern).withClassApplet().execute().asList().asScala.toList
        val name2App : Map[String, DXApplet] = applets.map { x =>
            val appName = x.describe().getName()
            appName -> x
        }.toMap

        // Match each call with its dx-applet
        children.map { call =>
            // find the dxapplet
            val nm = callUniqueNameInWorkflow(wf, call)
            val applet = name2App.get(nm) match {
                case Some(x) => x
                case None => throw new AppInternalException(s"Could not find applet ${nm}")
            }
            (call, applet)
        }
    }

    def buildAppletInputs(inputSpec : List[InputParameter],
                          env : ScatterEnv) : ObjectNode = {
        // Figure out which wdl fields this applet needs
        val appInputVars = inputSpec.map{ spec =>
            val name = spec.getName()
            if (name.endsWith(Utils.FLAT_FILE_ARRAY_SUFFIX))
                None
            else
                Some((Utils.appletVarNameStripSuffix(name), spec))
        }.flatten
        var builder : DXJSON.ObjectBuilder = DXJSON.getObjectBuilder()
        appInputVars.foreach{ case (varName, spec) =>
            env.get(varName) match {
                case None =>
                    if (!spec.isOptional()) {
                        throw new AppInternalException(s"""|Could not find required variable ${varName}
                                                           |in environment ${env}""".stripMargin.trim)
                    }
                case Some(wvl) =>
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
        val env: ScatterEnv = WorkflowCommonRunner.evalDeclarations(topDecls, outerScopeEnv).toMap

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
                val inputs : ObjectNode = buildAppletInputs(inputSpec, innerEnv)
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
        // Query the platform, and get the applet name.
        // It has a fully qualified name A.B, extract the last component (B)
        val dxEnv = DXEnvironment.create()
        val dxapp : DXApplet = dxEnv.getJob().describe().getApplet()
        val desc : DXApplet.Describe = dxapp.describe()
        val appFQName : String = desc.getName()
        val appName = appFQName.split("\\.").last

        // Extract types for closure inputs
        val closureTypes = Utils.loadExecInfo(Utils.readFileContent(jobInfoPath))
        System.err.println(s"WdlType mapping =${closureTypes}")

        // Parse the inputs, do not download files from the platform.
        // They will be passed as links to the tasks.
        val inputLines : String = Utils.readFileContent(jobInputPath)
        val outScopeEnv : ScatterEnv = WdlVarLinks.loadJobInputsAsLinks(inputLines, closureTypes)
        val scatter : Scatter = findScatter(wf, appName)

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
