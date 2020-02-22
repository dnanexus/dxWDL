package dxWDL.exec

import cats.data.Validated.{Invalid, Valid}
import common.validation.ErrorOr.ErrorOr
import java.nio.file.{Path, Paths}
import org.scalatest.{FlatSpec, Matchers}
import spray.json._
import wom.callable.{WorkflowDefinition}
import wom.executable.WomBundle
import wom.expression.WomExpression
import wom.graph._
import wom.graph.expression._
import wom.values._
import wom.types._

import dxWDL.base.{RunnerWfFragmentMode, Utils, WdlRuntimeAttrs}
import dxWDL.dx.ExecLinkInfo
import dxWDL.util.{Block, DxIoFunctions, DxInstanceType, DxPathConfig, InstanceTypeDB, ParseWomSourceFile}

// This test module requires being logged in to the platform.
// It compiles WDL scripts without the runtime library.
// This tests the compiler Native mode, however, it creates
// dnanexus applets and workflows that are not runnable.
class WfFragRunnerTest extends FlatSpec with Matchers {
    private val runtimeDebugLevel = 0
    private val unicornInstance = DxInstanceType("mem_ssd_unicorn",
                                                 100,
                                                 100,
                                                 4,
                                                 1.00F,
                                                 Vector(("Ubuntu", "16.04")),
                                                 false)
    private val instanceTypeDB = InstanceTypeDB(true, Vector(unicornInstance))

    private def setup() : (DxPathConfig, DxIoFunctions) = {
        // Create a clean directory in "/tmp" for the task to use
        val jobHomeDir : Path = Paths.get("/tmp/dxwdl_applet_test")
        Utils.deleteRecursive(jobHomeDir.toFile)
        Utils.safeMkdir(jobHomeDir)
        val dxPathConfig = DxPathConfig.apply(jobHomeDir, false, runtimeDebugLevel >= 1)
        dxPathConfig.createCleanDirs()
        val dxIoFunctions = DxIoFunctions(Map.empty, dxPathConfig, runtimeDebugLevel)
        (dxPathConfig, dxIoFunctions)
    }


    private def setupFragRunner(dxPathConfig: DxPathConfig,
                                dxIoFunctions: DxIoFunctions,
                                wfSourceCode: String) : (WorkflowDefinition, WfFragRunner) = {
        val (wf : WorkflowDefinition, taskDir, typeAliases) = ParseWomSourceFile(false).parseWdlWorkflow(wfSourceCode)
        val fragInputOutput = new WfFragInputOutput(dxIoFunctions,
                                                    null /*dxProject*/,
                                                    runtimeDebugLevel,
                                                    typeAliases)
        val fragRunner = new WfFragRunner(wf,
                                          taskDir,
                                          typeAliases,
                                          wfSourceCode,
                                          instanceTypeDB,
                                          Map.empty[String, ExecLinkInfo],
                                          dxPathConfig,
                                          dxIoFunctions,
                                          JsNull,
                                          fragInputOutput,
                                          Some(WdlRuntimeAttrs(Map.empty)),
                                          Some(false),
                                          runtimeDebugLevel)
        (wf, fragRunner)
    }

    // Note: if the file doesn't exist, this throws a null pointer exception
    def pathFromBasename(dir: String, basename: String) : Path = {
        val p = getClass.getResource(s"/${dir}/${basename}").getPath
        Paths.get(p)
    }


    def evaluateWomExpression(expr: WomExpression,
                              env: Map[String, WomValue],
                              dxIoFunctions : DxIoFunctions) : WomValue = {
        val result: ErrorOr[WomValue] =
            expr.evaluateValue(env, dxIoFunctions)
        result match {
            case Invalid(errors) => throw new Exception(
                s"Failed to evaluate expression ${expr} with ${errors}")
            case Valid(x: WomValue) => x
        }
    }

    it should "second block in a linear workflow" in {
        val source : Path = pathFromBasename("frag_runner", "wf_linear.wdl")
        val (dxPathConfig, dxIoFunctions) = setup()

        val (language, womBundle: WomBundle, allSources, subBundles) =
            ParseWomSourceFile(false).apply(source, List.empty)
        subBundles.size should be(0)
        val wfSource = allSources.values.head

        val wf: WorkflowDefinition = womBundle.primaryCallable match {
            case Some(wf: WorkflowDefinition) => wf
            case _ => throw new Exception("sanity")
        }
        val graph = wf.innerGraph
        val (inputNodes, _, subBlocks, outputNodes) = Block.split(graph, wfSource)

        val block = subBlocks(1)

        val env : Map[String, WomValue] =
            Map("x" -> WomInteger(3),
                "y" -> WomInteger(5),
                "add.result" -> WomInteger(8))

        val eNodes : Vector[ExposedExpressionNode] = block.nodes.collect{
            case eNode: ExposedExpressionNode => eNode
        }
        val expr : WomExpression = eNodes(0).womExpression
        val value: WomValue = evaluateWomExpression(expr, env, dxIoFunctions)
        value should be(WomInteger(9))
    }

    it should "evaluate a scatter without a call" in {
        val path = pathFromBasename("frag_runner", "scatter_no_call.wdl")
        val wfSourceCode = Utils.readFileContent(path)

        val (dxPathConfig, dxIoFunctions) = setup()
        val (wf, fragRunner) = setupFragRunner(dxPathConfig, dxIoFunctions, wfSourceCode)
        val (_, _, subBlocks, _) = Block.split(wf.innerGraph, wfSourceCode)
        val block = subBlocks(0)

        val env = Map.empty[String, WomValue]
        val results : Map[String, WomValue] = fragRunner.evalExpressions(block.nodes, env)
        results.keys should be(Set("names", "full_name"))
        results should be(
            Map(
                "names" -> WomArray(
                    WomArrayType(WomStringType),
                    Vector(WomString("Michael"),
                           WomString("Lukas"),
                           WomString("Martin"),
                           WomString("Shelly"),
                           WomString("Amy"))),
                "full_name" -> WomArray(
                    WomArrayType(WomStringType),
                    Vector(WomString("Michael_Manhaim"),
                           WomString("Lukas_Manhaim"),
                           WomString("Martin_Manhaim"),
                           WomString("Shelly_Manhaim"),
                           WomString("Amy_Manhaim")))
            ))
    }

    it should "evaluate a conditional without a call" in {
        val path = pathFromBasename("frag_runner", "conditional_no_call.wdl")
        val wfSourceCode = Utils.readFileContent(path)

        val (dxPathConfig, dxIoFunctions) = setup()
        val (wf, fragRunner) = setupFragRunner(dxPathConfig, dxIoFunctions, wfSourceCode)
        val (_, _, subBlocks, _) = Block.split(wf.innerGraph, wfSourceCode)
        val block = subBlocks(0)

        val env = Map.empty[String, WomValue]
        val results : Map[String, WomValue] = fragRunner.evalExpressions(block.nodes, env)
        results should be(
            Map("flag" -> WomBoolean(true),
                "cats" -> WomOptionalValue(
                    WomStringType,
                    Some(WomString("Mr. Baggins"))
                )))
    }

    it should "evaluate a nested conditional/scatter without a call" in {
        val path = pathFromBasename("frag_runner", "nested_no_call.wdl")
        val wfSourceCode = Utils.readFileContent(path)

        val (dxPathConfig, dxIoFunctions) = setup()
        val (wf, fragRunner) = setupFragRunner(dxPathConfig, dxIoFunctions, wfSourceCode)
        val (_, _, subBlocks, _) = Block.split(wf.innerGraph, wfSourceCode)

        val results = fragRunner.evalExpressions(subBlocks(0).nodes,
                                                 Map.empty[String, WomValue])
        results should be (Map("z" -> WomOptionalValue(WomMaybeEmptyArrayType(WomIntegerType),None)))
    }

    it should "create proper names for scatter results" in {
        val path = pathFromBasename("frag_runner", "strings.wdl")
        val wfSourceCode = Utils.readFileContent(path)
        val (wf : WorkflowDefinition, _, _) = ParseWomSourceFile(false).parseWdlWorkflow(wfSourceCode)

        val sctNode = wf.innerGraph.scatters.head
        val svNode: ScatterVariableNode = sctNode.scatterVariableNodes.head
        /*System.out.println(s"""|name = ${svNode.identifier.localName.value}
                               |       ${svNode.identifier.workflowLocalName}
                               |       ${svNode.identifier.localName}
                               |""".stripMargin)*/
        svNode.identifier.localName.value should be("x")
        svNode.identifier.workflowLocalName should be("x")
        svNode.identifier.localName.toString should be ("LocalName(x)")
    }

    it should "Make sure calls cannot be handled by evalExpressions" in {
        val path = pathFromBasename("draft2", "shapes.wdl")
        val wfSourceCode = Utils.readFileContent(path)

        val (dxPathConfig, dxIoFunctions) = setup()
        val (wf, fragRunner) = setupFragRunner(dxPathConfig, dxIoFunctions, wfSourceCode)
        val (_, _, subBlocks, _) = Block.split(wf.innerGraph, wfSourceCode)

        // Make sure an exception is thrown if eval-expressions is called with
        // a wdl-call.
        assertThrows[Exception] {
            fragRunner.evalExpressions(subBlocks(1).nodes,
                                       Map.empty[String, WomValue])
        }
    }

    it should "handles draft2" in {
        val path = pathFromBasename("draft2", "conditionals2.wdl")
        val wfSourceCode = Utils.readFileContent(path)

        val (dxPathConfig, dxIoFunctions) = setup()
        val (wf, fragRunner) = setupFragRunner(dxPathConfig, dxIoFunctions, wfSourceCode)
        val (_, _, subBlocks, _) = Block.split(wf.innerGraph, wfSourceCode)
/*
        val results = fragRunner.evalExpressions(subBlocks(0).nodes,
                                                 Map.empty[String, WomValue])
 Utils.ignore(results)*/
        Utils.ignore(subBlocks)
    }

    it should "evaluate expressions that define variables" in {
        val path = pathFromBasename("draft2", "conditionals3.wdl")
        val wfSourceCode = Utils.readFileContent(path)

        val (dxPathConfig, dxIoFunctions) = setup()
        val (wf, fragRunner) = setupFragRunner(dxPathConfig, dxIoFunctions, wfSourceCode)
        val (_, _, subBlocks, _) = Block.split(wf.innerGraph, wfSourceCode)

        val results = fragRunner.evalExpressions(subBlocks(0).nodes,
                                                 Map.empty[String, WomValue])
        results.keys should be(Set("powers10", "i1", "i2", "i3"))
        results("i1") should be(WomOptionalValue(WomIntegerType, Some(WomInteger(1))))
        results("i2") should be(WomOptionalValue(WomIntegerType, None))
        results("i3") should be(WomOptionalValue(WomIntegerType, Some(WomInteger(100))))
        results("powers10") should be(
            WomArray(WomArrayType(WomOptionalType(WomIntegerType)),
                     Vector(WomOptionalValue(WomIntegerType, Some(WomInteger(1))),
                            WomOptionalValue(WomIntegerType, None),
                            WomOptionalValue(WomIntegerType, Some(WomInteger(100))))))
    }


    // find the call
    private def findCallByName(callName: String,
                               graph: Graph) : CallNode = {
        val allCalls = graph.allNodes.collect{
            case call: CallNode => call
        }
        val callNode = allCalls.find{
            case callNode: CallNode =>
                callNode.identifier.localName.value == callName
        }
        callNode match {
            case None => throw new Exception(s"call ${callName} not found")
            case Some(x : CallNode) => x
            case Some(other) => throw new Exception(s"call ${callName} found with wrong class ${other}")
        }
    }

    it should "evaluate call inputs properly" in {
        val path = pathFromBasename("draft2", "various_calls.wdl")
        val wfSourceCode = Utils.readFileContent(path)
        val (dxPathConfig, dxIoFunctions) = setup()
        val (wf, fragRunner) = setupFragRunner(dxPathConfig, dxIoFunctions, wfSourceCode)

        val call1 = findCallByName("MaybeInt", wf.innerGraph)
        val callInputs1: Map[String, WomValue] = fragRunner.evalCallInputs(call1,
                                                                           Map("i" -> WomInteger(1)))
        callInputs1 should be(Map("a" -> WomOptionalValue(WomIntegerType,
                                                          Some(WomInteger(1)))))

        val call2 = findCallByName("ManyArgs", wf.innerGraph)
        val callInputs2: Map[String, WomValue] = fragRunner.evalCallInputs(
            call2,
            Map("powers10"-> WomArray(WomArrayType(WomIntegerType),
                                      Vector(WomInteger(1), WomInteger(10)))
            ))

        callInputs2 should be(Map("a" -> WomString("hello"),
                                  "b" -> WomArray(WomArrayType(WomIntegerType),
                                                  Vector(WomInteger(1), WomInteger(10)))))
    }

    it should "evaluate call constant inputs" in {
        val path = pathFromBasename("nested", "two_levels.wdl")
        val wfSourceCode = Utils.readFileContent(path)
        val (dxPathConfig, dxIoFunctions) = setup()
        val (wf, fragRunner) = setupFragRunner(dxPathConfig, dxIoFunctions, wfSourceCode)

        val inc4Call = findCallByName("inc5", wf.innerGraph)

        val args = fragRunner.evalCallInputs(inc4Call, Map.empty)
        args shouldBe (Map("a" -> WomInteger(3)))
    }

    it should "expressions with structs" in {
        val path = pathFromBasename("frag_runner", "House.wdl")
        val wfSourceCode = Utils.readFileContent(path)

        val (dxPathConfig, dxIoFunctions) = setup()
        val (wf, fragRunner) = setupFragRunner(dxPathConfig, dxIoFunctions, wfSourceCode)
        val (_, _, subBlocks, _) = Block.split(wf.innerGraph, wfSourceCode)

        val results = fragRunner.evalExpressions(subBlocks(0).nodes,
                                                 Map.empty[String, WomValue])
        results.keys should be(Set("a", "b", "tot_height", "tot_num_floors",
                                   "streets", "cities", "tot"))
        results("tot") should be(WomObject(Map(
                                               "height" -> WomInteger(32),
                                               "num_floors" -> WomInteger(4),
                                               "street" -> WomString("Alda_Mary"),
                                               "city" -> WomString("Sunnyvale_Santa Clara")),
                                           WomCompositeType(Map("height" -> WomIntegerType,
                                                                "num_floors" -> WomIntegerType,
                                                                "street" -> WomStringType,
                                                                "city" -> WomStringType),
                                                            Some("House"))
                                 ))
    }

    it should "fill in missing optionals" in {
        val path = pathFromBasename("frag_runner", "missing_args.wdl")
        val wfSourceCode = Utils.readFileContent(path)

        val (dxPathConfig, dxIoFunctions) = setup()
        val (wf, fragRunner) = setupFragRunner(dxPathConfig, dxIoFunctions, wfSourceCode)
        val results: Map[String, JsValue] =
            fragRunner.apply(Vector(0), Map("y" -> WomInteger(5)), RunnerWfFragmentMode.Launch)
        results shouldBe (Map("retval" -> JsNumber(5)))
    }

    it should "evaluate expressions in correct order" taggedAs(EdgeTest) in {
        val path = pathFromBasename("frag_runner", "scatter_variable_not_found.wdl")
        val wfSourceCode = Utils.readFileContent(path)

        val (dxPathConfig, dxIoFunctions) = setup()
        val (wf, fragRunner) = setupFragRunner(dxPathConfig, dxIoFunctions, wfSourceCode)
        val results: Map[String, JsValue] =
            fragRunner.apply(Vector(0), Map.empty, RunnerWfFragmentMode.Launch)
        results.keys should contain("bam_lane1")
        results("bam_lane1") shouldBe (JsObject(
                                           "___" -> JsArray(JsString("1_ACGT_1.bam"), JsNull)))
    }
}
