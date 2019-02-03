package dxWDL.runner

import cats.data.Validated.{Invalid, Valid}
import common.validation.ErrorOr.ErrorOr
import java.nio.file.{Path, Paths}
import org.scalatest.{FlatSpec, Matchers}
import spray.json._
import wom.callable.{WorkflowDefinition}
import wom.executable.WomBundle
import wom.expression.WomExpression
import wom.graph.expression._
import wom.values._
import wom.types._

import dxWDL.util._

// This test module requires being logged in to the platform.
// It compiles WDL scripts without the runtime library.
// This tests the compiler Native mode, however, it creates
// dnanexus applets and workflows that are not runnable.
class WfFragRunnerTest extends FlatSpec with Matchers {
    private val runtimeDebugLevel = 2
    //private val verbose = runtimeDebugLevel >= 1
    private val instanceTypeDB = InstanceTypeDB.genTestDB(false)

    private def setup() : (DxPathConfig, DxIoFunctions) = {
        // Create a clean directory in "/tmp" for the task to use
        val jobHomeDir : Path = Paths.get("/tmp/dxwdl_applet_test")
        Utils.deleteRecursive(jobHomeDir.toFile)
        Utils.safeMkdir(jobHomeDir)
        val dxPathConfig = DxPathConfig.apply(jobHomeDir, runtimeDebugLevel >= 1)
        dxPathConfig.createCleanDirs()
        val dxIoFunctions = DxIoFunctions(dxPathConfig, runtimeDebugLevel)
        (dxPathConfig, dxIoFunctions)
    }


    private def setupFragRunner(dxPathConfig: DxPathConfig,
                                dxIoFunctions: DxIoFunctions,
                                wfSourceCode: String) : (WorkflowDefinition, WfFragRunner) = {
        val wf : WorkflowDefinition = ParseWomSourceFile.parseWdlWorkflow(wfSourceCode)
        val fragInputOutput = new WfFragInputOutput(dxIoFunctions, null /*dxProject*/, runtimeDebugLevel)
        val fragRunner = new WfFragRunner(wf, wfSourceCode,
                                          instanceTypeDB,
                                          Map.empty[String, ExecLinkInfo],
                                          dxPathConfig,
                                          dxIoFunctions,
                                          JsNull,
                                          fragInputOutput,
                                          runtimeDebugLevel)
        (wf, fragRunner)
    }

    // Note: if the file doesn't exist, this throws a null pointer exception
    def pathFromBasename(basename: String) : Path = {
        val p = getClass.getResource(s"/frag_runner/${basename}").getPath
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
        val source : Path = pathFromBasename("wf_linear.wdl")
        val (dxPathConfig, dxIoFunctions) = setup()

        val (language, womBundle: WomBundle, allSources, subBundles) = ParseWomSourceFile.apply(source)
        subBundles.size should be(0)
        val wfSource = allSources.values.head

        val wf: WorkflowDefinition = womBundle.primaryCallable match {
            case Some(wf: WorkflowDefinition) => wf
            case _ => throw new Exception("sanity")
        }
        val graph = wf.innerGraph
        val (inputNodes, subBlocks, outputNodes) = Block.split(graph, wfSource)

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
        val path = pathFromBasename("scatter_no_call.wdl")
        val wfSourceCode = Utils.readFileContent(path)

        val (dxPathConfig, dxIoFunctions) = setup()
        val (wf, fragRunner) = setupFragRunner(dxPathConfig, dxIoFunctions, wfSourceCode)
        val (_, subBlocks, _) = Block.split(wf.innerGraph, wfSourceCode)
        val block = subBlocks(0)

        val env = Map.empty[String, WomValue]
        val results : Map[String, WomValue] = fragRunner.evalExpressions(block.nodes, env)
        results should be(
            Map("full_name" -> WomArray(
                    WomArrayType(WomStringType),
                    Vector(WomString("Michael_Manhaim"),
                           WomString("Lukas_Manhaim"),
                           WomString("Martin_Manhaim"),
                           WomString("Shelly_Manhaim"),
                           WomString("Amy_Manhaim"))

                )
            ))
    }

    it should "evaluate a conditional without a call" in {
        val path = pathFromBasename("conditional_no_call.wdl")
        val wfSourceCode = Utils.readFileContent(path)

        val (dxPathConfig, dxIoFunctions) = setup()
        val (wf, fragRunner) = setupFragRunner(dxPathConfig, dxIoFunctions, wfSourceCode)
        val (_, subBlocks, _) = Block.split(wf.innerGraph, wfSourceCode)
        val block = subBlocks(0)

        val env = Map.empty[String, WomValue]
        val results : Map[String, WomValue] = fragRunner.evalExpressions(block.nodes, env)
        results should be(
            Map("cats" -> WomOptionalValue(
                    WomStringType,
                    Some(WomString("Mr. Baggins"))
                )))
    }

    it should "evaluate a nested conditional/scatter without a call" in {
        val path = pathFromBasename("nested_no_call.wdl")
        val wfSourceCode = Utils.readFileContent(path)

        val (dxPathConfig, dxIoFunctions) = setup()
        val (wf, fragRunner) = setupFragRunner(dxPathConfig, dxIoFunctions, wfSourceCode)
        val (_, subBlocks, _) = Block.split(wf.innerGraph, wfSourceCode)

        fragRunner.evalExpressions(subBlocks(0).nodes,
                                   Map.empty[String, WomValue]) should
        be (Map("z" -> WomOptionalValue(WomMaybeEmptyArrayType(WomIntegerType),None)))
    }
}
