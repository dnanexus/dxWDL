package dxWDL.runner

import cats.data.Validated.{Invalid, Valid}
import common.validation.ErrorOr.ErrorOr
import java.nio.file.{Path, Paths}
import org.scalatest.{FlatSpec, Matchers}
import wom.callable.{WorkflowDefinition}
import wom.executable.WomBundle
import wom.graph.expression._
//import wom.types._
import wom.values._
import wom.expression.WomExpression

import dxWDL.util.{Block, DxIoFunctions, DxPathConfig, ParseWomSourceFile, Utils}

// This test module requires being logged in to the platform.
// It compiles WDL scripts without the runtime library.
// This tests the compiler Native mode, however, it creates
// dnanexus applets and workflows that are not runnable.
class WfFragRunnerTest extends FlatSpec with Matchers {
    private val runtimeDebugLevel = 0

    // Create a clean directory in "/tmp" for the task to use
    lazy val dxPathConfig = {
        val jobHomeDir : Path = Paths.get("/tmp/dxwdl_applet_test")
        Utils.deleteRecursive(jobHomeDir.toFile)
        Utils.safeMkdir(jobHomeDir)
        val dxPathConfig = DxPathConfig.apply(jobHomeDir)
        dxPathConfig.createCleanDirs()
        dxPathConfig
    }
    lazy val dxIoFunctions = DxIoFunctions(dxPathConfig, runtimeDebugLevel)

    // Note: if the file doesn't exist, this throws a null pointer exception
    def pathFromBasename(basename: String) : Path = {
        val p = getClass.getResource(s"/frag_runner/${basename}").getPath
        Paths.get(p)
    }

    def evaluateWomExpression(expr: WomExpression, env: Map[String, WomValue]) : WomValue = {
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
        val (language, womBundle: WomBundle, allSources) = ParseWomSourceFile.apply(source)

        val wf: WorkflowDefinition = womBundle.primaryCallable match {
            case Some(wf: WorkflowDefinition) => wf
            case _ => throw new Exception("sanity")
        }
        val graph = wf.innerGraph
        val (inputNodes, subBlocks, outputNodes) = Block.splitIntoBlocks(graph)

        val block = subBlocks(1)

        val env : Map[String, WomValue] =
            Map("x" -> WomInteger(3),
                "y" -> WomInteger(5),
                "add.result" -> WomInteger(8))

        val eNodes : Vector[ExposedExpressionNode] = block.nodes.collect{
            case eNode: ExposedExpressionNode => eNode
        }
        val expr : WomExpression = eNodes(0).womExpression
        val value: WomValue = evaluateWomExpression(expr, env)
        value should be(WomInteger(9))
    }
}
