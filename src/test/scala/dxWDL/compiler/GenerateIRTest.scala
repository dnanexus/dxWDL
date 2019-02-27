package dxWDL.compiler

import dxWDL.Main
import java.nio.file.{Path, Paths}
import org.scalatest.{FlatSpec, Matchers}

// These tests involve compilation -without- access to the platform.
//
class GenerateIRTest extends FlatSpec with Matchers {
    private def pathFromBasename(dir: String, basename: String) : Path = {
        val p = getClass.getResource(s"/${dir}/${basename}").getPath
        Paths.get(p)
    }

    // task compilation
    private val cFlags = List("--compileMode", "ir", "-quiet", "-fatalValidationWarnings",
                              "--locked")
    private val cFlagsUnlocked = List("--compileMode", "ir", "-quiet", "-fatalValidationWarnings")
    val dbgFlags = List("--compileMode", "ir",
                        "--verbose",
                        "--verboseKey", "GenerateIR")

    it should "IR compile a single WDL task" in {
        val path = pathFromBasename("compiler", "add.wdl")
        Main.compile(path.toString :: cFlags) shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "IR compile a task with docker" in {
        val path = pathFromBasename("compiler", "BroadGenomicsDocker.wdl")
        Main.compile(path.toString :: cFlags) shouldBe a [Main.SuccessfulTerminationIR]
    }

    // workflow compilation
    it should "IR compile a linear WDL workflow without expressions" in {
        val path = pathFromBasename("compiler", "wf_linear_no_expr.wdl")
        Main.compile(path.toString :: cFlags) shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "IR compile a linear WDL workflow" in {
        val path = pathFromBasename("compiler", "wf_linear.wdl")
        Main.compile(
            path.toString :: cFlags
        ) shouldBe a [Main.SuccessfulTerminationIR]
    }


    it should "IR compile unlocked workflow" taggedAs(EdgeTest) in {
        val path = pathFromBasename("compiler", "wf_linear.wdl")
        Main.compile(
            path.toString :: cFlagsUnlocked
        ) shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "IR compile a non trivial linear workflow with variable coercions" in {
        val path = pathFromBasename("compiler", "cast.wdl")
        Main.compile(path.toString :: cFlags) shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "IR compile a workflow with two consecutive calls" in {
        val path = pathFromBasename("compiler", "strings.wdl")
        Main.compile(path.toString :: cFlags) shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "IR compile a workflow with a scatter without a call" in {
        val path = pathFromBasename("compiler", "scatter_no_call.wdl")
        Main.compile(
            path.toString :: cFlags
        ) shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "IR compile optionals" in {
        val path = pathFromBasename("compiler", "optionals.wdl")
        Main.compile(
            path.toString :: cFlags
        ) shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "support imports" in {
        val path = pathFromBasename("compiler", "check_imports.wdl")
        Main.compile(
            path.toString :: cFlags
        ) shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "IR compile a draft2 workflow" in {
        val path = pathFromBasename("draft2", "shapes.wdl")
        val retval = Main.compile(
            path.toString :: cFlags
        )
        retval shouldBe a [Main.SuccessfulTerminationIR]
/*
        val primary = retval match {
            case Main.SuccessfulTerminationIR(ir) =>
                ir.primaryCallable.get
            case _ =>
                throw new Exception("sanity")
        }

        val wf = primary match {
            case wf: IR.Workflow => wf
            case _ => throw new Exception("sanity")
        }

        val stages = wf.stages.map{ stg =>
            s"   ${stg}"
        }.mkString("\n")
        System.out.println(s"""|name = ${wf.name}
                               |inputs = ${wf.inputs}
                               |outputs = ${wf.outputs}
                               |stages = \n${stages}
                               |locked = ${wf.locked}
                               |kind = ${wf.kind}
                               |
                               |""".stripMargin)*/
    }

    it should "expressions in an output block" in {
        val path = pathFromBasename("compiler", "expr_output_block.wdl")
        Main.compile(
            path.toString :: cFlags
        ) shouldBe a [Main.SuccessfulTerminationIR]
    }

    ignore should "compile scatters over maps --- doesn't work in cromwell v37" in {
        val path = pathFromBasename("compiler", "dict2.wdl")
        Main.compile(
            path.toString :: cFlags
        ) shouldBe a [Main.SuccessfulTerminationIR]
    }
}
