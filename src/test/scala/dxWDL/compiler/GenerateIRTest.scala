package dxWDL.compiler

import dxWDL.Main
import java.nio.file.{Path, Paths}
import org.scalatest.{FlatSpec, Matchers}

import org.scalatest.Tag
object EdgeTest extends Tag("edge")

// These tests involve compilation -without- access to the platform.
//
class GenerateIRTest extends FlatSpec with Matchers {
    private def pathFromBasename(basename: String) : Path = {
        val p = getClass.getResource(s"/compiler/${basename}").getPath
        Paths.get(p)
    }

    // task compilation
    private val cFlags = List("--compileMode", "ir", "-quiet", "-fatalValidationWarnings")

    it should "IR compile a single WDL task" in {
        val path = pathFromBasename("add.wdl")
        Main.compile(path.toString :: cFlags) shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "IR compile a task with docker" in {
        val path = pathFromBasename("BroadGenomicsDocker.wdl")
        Main.compile(path.toString :: cFlags) shouldBe a [Main.SuccessfulTerminationIR]
    }

    // workflow compilation
    it should "IR compile a linear WDL workflow without expressions" in {
        val path = pathFromBasename("wf_linear_no_expr.wdl")
        Main.compile(path.toString :: cFlags) shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "IR compile a linear WDL workflow" in {
        val path = pathFromBasename("wf_linear.wdl")
        Main.compile(
            /*List(path.toString, "--compileMode", "ir",
                 "--verbose", "GenerateIR",
             "-fatalValidationWarnings")*/
            path.toString :: cFlags
        ) shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "IR compile a non trivial linear workflow with variable coercions" in {
        val path = pathFromBasename("cast.wdl")
        Main.compile(path.toString :: cFlags) shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "IR compile a workflow with two consecutive calls" in {
        val path = pathFromBasename("strings.wdl")
        Main.compile(path.toString :: cFlags) shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "IR compile a workflow with a scatter without a call" in {
        val path = pathFromBasename("scatter_no_call.wdl")
        Main.compile(
/*            List(path.toString, "--compileMode", "ir",
                 "--quiet",
 "--verboseKey", "GenerateIR")*/
            path.toString :: cFlags
        ) shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "IR compile optionals" taggedAs(EdgeTest) in {
        val path = pathFromBasename("optionals.wdl")
        Main.compile(
            /*List(path.toString, "--compileMode", "ir",
                 "--quiet",
                 "--verbose",
                 "--verboseKey", "GenerateIR")*/
            path.toString :: cFlags
        ) shouldBe a [Main.SuccessfulTerminationIR]
    }
}
