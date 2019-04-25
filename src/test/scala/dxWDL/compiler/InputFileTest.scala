package dxWDL.compiler

import dxWDL.{Main}
import java.nio.file.{Path, Paths}
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.Inside._

// These tests involve compilation -without- access to the platform.
//
class InputFileTest extends FlatSpec with Matchers {
    private def pathFromBasename(dirname: String, basename: String) : Path = {
        val p = getClass.getResource(s"/${dirname}/${basename}").getPath
        Paths.get(p)
    }

    val cFlags = List("--compileMode", "ir", "-quiet")

    it should "handle one task and two inputs" in {
        val wdlCode = pathFromBasename("input_file", "add.wdl")
        val inputs = pathFromBasename("input_file", "add_inputs.json")
        Main.compile(
            List(wdlCode.toString, "-inputs", inputs.toString)
                ++ cFlags
        ) shouldBe a [Main.SuccessfulTerminationIR]
    }


    it should "deal with a locked workflow" in {
        val wdlCode = pathFromBasename("input_file", "math.wdl")
        val inputs = pathFromBasename("input_file", "math_inputs.json")
        Main.compile(
            List(wdlCode.toString, "-inputs", inputs.toString, "--locked"
                 //, "--verbose", "--verboseKey", "GenerateIR"
            ) ++ cFlags
        ) shouldBe a [Main.SuccessfulTerminationIR]
    }


    it should "not compile for several applets without a workflow" in {
        val wdlCode = pathFromBasename("input_file", "several_tasks.wdl")
        val inputs = pathFromBasename("input_file", "several_tasks_inputs.json")
        val retval = Main.compile(
            List(wdlCode.toString, "-inputs", inputs.toString)
                ++ cFlags
        )
        inside(retval) {
            case Main.UnsuccessfulTermination(errMsg) =>
                errMsg should include ("Cannot generate one input file for 2 tasks")
        }
    }


    it should "one input too many" in {
        val wdlCode = pathFromBasename("input_file", "math.wdl")
        val inputs = pathFromBasename("input_file", "math_inputs2.json")
        val retval = Main.compile(
            List(wdlCode.toString, "-inputs", inputs.toString, "--locked")
                ++ cFlags
        )

        inside(retval) {
            case Main.UnsuccessfulTermination(errMsg) =>
                errMsg should include ("Could not map all default fields")
        }
    }

    it should "build defaults into applet underneath workflow" in {
        val wdlCode = pathFromBasename("input_file", "population.wdl")
        val defaults = pathFromBasename("input_file", "population_inputs.json")
        val retval = Main.compile(
            List(wdlCode.toString, "--compileMode", "ir",
                 "-defaults", defaults.toString)
        )
        retval shouldBe a [Main.SuccessfulTerminationIR]
    }

    ignore should "handle inputs specified in the json file, but missing in the workflow" in {
        val wdlCode = pathFromBasename("input_file", "missing_args.wdl")
        val inputs = pathFromBasename("input_file", "missing_args_inputs.json")

        Main.compile(
            List(wdlCode.toString, "--compileMode", "ir", "-quiet",
                 //"--verbose", "--verboseKey", "GenerateIR",
                 "-inputs", inputs.toString)
        ) shouldBe a [Main.SuccessfulTerminationIR]

        // inputs as defaults
        Main.compile(
            List(wdlCode.toString, "--compileMode", "ir", "-quiet",
                 "-defaults", inputs.toString)
        ) shouldBe a [Main.SuccessfulTerminationIR]

        // Input to an applet.
        // Missing argument in a locked workflow should throw an exception.
        val retval = Main.compile(
            List(wdlCode.toString, "--compileMode", "ir", "--locked",
                 "-quiet", "-inputs", inputs.toString)
        )
        inside(retval) {
            case Main.UnsuccessfulTermination(errMsg) =>
                errMsg should include ("input")
                errMsg should include ("to call <missing_args.Add> is unspecified")
        }

        // Missing arguments are legal in an unlocked workflow
/*        Main.compile(
            List(wdlCode.toString, "--compileMode", "ir",
                 "-quiet", "-inputs", inputs.toString)
        ) shouldBe a [Main.SuccessfulTerminationIR]*/
    }


    it should "support struct inputs" taggedAs(EdgeTest) in {
        val wdlCode = pathFromBasename("struct", "Person.wdl")
        val inputs = pathFromBasename("struct", "Person_input.json")

        val retval = Main.compile(
            List(wdlCode.toString, "--compileMode", "ir",
                 "-inputs", inputs.toString)
        )
        retval shouldBe a [Main.SuccessfulTerminationIR]
    }
}
