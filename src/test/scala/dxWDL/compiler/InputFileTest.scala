package dxWDL.compiler

import java.nio.file.{Path, Paths}
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.Inside._

import dxWDL.{Main}
import dxWDL.dx.DxUtils

// These tests involve compilation -without- access to the platform.
//
class InputFileTest extends FlatSpec with Matchers {
    private def pathFromBasename(dirname: String, basename: String) : Path = {
        val p = getClass.getResource(s"/${dirname}/${basename}").getPath
        Paths.get(p)
    }

    val dxProject = DxUtils.dxEnv.getProjectContext()
    if (dxProject == null)
        throw new Exception("Must be logged in to run this test")

    val cFlags = List("--compileMode", "ir", "-quiet",
                      "--project", dxProject.getId)

    // make sure we are logged in

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
            List(wdlCode.toString, "-defaults", defaults.toString)
                ++ cFlags
        )
        retval shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "handle inputs specified in the json file, but missing in the workflow" taggedAs(EdgeTest) in {
        val wdlCode = pathFromBasename("input_file", "missing_args.wdl")
        val inputs = pathFromBasename("input_file", "missing_args_inputs.json")

        Main.compile(
            List(wdlCode.toString, "-inputs", inputs.toString)
                ++ cFlags
        ) shouldBe a [Main.SuccessfulTerminationIR]

        // inputs as defaults
        Main.compile(
            List(wdlCode.toString, "-defaults", inputs.toString)
                ++ cFlags
        ) shouldBe a [Main.SuccessfulTerminationIR]

        // Input to an applet.
        // Missing argument in a locked workflow should throw an exception.
        val retval = Main.compile(
            List(wdlCode.toString, "-inputs", inputs.toString, "--locked")
                ++ cFlags
        )
        inside(retval) {
            case Main.UnsuccessfulTermination(errMsg) =>
                errMsg should include ("input")
                errMsg should include ("to call <missing_args.Add> is unspecified")
        }

        // Missing arguments are legal in an unlocked workflow
        Main.compile(
            List(wdlCode.toString, "-inputs", inputs.toString)
                ++ cFlags
        ) shouldBe a [Main.SuccessfulTerminationIR]
    }


    it should "support struct inputs" in {
        val wdlCode = pathFromBasename("struct", "Person.wdl")
        val inputs = pathFromBasename("struct", "Person_input.json")

        val retval = Main.compile(
            List(wdlCode.toString, "-inputs", inputs.toString)
                ++ cFlags
        )
        retval shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "support array of pairs" in {
        val wdlCode = pathFromBasename("input_file", "echo_pairs.wdl")
        val inputs = pathFromBasename("input_file", "echo_pairs.json")

        val retval = Main.compile(
            List(wdlCode.toString, "-inputs", inputs.toString)
                ++ cFlags
        )
        retval shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "array of structs" in {
        val wdlCode = pathFromBasename("struct", "array_of_structs.wdl")
        val inputs = pathFromBasename("struct", "array_of_structs_input.json")

        val retval = Main.compile(
            List(wdlCode.toString, "-inputs", inputs.toString)
                ++ cFlags
        )
        retval shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "override default values in input file" in {
        val wdlCode = pathFromBasename("input_file", "override.wdl")
        val inputs = pathFromBasename("input_file", "override_input.json")

        val retval = Main.compile(
            List(wdlCode.toString, "-inputs", inputs.toString)
                ++ cFlags
        )
        retval shouldBe a [Main.SuccessfulTerminationIR]
    }
}
