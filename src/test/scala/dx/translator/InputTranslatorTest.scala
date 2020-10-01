package dx.translator

import java.nio.file.{Path, Paths}

import dx.Assumptions.isLoggedIn
import dx.Tags.EdgeTest
import dx.api._
import dx.compiler.Main
import dx.compiler.Main.SuccessIR
import dx.core.CliUtils.Failure
import org.scalatest.Inside._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

// These tests involve compilation -without- access to the platform.
//
class InputTranslatorTest extends AnyFlatSpec with Matchers {
  assume(isLoggedIn)

  private def pathFromBasename(dirname: String, basename: String): Path = {
    val p = getClass.getResource(s"/${dirname}/${basename}").getPath
    Paths.get(p)
  }

  private val cFlags =
    List("--compileMode", "ir", "-quiet", "--project", DxApi.get.currentProject.id)

  // make sure we are logged in

  it should "handle one task and two inputs" in {
    val wdlCode = pathFromBasename("input_file", "add.wdl")
    val inputs = pathFromBasename("input_file", "add_inputs.json")
    val args = List(wdlCode.toString, "-inputs", inputs.toString) ++ cFlags
    Main.compile(args.toVector) shouldBe a[SuccessIR]
  }

  it should "deal with a locked workflow" in {
    val wdlCode = pathFromBasename("input_file", "math.wdl")
    val inputs = pathFromBasename("input_file", "math_inputs.json")
    val args = List(wdlCode.toString,
                    "-inputs",
                    inputs.toString,
                    "--locked"
                    //, "--verbose", "--verboseKey", "GenerateIR"
    ) ++ cFlags
    Main.compile(args.toVector) shouldBe a[SuccessIR]
  }

  it should "not compile for several applets without a workflow" in {
    val wdlCode = pathFromBasename("input_file", "several_tasks.wdl")
    val inputs = pathFromBasename("input_file", "several_tasks_inputs.json")
    val args = List(wdlCode.toString, "-inputs", inputs.toString) ++ cFlags
    val retval = Main.compile(args.toVector)
    inside(retval) {
      case Failure(_, Some(e)) =>
        e.getMessage should include("Cannot generate one input file for 2 tasks")
    }
  }

  it should "one input too many" in {
    val wdlCode = pathFromBasename("input_file", "math.wdl")
    val inputs = pathFromBasename("input_file", "math_inputs2.json")
    val args = List(wdlCode.toString, "-inputs", inputs.toString, "--locked") ++ cFlags
    val retval = Main.compile(args.toVector)
    inside(retval) {
      case Failure(_, Some(e)) =>
        e.getMessage should include("Could not map all input fields")
    }
  }

  it should "build defaults into applet underneath workflow" in {
    val wdlCode = pathFromBasename("input_file", "population.wdl")
    val defaults = pathFromBasename("input_file", "population_inputs.json")
    val args = List(wdlCode.toString, "-defaults", defaults.toString) ++ cFlags
    val retval = Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
  }

  it should "handle inputs specified in the json file, but missing in the workflow" in {
    val wdlCode = pathFromBasename("input_file", "missing_args.wdl")
    val inputs = pathFromBasename("input_file", "missing_args_inputs.json")
    val args = List(wdlCode.toString, "-inputs", inputs.toString) ++ cFlags
    Main.compile(args.toVector) shouldBe a[SuccessIR]

    // inputs as defaults
    val args2 = List(wdlCode.toString, "-defaults", inputs.toString) ++ cFlags
    Main.compile(args2.toVector) shouldBe a[SuccessIR]

    // Input to an applet.
    // Missing argument in a locked workflow should throw an exception.
    val args3 = List(wdlCode.toString, "-inputs", inputs.toString, "--locked") ++ cFlags
    val retval = Main.compile(args3.toVector)
    inside(retval) {
      case Failure(_, Some(e)) =>
        e.getMessage should include("Could not map all input fields")
    }

    // Missing arguments are legal in an unlocked workflow
    val args4 = List(wdlCode.toString, "-inputs", inputs.toString) ++ cFlags
    Main.compile(args4.toVector) shouldBe a[SuccessIR]
  }

  it should "support struct inputs" in {
    val wdlCode = pathFromBasename("struct", "Person.wdl")
    val inputs = pathFromBasename("struct", "Person_input.json")
    val args = List(wdlCode.toString, "-inputs", inputs.toString) ++ cFlags
    val retval = Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
  }

  it should "support array of pairs" taggedAs EdgeTest in {
    val wdlCode = pathFromBasename("input_file", "echo_pairs.wdl")
    val inputs = pathFromBasename("input_file", "echo_pairs_input.json")
    val args = List(wdlCode.toString, "-inputs", inputs.toString) ++ cFlags
    //        ++ List("--verbose", "--verboseKey", "GenerateIR")
    val retval = Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
  }

  it should "handle an array of structs" in {
    val wdlCode = pathFromBasename("struct", "array_of_structs.wdl")
    val inputs = pathFromBasename("struct", "array_of_structs_input.json")
    val args = List(wdlCode.toString, "-inputs", inputs.toString) ++ cFlags
    val retval = Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
  }

  it should "override default values in input file" in {
    val wdlCode = pathFromBasename("input_file", "override.wdl")
    val inputs = pathFromBasename("input_file", "override_input.json")
    val args = List(wdlCode.toString, "-inputs", inputs.toString) ++ cFlags
    val retval = Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
  }

  it should "WDL map input" in {
    val wdlCode = pathFromBasename("input_file", "map_argument.wdl")
    val inputs = pathFromBasename("input_file", "map_argument_input.json")
    val args = List(wdlCode.toString, "-inputs", inputs.toString) ++ cFlags
    val retval = Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
  }

  it should "allow file as WDL map key" in {
    val wdlCode = pathFromBasename("input_file", "no_file_key.wdl")
    val inputs = pathFromBasename("input_file", "no_file_key_input.json")
    val args = List(wdlCode.toString, "-inputs", inputs.toString) ++ cFlags
    val retval = Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
  }
}
