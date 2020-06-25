package dx.compiler

import java.nio.file.{Path, Paths}

import dx.api._
import dx.compiler.Main.SuccessIR
import dx.core.util.MainUtils.{Failure}
import dx.util.Logger
import org.scalatest.Inside._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

// These tests involve compilation -without- access to the platform.
//
class InputFileTest extends AnyFlatSpec with Matchers {
  private val DX_API = DxApi(Logger.Quiet)

  private def pathFromBasename(dirname: String, basename: String): Path = {
    val p = getClass.getResource(s"/${dirname}/${basename}").getPath
    Paths.get(p)
  }

  private val dxProject = {
    val p = DX_API.currentProject
    if (p == null)
      throw new Exception("Must be logged in to run this test")
    p
  }

  val cFlags = List("--compileMode", "ir", "-quiet", "--project", dxProject.id)

  // make sure we are logged in

  it should "handle one task and two inputs" in {
    val wdlCode = pathFromBasename("input_file", "add.wdl")
    val inputs = pathFromBasename("input_file", "add_inputs.json")
    Main.compile(
        List(wdlCode.toString, "-inputs", inputs.toString)
          ++ cFlags
    ) shouldBe a[SuccessIR]
  }

  it should "deal with a locked workflow" in {
    val wdlCode = pathFromBasename("input_file", "math.wdl")
    val inputs = pathFromBasename("input_file", "math_inputs.json")
    Main.compile(
        List(wdlCode.toString,
             "-inputs",
             inputs.toString,
             "--locked"
             //, "--verbose", "--verboseKey", "GenerateIR"
        ) ++ cFlags
    ) shouldBe a[SuccessIR]
  }

  it should "not compile for several applets without a workflow" in {
    val wdlCode = pathFromBasename("input_file", "several_tasks.wdl")
    val inputs = pathFromBasename("input_file", "several_tasks_inputs.json")
    val retval = Main.compile(
        List(wdlCode.toString, "-inputs", inputs.toString)
          ++ cFlags
    )
    inside(retval) {
      case Failure(_, Some(e)) =>
        e.getMessage should include("Cannot generate one input file for 2 tasks")
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
      case Failure(_, Some(e)) =>
        e.getMessage should include("Could not map all input fields")
    }
  }

  it should "build defaults into applet underneath workflow" in {
    val wdlCode = pathFromBasename("input_file", "population.wdl")
    val defaults = pathFromBasename("input_file", "population_inputs.json")
    val retval = Main.compile(
        List(wdlCode.toString, "-defaults", defaults.toString)
          ++ cFlags
    )
    retval shouldBe a[SuccessIR]
  }

  it should "handle inputs specified in the json file, but missing in the workflow" in {
    val wdlCode = pathFromBasename("input_file", "missing_args.wdl")
    val inputs = pathFromBasename("input_file", "missing_args_inputs.json")

    Main.compile(
        List(wdlCode.toString, "-inputs", inputs.toString)
          ++ cFlags
    ) shouldBe a[SuccessIR]

    // inputs as defaults
    Main.compile(
        List(wdlCode.toString, "-defaults", inputs.toString)
          ++ cFlags
    ) shouldBe a[SuccessIR]

    // Input to an applet.
    // Missing argument in a locked workflow should throw an exception.
    val retval = Main.compile(
        List(wdlCode.toString, "-inputs", inputs.toString, "--locked")
          ++ cFlags
    )
    inside(retval) {
      case Failure(_, Some(e)) =>
        e.getMessage should include("Could not map all input fields")
    }

    // Missing arguments are legal in an unlocked workflow
    Main.compile(
        List(wdlCode.toString, "-inputs", inputs.toString)
          ++ cFlags
    ) shouldBe a[SuccessIR]
  }

  it should "support struct inputs" in {
    val wdlCode = pathFromBasename("struct", "Person.wdl")
    val inputs = pathFromBasename("struct", "Person_input.json")

    val retval = Main.compile(
        List(wdlCode.toString, "-inputs", inputs.toString)
          ++ cFlags
    )
    retval shouldBe a[SuccessIR]
  }

  it should "support array of pairs" taggedAs EdgeTest in {
    val wdlCode = pathFromBasename("input_file", "echo_pairs.wdl")
    val inputs = pathFromBasename("input_file", "echo_pairs.json")

    val retval = Main.compile(
        List(wdlCode.toString, "-inputs", inputs.toString)
//        ++ List("--verbose", "--verboseKey", "GenerateIR")
          ++ cFlags
    )
    retval shouldBe a[SuccessIR]
  }

  it should "array of structs" in {
    val wdlCode = pathFromBasename("struct", "array_of_structs.wdl")
    val inputs = pathFromBasename("struct", "array_of_structs_input.json")

    val retval = Main.compile(
        List(wdlCode.toString, "-inputs", inputs.toString)
          ++ cFlags
    )
    retval shouldBe a[SuccessIR]
  }

  it should "override default values in input file" in {
    val wdlCode = pathFromBasename("input_file", "override.wdl")
    val inputs = pathFromBasename("input_file", "override_input.json")

    val retval = Main.compile(
        List(wdlCode.toString, "-inputs", inputs.toString)
          ++ cFlags
    )
    retval shouldBe a[SuccessIR]
  }

  it should "WDL map input" in {
    val wdlCode = pathFromBasename("input_file", "map_argument.wdl")
    val inputs = pathFromBasename("input_file", "map_argument_input.json")

    val retval = Main.compile(
        List(wdlCode.toString, "-inputs", inputs.toString)
          ++ cFlags
    )
    retval shouldBe a[SuccessIR]
  }

  it should "allow file as WDL map key" in {
    val wdlCode = pathFromBasename("input_file", "no_file_key.wdl")
    val inputs = pathFromBasename("input_file", "no_file_key_input.json")

    val retval = Main.compile(
        List(wdlCode.toString, "-inputs", inputs.toString)
          ++ cFlags
    )
    retval shouldBe a[SuccessIR]
  }

}
