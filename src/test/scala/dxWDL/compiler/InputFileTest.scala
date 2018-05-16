package dxWDL.compiler

import dxWDL.{Main}
import java.nio.file.{Path, Paths}
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.Inside._

// These tests involve compilation -without- access to the platform.
//
class InputFileTest extends FlatSpec with Matchers {
    lazy val currentWorkDir:Path = Paths.get(System.getProperty("user.dir"))

    private def pathFromBasename(basename: String) : Path = {
        currentWorkDir.resolve(s"src/test/resources/input_file/${basename}")
    }

    it should "one input" in {
        val wdlCode = pathFromBasename("math.wdl")
        val inputs = pathFromBasename("math_inputs.json")
        Main.compile(
            List(wdlCode.toString, "--compileMode", "ir", "-quiet",
                 "-inputs", inputs.toString)
        ) shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "one input too many" in {
        val wdlCode = pathFromBasename("math.wdl")
        val inputs = pathFromBasename("math_inputs2.json")
        val retval = Main.compile(
            List(wdlCode.toString, "--compileMode", "ir", "-quiet",
                 "-inputs", inputs.toString)
        )

        inside(retval) {
            case Main.UnsuccessfulTermination(errMsg) =>
                errMsg should include ("Failed to map all input fields")
        }
    }
}
