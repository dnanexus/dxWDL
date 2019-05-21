package dxWDL.compiler

import java.nio.file.{Path, Paths}
import org.scalatest.{FlatSpec, Matchers}
import scala.io.Source

import dxWDL.Main
import dxWDL.dx.DxPath
import dxWDL.util.ParseWomSourceFile

// This test module requires being logged in to the platform.
// It compiles WDL scripts without the runtime library.
// This tests the compiler Native mode, however, it creates
// dnanexus applets and workflows that are not runnable.

class NativeTest extends FlatSpec with Matchers {
    private def pathFromBasename(dir: String, basename: String) : Path = {
        val p = getClass.getResource(s"/${dir}/${basename}").getPath
        Paths.get(p)
    }

    val TEST_PROJECT = "dxWDL_playground"
    lazy val dxTestProject =
        try {
            DxPath.lookupProject(TEST_PROJECT)
        } catch {
            case e : Exception =>
                throw new Exception(s"""|Could not find project ${TEST_PROJECT}, you probably need to be logged into
                                        |the platform""".stripMargin)
        }
    lazy val cFlags = List("-compileMode", "NativeWithoutRuntimeAsset",
                           "-project", dxTestProject.getId,
                           "-folder", "/unit_tests",
                           "-force",
                           "-locked",
                           "-quiet")

    it should "Native compile a single WDL task" taggedAs(NativeTestXX) in {
        val path = pathFromBasename("compiler", "add.wdl")
        val retval = Main.compile(path.toString :: cFlags)
        retval shouldBe a [Main.SuccessfulTermination]
    }

    // linear workflow
    it  should "Native compile a linear WDL workflow without expressions" taggedAs(NativeTestXX) in {
        val path = pathFromBasename("compiler", "wf_linear_no_expr.wdl")
        val retval = Main.compile(path.toString :: cFlags)
        retval shouldBe a [Main.SuccessfulTermination]
    }

    it  should "Native compile a linear WDL workflow" taggedAs(NativeTestXX) in {
        val path = pathFromBasename("compiler", "wf_linear.wdl")
        val retval = Main.compile(path.toString
                                      :: cFlags)
        retval shouldBe a [Main.SuccessfulTermination]
    }

    it should "Native compile a workflow with a scatter without a call" taggedAs(NativeTestXX) in {
        val path = pathFromBasename("compiler", "scatter_no_call.wdl")
        Main.compile(
            path.toString :: cFlags
        ) shouldBe a [Main.SuccessfulTermination]
    }


    it should "Native compile a draft2 workflow" taggedAs(NativeTestXX) in {
        val path = pathFromBasename("draft2", "shapes.wdl")
        Main.compile(
            path.toString :: "--force" :: cFlags
        ) shouldBe a [Main.SuccessfulTermination]
    }

    it should "Native compile a workflow with one level nesting" taggedAs(NativeTestXX) in {
        val path = pathFromBasename("nested", "two_levels.wdl")
        Main.compile(
            path.toString :: "--force" :: cFlags
        ) shouldBe a [Main.SuccessfulTermination]
    }

    it should "handle various conditionals" taggedAs(NativeTestXX) in {
        val path = pathFromBasename("draft2", "conditionals_base.wdl")
        Main.compile(
            path.toString
/*                :: "--verbose"
                :: "--verboseKey" :: "Native"
                :: "--verboseKey" :: "GenerateIR"*/
                :: cFlags
        ) shouldBe a [Main.SuccessfulTermination]
    }

    it should "be able to build interfaces to native applets" taggedAs(NativeTestXX, EdgeTest) in {
        val outputPath = "/tmp/dx_extern.wdl"
        Main.dxni(
            List("--force", "--quiet",
                 "--folder", "/unit_tests/applets",
                 "--project", dxTestProject.getId,
                 "--language", "wdl_draft2",
                 "--output", outputPath)
        ) shouldBe a [Main.SuccessfulTermination]

        // check that the generated file contains the correct tasks
        val content = Source.fromFile(outputPath).getLines.mkString("\n")

        val tasks : Map[String, String] = ParseWomSourceFile.scanForTasks(content)
        tasks.keys shouldBe(Set("native_sum", "native_sum_012", "native_mk_list", "native_diff", "native_concat"))
    }
}
