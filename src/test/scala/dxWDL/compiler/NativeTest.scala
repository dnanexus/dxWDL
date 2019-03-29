package dxWDL.compiler

import dxWDL.Main
import dxWDL.util.DxPath
import java.nio.file.{Path, Paths}
import org.scalatest.{FlatSpec, Matchers}

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
                                 "-force",
                                 "-locked")

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

    it should "Native compile a workflow with one level nesting" taggedAs(NativeTestXX, EdgeTest) in {
        val path = pathFromBasename("nested", "two_levels.wdl")
        Main.compile(
            path.toString :: "--force" :: cFlags
        ) shouldBe a [Main.SuccessfulTermination]
    }

    it should "handle various conditionals" taggedAs(EdgeTest) in {
        val path = pathFromBasename("draft2", "conditionals_base.wdl")
        Main.compile(
            path.toString
                :: "--verbose"
                :: "--verboseKey" :: "Native"
                :: "--verboseKey" :: "GenerateIR"
                :: cFlags
        ) shouldBe a [Main.SuccessfulTermination]
    }
}
