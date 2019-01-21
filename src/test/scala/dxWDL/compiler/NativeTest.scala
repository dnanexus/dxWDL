package dxWDL.compiler

import dxWDL.Main
import dxWDL.util.DxPath
import java.nio.file.{Path, Paths}
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.tags.Slow

// This test module requires being logged in to the platform.
// It compiles WDL scripts without the runtime library.
// This tests the compiler Native mode, however, it creates
// dnanexus applets and workflows that are not runnable.
@Slow
class NativeTest extends FlatSpec with Matchers {
    private def pathFromBasename(basename: String) : Path = {
        val p = getClass.getResource(s"/compiler/${basename}").getPath
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
    lazy val compileFlags = List("-compileMode", "NativeWithoutRuntimeAsset",
                                 "-project", dxTestProject.getId,
                                 "-force",
                                 "-locked")

    it  should "Native compile a single WDL task" in {
        val path = pathFromBasename("add.wdl")
        val retval = Main.compile(path.toString :: compileFlags)
        retval shouldBe a [Main.SuccessfulTermination]
    }

    // linear workflow
    it  should "Native compile a linear WDL workflow without expressions" in {
        val path = pathFromBasename("wf_linear_no_expr.wdl")
        val retval = Main.compile(path.toString :: compileFlags)
        retval shouldBe a [Main.SuccessfulTermination]
    }

    it  should "Native compile a linear WDL workflow" in {
        val path = pathFromBasename("wf_linear.wdl")
        val retval = Main.compile(path.toString
//                                      :: "--verboseKey" :: "Native"
//                                      :: "--verboseKey" :: "GenerateIR"
                                      :: compileFlags)
        retval shouldBe a [Main.SuccessfulTermination]
    }

    it should "Native compile a workflow with a scatter without a call" in {
        val path = pathFromBasename("scatter_no_call.wdl")
        Main.compile(
            path.toString :: compileFlags
        ) shouldBe a [Main.SuccessfulTermination]
    }
}
