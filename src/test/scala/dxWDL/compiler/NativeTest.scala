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
    lazy val currentWorkDir:Path = Paths.get(System.getProperty("user.dir"))

    private def pathFromBasename(basename: String) : Path = {
        currentWorkDir.resolve(s"src/test/resources/${basename}")
    }

    val TEST_PROJECT = "dxWDL_playground"
    val dxTestProject =
        try {
            DxPath.lookupProject(TEST_PROJECT)
        } catch {
            case e : Exception =>
                throw new Exception(s"""|Could not find project ${TEST_PROJECT}, you probably need to be logged into
                                        |the platform""".stripMargin)
        }

    it should "compile a single WDL task" in {
        val path = pathFromBasename("tasks/add.wdl")
        val retval = Main.compile(
            List(path.toString,
                 "-compileMode", "NativeWithoutRuntimeAsset",
                 "-project", dxTestProject.getId,
                 "-force")
        )
        retval shouldBe a [Main.SuccessfulTermination]
    }
}
