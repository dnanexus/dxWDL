package dxWDL.runner

import dxWDL.Main
import dxWDL.util.{InstanceTypeDB, ParseWomSourceFile}
import java.nio.file.{Path, Paths}
import org.scalatest.{FlatSpec, Matchers}
import spray.json._
import wom.callable.{CallableTaskDefinition}
import wom.executable.WomBundle

// This test module requires being logged in to the platform.
// It compiles WDL scripts without the runtime library.
// This tests the compiler Native mode, however, it creates
// dnanexus applets and workflows that are not runnable.
class TaskRunnerTest extends FlatSpec with Matchers {
    lazy val currentWorkDir:Path = Paths.get(System.getProperty("user.dir"))

    private val instanceTypeDB = InstanceTypeDB.genTestDB(true)

    private def pathFromBasename(basename: String) : Path = {
        currentWorkDir.resolve(s"src/test/resources/runner_tasks/${basename}")
    }


    // Parse the WDL source code, and extract the single task that is supposed to be there.
    // Also return the source script itself, verbatim.
    private def runTask(wdlCode: Path,
                        inputsOrg: Map[String, JsValue]) : Map[String, JsValue] = {
        val (_, womBundle: WomBundle, allSources) = ParseWomSourceFile.apply(wdlCode)
        val task : CallableTaskDefinition = Main.getMainTask(womBundle)
        assert(allSources.size == 1)
        val sourceDict  = ParseWomSourceFile.scanForTasks(allSources.values.head)
        assert(sourceDict.size == 1)
        val taskSourceCode = sourceDict.values.head

        // Parse the inputs, convert to WOM values. Delay downloading files
        // from the platform, we may not need to access them.
        val inputs = JobInputOutput.loadInputs(JsObject(inputsOrg).prettyPrint, task)

        val r = TaskRunner(task, taskSourceCode, instanceTypeDB, 1)
        val (env, dxUrl2path) = r.prolog(inputs)
        val outputFields: Map[String, JsValue] = r.epilog(env, dxUrl2path)

        outputFields
    }

    it should "execute a simple WDL task" in {
        val wdlCode : Path = pathFromBasename("add.wdl")
        val outputFields : Map[String, JsValue] =
            runTask(wdlCode, Map("a" -> JsNumber(1),
                                 "b" -> JsNumber(2)))
        outputFields should be (Map("result" -> JsNumber(3)))
    }

    it should "execute a WDL task with expressions" in {
        val wdlCode : Path = pathFromBasename("float_arith.wdl")
        val outputFields : Map[String, JsValue] =
            runTask(wdlCode, Map("x" -> JsNumber(1)))
        outputFields should be(Map("x_round" -> JsNumber(1),
                                   "y_floor" -> JsNumber(2),
                                   "z_ceil" -> JsNumber(1)))
    }
}
