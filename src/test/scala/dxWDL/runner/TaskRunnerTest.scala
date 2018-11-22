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

    private def pathFromBasename(basename: String) : Path = {
        currentWorkDir.resolve(s"src/test/resources/${basename}")
    }

    it should "execute a simple WDL task" in {
        val wdlCode : Path = pathFromBasename("runner/add.wdl")

        val (_, womBundle: WomBundle, allSources) = ParseWomSourceFile.apply(wdlCode)
        val task : CallableTaskDefinition = Main.getMainTask(womBundle)
        assert(allSources.size == 1)
        val sourceDict  = ParseWomSourceFile.scanForTasks(allSources.values.head)
        assert(sourceDict.size == 1)
        val taskSourceCode = sourceDict.values.head

        // Parse the inputs, convert to WOM values. Delay downloading files
        // from the platform, we may not need to access them.
        val inputsOrg = JsObject(Map("a" -> JsNumber(1),
                                     "b" -> JsNumber(2)))
        val inputs = JobInputOutput.loadInputs(inputsOrg.prettyPrint, task)

        // Figure out the available instance types, and their prices,
        // by reading the file
        val instanceTypeDB = InstanceTypeDB.genTestDB(true)
        val r = TaskRunner(task, taskSourceCode, instanceTypeDB, 1)

        val (env, dxUrl2path) = r.prolog(inputs)
        val outputFields: Map[String, JsValue] = r.epilog(env, dxUrl2path)

        outputFields should be (Map("result" -> JsNumber(3)))
    }
}
