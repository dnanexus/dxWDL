package dxWDL.runner

import dxWDL.Main
import dxWDL.util.{InstanceTypeDB, ParseWomSourceFile, Utils}
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
    private def runTask(wdlName: String) : Unit = {
        val wdlCode : Path = pathFromBasename(s"${wdlName}.wdl")
        val inputsOrg : Map[String, JsValue] =
            Utils.readFileContent(pathFromBasename(s"${wdlName}_input.json"))
                .parseJson
                .asJsObject.fields
        val outputFieldsExpected : Map[String, JsValue] =
            Utils.readFileContent(pathFromBasename(s"${wdlName}_output.json"))
                .parseJson
                .asJsObject.fields

        val (_, womBundle: WomBundle, allSources) = ParseWomSourceFile.apply(wdlCode)
        val task : CallableTaskDefinition = Main.getMainTask(womBundle)
        assert(allSources.size == 1)
        val sourceDict  = ParseWomSourceFile.scanForTasks(allSources.values.head)
        assert(sourceDict.size == 1)
        val taskSourceCode = sourceDict.values.head

        // Parse the inputs, convert to WOM values. Delay downloading files
        // from the platform, we may not need to access them.
        val inputs = JobInputOutput.loadInputs(JsObject(inputsOrg).prettyPrint, task)

        val r = TaskRunner(task, taskSourceCode, instanceTypeDB, 0)
        val (env, dxUrl2path) = r.prolog(inputs)
        val outputFields: Map[String, JsValue] = r.epilog(env, dxUrl2path)

        outputFields should be(outputFieldsExpected)
    }

    it should "execute a simple WDL task" in {
        runTask("add")
    }

    it should "execute a WDL task with expressions" in {
        runTask("float_arith")
    }

    it should "evaluate expressions in runtime section" in {
        runTask("expressions_runtime_section")
    }

    it should "evaluate expressions in runtime section II" in {
        runTask("expressions_runtime_section_2")
    }

    it should "evaluate a command section" in {
        runTask("sub")
    }

}
