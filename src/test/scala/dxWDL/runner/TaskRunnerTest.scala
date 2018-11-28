package dxWDL.runner

import java.nio.file.{Files, Path, Paths}
import org.scalatest.{FlatSpec, Matchers}
import spray.json._
import wom.callable.{CallableTaskDefinition}
import wom.executable.WomBundle

import dxWDL.Main
import dxWDL.util.{DxPathConfig, InstanceTypeDB, ParseWomSourceFile, Utils}

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

        // load the inputs
        val inputsFile = pathFromBasename(s"${wdlName}_input.json")
        val inputsOrg : Map[String, JsValue] =
            if (Files.exists(inputsFile))
                Utils.readFileContent(inputsFile)
                    .parseJson
                    .asJsObject.fields
            else Map.empty

        // load the outputs
        val outputsFile = pathFromBasename(s"${wdlName}_output.json")
        val outputFieldsExpected : Option[Map[String, JsValue]] =
            if (Files.exists(outputsFile))
                Some(Utils.readFileContent(outputsFile)
                         .parseJson
                         .asJsObject.fields)
            else None

        // Create a clean directory in "/tmp" for the task to use
        val jobHomeDir : Path = Paths.get("/tmp/dxwdl_applet_test")
        Utils.deleteRecursive(jobHomeDir.toFile)
        Utils.safeMkdir(jobHomeDir)
        val dxPathConfig = DxPathConfig.apply(jobHomeDir)
        dxPathConfig.createCleanDirs()

        val (_, womBundle: WomBundle, allSources) = ParseWomSourceFile.apply(wdlCode)
        val task : CallableTaskDefinition = Main.getMainTask(womBundle)
        assert(allSources.size == 1)
        val sourceDict  = ParseWomSourceFile.scanForTasks(allSources.values.head)
        assert(sourceDict.size == 1)
        val taskSourceCode = sourceDict.values.head

        // Parse the inputs, convert to WOM values. Delay downloading files
        // from the platform, we may not need to access them.
        val taskRunner = TaskRunner(task, taskSourceCode, instanceTypeDB, dxPathConfig, 0)
        val inputs = taskRunner.jobInputOutput.loadInputs(JsObject(inputsOrg).prettyPrint, task)

        // run the entire task

        // 1. prolog
        val (env, dxUrl2path) = taskRunner.prolog(inputs)

        // 2. execute the shell script in a child job
        val script : Path = dxPathConfig.script
        if (Files.exists(script)) {
            val (stdout, stderr) = Utils.execCommand(script.toString, None)
        }

        // 3. epilog
        val outputFields: Map[String, JsValue] = taskRunner.epilog(env, dxUrl2path)

        outputFieldsExpected match {
            case None => ()
            case Some(exp) => outputFields should be(exp)
        }
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

    it should "run ps in a command section" in {
        runTask("ps")
    }

    it should "localize a file to a task" in {
        runTask("cgrep")
    }
}
