package dxWDL.exec

import java.nio.file.{Files, Path, Paths}
import org.scalatest.{FlatSpec, Matchers}
import spray.json._
import wom.callable.{CallableTaskDefinition}
import wom.executable.WomBundle
import wom.types._
import wom.values._

import dxWDL.util.{DxIoFunctions, DxPathConfig, InstanceTypeDB, ParseWomSourceFile, Utils}

// This test module requires being logged in to the platform.
// It compiles WDL scripts without the runtime library.
// This tests the compiler Native mode, however, it creates
// dnanexus applets and workflows that are not runnable.
class TaskRunnerTest extends FlatSpec with Matchers {
    private val runtimeDebugLevel = 0
    private val instanceTypeDB = InstanceTypeDB.genTestDB(true)
    private val verbose = false

    // Note: if the file doesn't exist, this throws a null pointer exception
    private def pathFromBasename(basename: String) : Path = {
        val p = getClass.getResource(s"/task_runner/${basename}").getPath
        Paths.get(p)
    }

/*    private def pathFromDirAndBasename(dirname: String, basename: String) : Path = {
        val p = getClass.getResource(s"/${dirname}/${basename}").getPath
        Paths.get(p)
    }*/

    // Recursively go into a womValue, and add a base path to the file.
    // For example:
    //   foo.txt ---> /home/joe_heller/foo.txt
    //
    // This is used to convert relative paths to test files into absolute paths.
    // For example, convert:
    //  {
    //    "pattern" : "snow",
    //    "in_file" : "manuscript.txt"
    //  }
    // into:
    //  {
    //    "pattern" : "snow",
    //    "in_file" : "/home/joe_heller/dxWDL/src/test/resources/runner_tasks/manuscript.txt"
    // }
    //
    private def addBaseDir(womValue: WomValue) : WomValue = {
        womValue match {
            // primitive types, pass through
            case WomBoolean(_) | WomInteger(_) | WomFloat(_) | WomString(_) => womValue

            // single file
            case WomSingleFile(s) => WomSingleFile(pathFromBasename(s).toString)

            // Maps
            case (WomMap(t: WomMapType, m: Map[WomValue, WomValue])) =>
                val m1 = m.map{ case (k, v) =>
                    val k1 = addBaseDir(k)
                    val v1 = addBaseDir(v)
                    k1 -> v1
                }
                WomMap(t, m1)

            case (WomPair(l, r)) =>
                val left = addBaseDir(l)
                val right = addBaseDir(r)
                WomPair(left, right)

            case WomArray(t: WomArrayType, a: Seq[WomValue]) =>
                val a1 = a.map{ v => addBaseDir(v) }
                WomArray(t, a1)

            case WomOptionalValue(t,  None) =>
                WomOptionalValue(t, None)
            case WomOptionalValue(t, Some(v)) =>
                val v1 = addBaseDir(v)
                WomOptionalValue(t, Some(v1))

            case WomObject(m, t) =>
                val m2 = m.map{ case (k, v) =>
                    k -> addBaseDir(v)
                }.toMap
                WomObject(m2, t)

            case _ =>
                throw new Exception(s"Unsupported wom value ${womValue}")
        }
    }


    // Parse the WDL source code, and extract the single task that is supposed to be there.
    // Also return the source script itself, verbatim.
    private def runTask(wdlName: String) : TaskRunner = {
        val wdlCode : Path = pathFromBasename(s"${wdlName}.wdl")

        // load the inputs
        val inputsOrg : Map[String, JsValue] =
            try {
                val inputsFile = pathFromBasename(s"${wdlName}_input.json")
                assert(Files.exists(inputsFile))
                Utils.readFileContent(inputsFile)
                    .parseJson
                    .asJsObject.fields
            } catch {
                case _: Throwable =>
                    Map.empty
            }

        // load the expected outputs
        val outputFieldsExpected : Option[Map[String, JsValue]] =
            try {
                val outputsFile = pathFromBasename(s"${wdlName}_output.json")
                assert(Files.exists(outputsFile))
                Some(Utils.readFileContent(outputsFile)
                         .parseJson
                         .asJsObject.fields)
            } catch {
                case _: Throwable =>
                    None
            }


        // Create a clean directory in "/tmp" for the task to use
        val jobHomeDir : Path = Paths.get("/tmp/dxwdl_applet_test")
        Utils.deleteRecursive(jobHomeDir.toFile)
        Utils.safeMkdir(jobHomeDir)
        val dxPathConfig = DxPathConfig.apply(jobHomeDir, verbose)
        dxPathConfig.createCleanDirs()

        val (language, womBundle: WomBundle, allSources, _) = ParseWomSourceFile.apply(wdlCode)
        val task : CallableTaskDefinition = ParseWomSourceFile.getMainTask(womBundle)
        assert(allSources.size == 1)
        val sourceDict  = ParseWomSourceFile.scanForTasks(allSources.values.head)
        assert(sourceDict.size == 1)
        val taskSourceCode = sourceDict.values.head

        // Parse the inputs, convert to WOM values. Delay downloading files
        // from the platform, we may not need to access them.
        val dxIoFunctions = DxIoFunctions(dxPathConfig, runtimeDebugLevel)
        val jobInputOutput = new JobInputOutput(dxIoFunctions, runtimeDebugLevel, womBundle.typeAliases)
        val taskRunner = TaskRunner(task, taskSourceCode, womBundle.typeAliases,
                                    instanceTypeDB,
                                    dxPathConfig, dxIoFunctions, jobInputOutput, 0)
        val inputsRelPaths = taskRunner.jobInputOutput.loadInputs(JsObject(inputsOrg), task)
        val inputs = inputsRelPaths.map{
            case (inpDef, value) => (inpDef, addBaseDir(value))
        }.toMap

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

        // return the task structure
        taskRunner
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
        val taskRunner = runTask("cgrep")
        taskRunner.commandSectionEmpty should be (false)
    }

    it should "handle type coercion" in {
        runTask("cast")
    }

    it should "handle spaces in file paths" in {
        runTask("spaces_in_file_paths")
    }

    it should "read_tsv" in {
        runTask("read_tsv_x")
    }

    it should "write_tsv" in {
        runTask("write_tsv_x")
    }

    it should "optimize task with an empty command section" in {
        val task = runTask("empty_command_section")
        task.commandSectionEmpty should be(true)
    }

    it should "read a docker manifest file" in {
        val buf = """|[
                     |{"Config":"4b778ee055da936b387080ba034c05a8fad46d8e50ee24f27dcd0d5166c56819.json",
                     |"RepoTags":["ubuntu_18_04_minimal:latest"],
                     |"Layers":[
                     |  "1053541ae4c67d0daa87babb7fe26bf2f5a3b29d03f4af94e9c3cb96128116f5/layer.tar",
                     |  "fb1542f1963e61a22f9416077bf5f999753cbf363234bf8c9c5c1992d9a0b97d/layer.tar",
                     |  "2652f5844803bcf8615bec64abd20959c023d34644104245b905bb9b08667c8d/layer.tar",
                     |  "386aac21291d1f58297bc7951ce00b4ff7485414d6a8e146d9fedb73e0ebfa5b/layer.tar",
                     |  "10d19fb34e1db6a5abf4a3c138dc21f67ef94c272cf359349da18ffa973b7246/layer.tar",
                     |  "c791705472caccd6c011326648cc9748bd1465451cd1cd28a809b0a7f4e8b671/layer.tar",
                     |  "d6cc894526fdfac9112633719d63806117b44cc7302f2a7ed6599b1a32f7c43a/layer.tar",
                     |  "3fbb031ee57d2a8b4b6615e540f55f9af88e88cdbceeffdac7033ec5c8ee327d/layer.tar"
                     |  ]
                     |}
                     |]""".stripMargin.trim

        val repo = TaskRunnerUtils.readManifestGetDockerImageName(buf)
        repo should equal("ubuntu_18_04_minimal:latest")
    }

    it should "handle structs" taggedAs(EdgeTest) in {
        runTask("Person2")
    }
}
