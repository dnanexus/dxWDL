package dx.exec

import java.nio.file.{Files, Path, Paths}

import dx.api.{DxApi, DxInstanceType, InstanceTypeDB}
import dx.compiler.{WdlCodeGen, WdlRuntimeAttrs}
import dx.core.io.{DxFileAccessProtocol, DxFileDescCache, DxPathConfig}
import dx.core.languages.Language
import dx.core.languages.wdl.{Evaluator, ParseSource, WdlVarLinksConverter, Bundle => WdlBundle}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import spray.json._
import wdlTools.eval.WdlValues
import wdlTools.types.{TypedAbstractSyntax => TAT}
import wdlTools.util.{FileSourceResolver, FileUtils, Logger, SysUtils}

// This test module requires being logged in to the platform.
// It compiles WDL scripts without the runtime library.
// This tests the compiler Native mode, however, it creates
// dnanexus applets and workflows that are not runnable.
class TaskRunnerTest extends AnyFlatSpec with Matchers {
  private val logger = Logger.Quiet
  private val dxApi = DxApi(logger)
  private val unicornInstance =
    DxInstanceType("mem_ssd_unicorn", 100, 100, 4, 1.00f, Vector(("Ubuntu", "16.04")), gpu = false)
  private val instanceTypeDB = InstanceTypeDB(pricingAvailable = true, Vector(unicornInstance))

  // Note: if the file doesn't exist, this throws a null pointer exception
  private def pathFromBasename(basename: String): Path = {
    val p = getClass.getResource(s"/task_runner/${basename}").getPath
    Paths.get(p)
  }

  /*    private def pathFromDirAndBasename(dirname: String, basename: String) : Path = {
        val p = getClass.getResource(s"/${dirname}/${basename}").getPath
        Paths.get(p)
    }*/

  // Recursively go into a wdlValue, and add a base path to the file.
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
  private def addBaseDir(wdlValue: WdlValues.V): WdlValues.V = {
    wdlValue match {
      // primitive types, pass through
      case WdlValues.V_Boolean(_) | WdlValues.V_Int(_) | WdlValues.V_Float(_) |
          WdlValues.V_String(_) | WdlValues.V_Null =>
        wdlValue

      // single file
      case WdlValues.V_File(s) => WdlValues.V_File(pathFromBasename(s).toString)

      // Maps
      case WdlValues.V_Map(m: Map[WdlValues.V, WdlValues.V]) =>
        val m1 = m.map {
          case (k, v) =>
            val k1 = addBaseDir(k)
            val v1 = addBaseDir(v)
            k1 -> v1
        }
        WdlValues.V_Map(m1)

      case WdlValues.V_Pair(l, r) =>
        val left = addBaseDir(l)
        val right = addBaseDir(r)
        WdlValues.V_Pair(left, right)

      case WdlValues.V_Array(a: Seq[WdlValues.V]) =>
        val a1 = a.map { v =>
          addBaseDir(v)
        }
        WdlValues.V_Array(a1)

      case WdlValues.V_Optional(v) =>
        val v1 = addBaseDir(v)
        WdlValues.V_Optional(v1)

      case WdlValues.V_Object(m) =>
        val m2 = m.map {
          case (k, v) =>
            k -> addBaseDir(v)
        }
        WdlValues.V_Object(m2)

      case other =>
        throw new Exception(s"Unsupported wdl value ${other}")
    }
  }

  private def scanForTasks(tDoc: TAT.Document): Map[String, TAT.Task] = {
    tDoc.elements.collect {
      case t: TAT.Task => t.name -> t
    }.toMap
  }

  // Parse the WDL source code, and extract the single task that is supposed to be there.
  // Also return the source script itself, verbatim.
  private def runTask(wdlName: String): TaskRunner = {
    val wdlCode: Path = pathFromBasename(s"${wdlName}.wdl")

    // load the inputs
    val inputsOrg: Map[String, JsValue] =
      try {
        val inputsFile = pathFromBasename(s"${wdlName}_input.json")
        assert(Files.exists(inputsFile))
        FileUtils.readFileContent(inputsFile).parseJson.asJsObject.fields
      } catch {
        case _: Throwable =>
          Map.empty
      }

    // load the expected outputs
    val outputFieldsExpected: Option[Map[String, JsValue]] =
      try {
        val outputsFile = pathFromBasename(s"${wdlName}_output.json")
        assert(Files.exists(outputsFile))
        Some(FileUtils.readFileContent(outputsFile).parseJson.asJsObject.fields)
      } catch {
        case _: Throwable =>
          None
      }

    // Create a clean temp directory for the task to use
    val jobHomeDir: Path = Files.createTempDirectory("dxwdl_applet_test")
    FileUtils.deleteRecursive(jobHomeDir)
    FileUtils.createDirectories(jobHomeDir)
    val dxPathConfig = DxPathConfig.apply(jobHomeDir, streamAllFiles = false, logger)
    dxPathConfig.createCleanDirs()

    val (_, language, wdlBundle: WdlBundle, allSources, _) =
      ParseSource(dxApi).apply(wdlCode, Vector.empty)
    val task: TAT.Task = ParseSource(dxApi).getMainTask(wdlBundle)
    assert(allSources.size == 1)
    val sourceDict = scanForTasks(allSources.values.head)
    assert(sourceDict.size == 1)
    val taskSourceCode = sourceDict.values.head

    // Parse the inputs, convert to WDL values. Delay downloading files
    // from the platform, we may not need to access them.
    val dxProtocol = DxFileAccessProtocol(dxApi)
    val fileResolver = FileSourceResolver.create(userProtocols = Vector(dxProtocol))
    val wdlVarLinksConverter =
      WdlVarLinksConverter(dxApi, fileResolver, DxFileDescCache.empty, wdlBundle.typeAliases)
    val evaluator =
      Evaluator.make(dxPathConfig, fileResolver, Language.toWdlVersion(language))
    val jobInputOutput =
      JobInputOutput(dxPathConfig,
                     fileResolver,
                     DxFileDescCache.empty,
                     wdlVarLinksConverter,
                     dxApi,
                     evaluator)

    // Add the WDL version to the task source code, so the parser
    // will pick up the correct language dielect.
    val wdlCodeGen = WdlCodeGen(logger, wdlBundle.typeAliases, language)
    val taskDocument = wdlCodeGen.standAloneTask(taskSourceCode)
    val taskRunner = TaskRunner(
        task,
        taskDocument,
        wdlBundle.typeAliases,
        instanceTypeDB,
        dxPathConfig,
        fileResolver,
        wdlVarLinksConverter,
        jobInputOutput,
        Some(WdlRuntimeAttrs(Map.empty)),
        Some(false),
        dxApi,
        evaluator
    )
    val inputsRelPaths = taskRunner.jobInputOutput.loadInputs(JsObject(inputsOrg), task)
    val inputs = inputsRelPaths.map {
      case (inpDef, value) => (inpDef, addBaseDir(value))
    }

    // run the entire task

    // prolog
    val (localizedInputs, dxUrl2path) = taskRunner.prolog(inputs)

    // instantiate the command
    val env = taskRunner.instantiateCommand(localizedInputs)

    // execute the shell script in a child job
    val script: Path = dxPathConfig.script
    if (Files.exists(script)) {
      val _ = SysUtils.execCommand(script.toString, None)
    }

    // epilog
    val outputFields: Map[String, JsValue] = taskRunner.epilog(env, dxUrl2path)

    outputFieldsExpected match {
      case None      => ()
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
    runTask("cgrep")
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
    val _ = runTask("empty_command_section")
    //task.commandSectionEmpty should be(true)
  }

  it should "handle structs" taggedAs EdgeTest in {
    runTask("Person2")
  }

  it should "handle missing optional files" in {
    runTask("missing_optional_output_file")
  }

  it should "run a python script" in {
    runTask("python_heredoc")
  }
}
