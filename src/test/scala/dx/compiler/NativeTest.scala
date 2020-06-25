package dx.compiler

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.{Path, Paths}

import dx.api._
import dx.compiler.Main.SuccessIR
import dx.core
import dx.core.languages.wdl.ParseSource
import dx.core.util.MainUtils.Success
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import spray.json._
import wdlTools.util.{Logger, Util}

import scala.io.Source

// This test module requires being logged in to the platform.
// It compiles WDL scripts without the runtime library.
// This tests the compiler Native mode, however, it creates
// dnanexus applets and workflows that are not runnable.

class NativeTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  private val logger = Logger.Quiet
  private val DX_API = DxApi(logger)

  private def pathFromBasename(dir: String, basename: String): Path = {
    val p = getClass.getResource(s"/${dir}/${basename}").getPath
    Paths.get(p)
  }

  val TEST_PROJECT = "dxWDL_playground"

  private lazy val dxTestProject =
    try {
      DX_API.resolveProject(TEST_PROJECT)
    } catch {
      case _: Exception =>
        throw new Exception(
            s"""|Could not find project ${TEST_PROJECT}, you probably need to be logged into
                |the platform""".stripMargin
        )
    }

  private lazy val username = System.getProperty("user.name")
  private lazy val unitTestsPath = s"unit_tests/${username}"
  private lazy val cFlags = List("-compileMode",
                                 "NativeWithoutRuntimeAsset",
                                 "-project",
                                 dxTestProject.getId,
                                 "-folder",
                                 "/" + unitTestsPath,
                                 "-force",
                                 "-locked",
                                 "-quiet")

  private lazy val cFlagsReorg = List("-compileMode",
                                      "NativeWithoutRuntimeAsset",
                                      "--project",
                                      dxTestProject.getId,
                                      "-quiet",
                                      "--folder",
                                      "/reorg_tests")

  override def beforeAll(): Unit = {
    // build the directory with the native applets

    dxTestProject.newFolder(s"/${unitTestsPath}/applets/", parents = true)
    // building necessary applets before starting the tests
    val native_applets = Vector("native_concat",
                                "native_diff",
                                "native_mk_list",
                                "native_sum",
                                "native_sum_012",
                                "functional_reorg_test")
    val topDir = Paths.get(System.getProperty("user.dir"))
    native_applets.foreach { app =>
      try {
        val (_, _) = Util.execCommand(
            s"dx build $topDir/test/applets/$app --destination ${TEST_PROJECT}:/${unitTestsPath}/applets/",
            quiet = true
        )
      } catch {
        case _: Throwable =>
      }
    }
  }

  private def getAppletId(path: String): String = {
    val folder = Paths.get(path).getParent.toAbsolutePath.toString
    val basename = Paths.get(path).getFileName.toString
    val results = DxFindDataObjects(DX_API, Some(10)).apply(
        Some(dxTestProject),
        Some(folder),
        recurse = false,
        klassRestriction = None,
        withProperties = Vector.empty,
        nameConstraints = Vector(basename),
        withInputOutputSpec = false,
        Vector.empty,
        Set.empty
    )
    results.size shouldBe 1
    val desc = results.values.head
    desc.id
  }

  private def createExtras(extrasContent: String): String = {

    val tmp_extras = File.createTempFile("reorg-", ".json")
    tmp_extras.deleteOnExit()

    val bw = new BufferedWriter(new FileWriter(tmp_extras))
    bw.write(extrasContent)
    bw.close()

    tmp_extras.toString
  }

  it should "Native compile a linear WDL workflow" taggedAs NativeTestXX in {
    val path = pathFromBasename("compiler", "wf_linear.wdl")
    val retval = Main.compile(
        path.toString
          :: cFlags
    )
    retval shouldBe a[Success]
  }

  it should "Native compile a workflow with a scatter without a call" taggedAs NativeTestXX in {
    val path = pathFromBasename("compiler", "scatter_no_call.wdl")
    Main.compile(
        path.toString :: cFlags
    ) shouldBe a[Success]
  }

  it should "Native compile a draft2 workflow" taggedAs NativeTestXX in {
    val path = pathFromBasename("draft2", "shapes.wdl")
    Main.compile(
        path.toString :: "--force" :: cFlags
    ) shouldBe a[Success]
  }

  it should "handle various conditionals" taggedAs NativeTestXX in {
    val path = pathFromBasename("draft2", "conditionals_base.wdl")
    Main.compile(
        path.toString
        /*                :: "--verbose"
                :: "--verboseKey" :: "Native"
                :: "--verboseKey" :: "GenerateIR"*/
          :: cFlags
    ) shouldBe a[Success]
  }

  it should "be able to build interfaces to native applets" taggedAs NativeTestXX in {
    val outputPath = "/tmp/dx_extern.wdl"
    Main.dxni(
        List("--force",
             "--quiet",
             "--folder",
             s"/${unitTestsPath}/applets",
             "--project",
             dxTestProject.getId,
             "--language",
             "wdl_draft2",
             "--output",
             outputPath)
    ) shouldBe a[Success]

    // check that the generated file contains the correct tasks
    val src = Source.fromFile(outputPath)
    val content =
      try {
        src.getLines.mkString("\n")
      } finally {
        src.close()
      }

    val (tasks, _, _) = ParseSource(logger).parseWdlTasks(content)

    tasks.keySet shouldBe Set(
        "native_sum",
        "native_sum_012",
        "functional_reorg_test",
        "native_mk_list",
        "native_diff",
        "native_concat"
    )
  }

  it should "be able to build an interface to a specific applet" taggedAs NativeTestXX in {
    val outputPath = "/tmp/dx_extern_one.wdl"
    Main.dxni(
        List("--force",
             "--quiet",
             "--path",
             s"/${unitTestsPath}/applets/native_sum",
             "--project",
             dxTestProject.getId,
             "--language",
             "wdl_1_0",
             "--output",
             outputPath)
    ) shouldBe a[Success]

    // check that the generated file contains the correct tasks
    val src = Source.fromFile(outputPath)
    val content =
      try {
        src.getLines.mkString("\n")
      } finally {
        src.close()
      }

    val (tasks, _, _) = ParseSource(logger).parseWdlTasks(content)

    tasks.keySet shouldBe Set("native_sum")
  }

  it should "build an interface to an applet specified by ID" taggedAs NativeTestXX in {
    val dxObj = DX_API.resolveDxPath(
        s"${DxPath.DX_URL_PREFIX}${dxTestProject.id}:/${unitTestsPath}/applets/native_sum"
    )
    dxObj shouldBe a[DxApplet]
    val applet = dxObj.asInstanceOf[DxApplet]

    val outputPath = "/tmp/dx_extern_one.wdl"
    Main.dxni(
        List("--force",
             "--quiet",
             "--path",
             applet.id,
             "--project",
             dxTestProject.getId,
             "--language",
             "wdl_1_0",
             "--output",
             outputPath)
    ) shouldBe a[Success]

    // check that the generated file contains the correct tasks
    val src = Source.fromFile(outputPath)
    val content =
      try {
        src.getLines.mkString("\n")
      } finally {
        src.close()
      }

    val (tasks, _, _) = ParseSource(logger).parseWdlTasks(content)

    tasks.keySet shouldBe Set("native_sum")
  }

  ignore should "be able to include pattern information in inputSpec" in {
    val path = pathFromBasename("compiler", "pattern_params.wdl")

    val appId = Main.compile(
        path.toString :: cFlags
    ) match {
      case Success(x) => x
      case _          => throw new Exception("sanity")

    }

    val dxApplet = DX_API.applet(appId)
    val inputSpec = dxApplet.describe(Set(Field.InputSpec))
    val (in_file, pattern) = inputSpec.inputSpec match {
      case Some(x) => (x(0), x(1))
      case other   => throw new Exception(s"Unexpected result ${other}")
    }
    pattern.help shouldBe Some("The pattern to use to search in_file")
    pattern.group shouldBe Some("Common")
    pattern.label shouldBe Some("Search pattern")
    in_file.patterns shouldBe Some(IOParameterPatternArray(Vector("*.txt", "*.tsv")))
    in_file.help shouldBe Some("The input file to be searched")
    in_file.group shouldBe Some("Common")
    in_file.label shouldBe Some("Input file")
    // TODO: fix this
    // out_file would be part of the outputSpec, but wom currently doesn't
    // support parameter_meta for output vars
    //out_file.pattern shouldBe Some(Vector("*.txt", "*.tsv"))
  }

  it should "be able to include pattern object information in inputSpec" in {
    val path = pathFromBasename("compiler", "pattern_obj_params.wdl")

    val appId = Main.compile(
        path.toString :: cFlags
    ) match {
      case Success(x) => x
      case _          => throw new Exception("sanity")

    }

    val dxApplet = DX_API.applet(appId)
    val inputSpec = dxApplet.describe(Set(Field.InputSpec))
    val (in_file, pattern) = inputSpec.inputSpec match {
      case Some(x) => (x(0), x(1))
      case other   => throw new Exception(s"Unexpected result ${other}")
    }
    pattern.help shouldBe Some("The pattern to use to search in_file")
    pattern.group shouldBe Some("Common")
    pattern.label shouldBe Some("Search pattern")
    in_file.patterns shouldBe Some(
        IOParameterPatternObject(Some(Vector("*.txt", "*.tsv")),
                                 Some("file"),
                                 Some(Vector("foo", "bar")))
    )
    in_file.help shouldBe Some("The input file to be searched")
    in_file.group shouldBe Some("Common")
    in_file.label shouldBe Some("Input file")
    // TODO: fix this
    // out_file would be part of the outputSpec, but wom currently doesn't
    // support parameter_meta for output vars
    //out_file.pattern shouldBe Some(Vector("*.txt", "*.tsv"))
  }

  it should "be able to include choices information in inputSpec" in {
    val path = pathFromBasename("compiler", "choice_values.wdl")

    val appId = Main.compile(
        path.toString :: cFlags
    ) match {
      case Success(x) => x
      case _          => throw new Exception("sanity")

    }

    val dxApplet = DX_API.applet(appId)
    val inputSpec = dxApplet.describe(Set(Field.InputSpec))
    val (in_file, pattern) = inputSpec.inputSpec match {
      case Some(x) => (x(0), x(1))
      case other   => throw new Exception(s"Unexpected result ${other}")
    }
    pattern.choices shouldBe Some(
        Vector(
            IOParameterChoiceString(value = "A"),
            IOParameterChoiceString(value = "B")
        )
    )
    in_file.choices shouldBe Some(
        Vector(
            IOParameterChoiceFile(
                name = None,
                value = DX_API.file("file-Fg5PgBQ0ffP7B8bg3xqB115G")
            ),
            IOParameterChoiceFile(
                name = None,
                value = DX_API.file("file-Fg5PgBj0ffPP0Jjv3zfv0yxq")
            )
        )
    )
  }

  it should "be able to include annotated choices information in inputSpec" in {
    val path = pathFromBasename("compiler", "choice_obj_values.wdl")

    val appId = Main.compile(
        path.toString :: cFlags
    ) match {
      case Success(x) => x
      case other      => throw new Exception(s"Unexpected result ${other}")
    }

    val dxApplet = DX_API.applet(appId)
    val inputSpec = dxApplet.describe(Set(Field.InputSpec))
    val (in_file, pattern) = inputSpec.inputSpec match {
      case Some(x) => (x(0), x(1))
      case other   => throw new Exception(s"Unexpected result ${other}")
    }
    pattern.choices shouldBe Some(
        Vector(
            IOParameterChoiceString(value = "A"),
            IOParameterChoiceString(value = "B")
        )
    )
    in_file.choices shouldBe Some(
        Vector(
            IOParameterChoiceFile(
                name = Some("file1"),
                value = DX_API.file("file-Fg5PgBQ0ffP7B8bg3xqB115G")
            ),
            IOParameterChoiceFile(
                name = Some("file2"),
                value = DX_API.file("file-Fg5PgBj0ffPP0Jjv3zfv0yxq")
            )
        )
    )
  }

  it should "be able to include suggestion information in inputSpec" in {
    val path = pathFromBasename("compiler", "suggestion_values.wdl")

    val appId = Main.compile(
        path.toString :: cFlags
    ) match {
      case Success(x) => x
      case _          => throw new Exception("sanity")

    }

    val dxApplet = DX_API.applet(appId)
    val inputSpec = dxApplet.describe(Set(Field.InputSpec))
    val (in_file, pattern) = inputSpec.inputSpec match {
      case Some(x) => (x(0), x(1))
      case other   => throw new Exception(s"Unexpected result ${other}")
    }
    pattern.suggestions shouldBe Some(
        Vector(
            IOParameterSuggestionString(value = "A"),
            IOParameterSuggestionString(value = "B")
        )
    )
    in_file.suggestions shouldBe Some(
        Vector(
            IOParameterSuggestionFile(
                name = None,
                value = Some(DX_API.file("file-Fg5PgBQ0ffP7B8bg3xqB115G")),
                project = None,
                path = None
            ),
            IOParameterSuggestionFile(
                name = None,
                value = Some(DX_API.file("file-Fg5PgBj0ffPP0Jjv3zfv0yxq")),
                project = None,
                path = None
            )
        )
    )
  }

  it should "be able to include annotated suggestion information in inputSpec" in {
    val path = pathFromBasename("compiler", "suggestion_obj_values.wdl")

    val appId = Main.compile(
        path.toString :: cFlags
    ) match {
      case Success(x) => x
      case other      => throw new Exception(s"Unexpected result ${other}")
    }

    val dxApplet = DX_API.applet(appId)
    val inputSpec = dxApplet.describe(Set(Field.InputSpec))
    val (in_file, pattern) = inputSpec.inputSpec match {
      case Some(x) => (x(0), x(1))
      case other   => throw new Exception(s"Unexpected result ${other}")
    }
    pattern.suggestions shouldBe Some(
        Vector(
            IOParameterSuggestionString(value = "A"),
            IOParameterSuggestionString(value = "B")
        )
    )
    in_file.suggestions shouldBe Some(
        Vector(
            IOParameterSuggestionFile(
                name = Some("file1"),
                value = Some(DX_API.file("file-Fg5PgBQ0ffP7B8bg3xqB115G")),
                project = None,
                path = None
            ),
            IOParameterSuggestionFile(
                name = Some("file2"),
                value = None,
                project = Some(DX_API.project("project-FGpfqjQ0ffPF1Q106JYP2j3v")),
                path = Some("/test_data/f2.txt.gz")
            )
        )
    )
  }

  it should "be able to include dx_type information in inputSpec" in {
    val path = pathFromBasename("compiler", "add_dx_type.wdl")

    val appId = Main.compile(
        path.toString :: cFlags
    ) match {
      case Success(x) => x
      case other      => throw new Exception(s"Unexpected result ${other}")
    }

    val dxApplet = DX_API.applet(appId)
    val inputSpec = dxApplet.describe(Set(Field.InputSpec))
    val (file_a, file_b) = inputSpec.inputSpec match {
      case Some(x) => (x(0), x(1))
      case other   => throw new Exception(s"Unexpected result ${other}")
    }
    file_a.dx_type shouldBe Some(IOParameterTypeConstraintString("fastq"))
    file_b.dx_type shouldBe Some(
        IOParameterTypeConstraintOper(
            ConstraintOper.AND,
            Vector(
                IOParameterTypeConstraintString("fastq"),
                IOParameterTypeConstraintOper(
                    ConstraintOper.OR,
                    Vector(
                        IOParameterTypeConstraintString("Read1"),
                        IOParameterTypeConstraintString("Read2")
                    )
                )
            )
        )
    )
  }

  it should "be able to include default information in inputSpec" in {
    val path = pathFromBasename("compiler", "add_default.wdl")

    val appId = Main.compile(
        path.toString :: cFlags
    ) match {
      case Success(x) => x
      case other      => throw new Exception(s"Unexpected result ${other}")
    }

    val dxApplet = DX_API.applet(appId)
    val inputSpec = dxApplet.describe(Set(Field.InputSpec))
    val (int_a, int_b) = inputSpec.inputSpec match {
      case Some(x) => (x(0), x(1))
      case other   => throw new Exception(s"Unexpected result ${other}")
    }
    int_a.default shouldBe Some(IOParameterDefaultNumber(1))
    int_b.default shouldBe Some(IOParameterDefaultNumber(2))
  }

  it should "be able to include help information in inputSpec" in {
    val path = pathFromBasename("compiler", "add_help.wdl")

    val appId = Main.compile(
        path.toString :: cFlags
    ) match {
      case Success(x) => x
      case _          => throw new Exception("sanity")

    }

    val dxApplet = DX_API.applet(appId)
    val inputSpec = dxApplet.describe(Set(Field.InputSpec))
    val (a, b, c, d, e) = inputSpec.inputSpec match {
      case Some(x) => (x(0), x(1), x(2), x(3), x(4))
      case other   => throw new Exception(s"Unexpected result ${other}")
    }
    a.help shouldBe Some("lefthand side")
    b.help shouldBe Some("righthand side")
    c.help shouldBe Some("Use this")
    d.help shouldBe Some("Use this")
    e.help shouldBe Some("Use this")
  }

  it should "be able to include group information in inputSpec" in {
    val path = pathFromBasename("compiler", "add_group.wdl")

    val appId = Main.compile(
        path.toString :: cFlags
    ) match {
      case Success(x) => x
      case _          => throw new Exception("sanity")

    }

    val dxApplet = DX_API.applet(appId)
    val inputSpec = dxApplet.describe(Set(Field.InputSpec))
    val (a, b) = inputSpec.inputSpec match {
      case Some(x) => (x(0), x(1))
      case other   => throw new Exception(s"Unexpected result ${other}")
    }
    a.group shouldBe Some("common")
    b.group shouldBe Some("obscure")
  }

  it should "be able to include label information in inputSpec" in {
    val path = pathFromBasename("compiler", "add_label.wdl")

    val appId = Main.compile(
        path.toString :: cFlags
    ) match {
      case Success(x) => x
      case _          => throw new Exception("sanity")

    }

    val dxApplet = DX_API.applet(appId)
    val inputSpec = dxApplet.describe(Set(Field.InputSpec))
    val (a, b) = inputSpec.inputSpec match {
      case Some(x) => (x(0), x(1))
      case other   => throw new Exception(s"Unexpected result ${other}")
    }
    a.label shouldBe Some("A positive integer")
    b.label shouldBe Some("A negative integer")
  }

  it should "be able to include information from task meta and extras" in {
    val expectedUpstreamProjects =
      """
        |[
        |  {
        |    "author":"Broad Institute",
        |    "license":"BSD-3-Clause",
        |    "licenseUrl":"https://github.com/broadinstitute/LICENSE.TXT",
        |    "name":"GATK4",
        |    "repoUrl":"https://github.com/broadinstitute/gatk",
        |    "version":"GATK-4.0.1.2"
        |    }
        |]
            """.stripMargin.parseJson

    val expectedWhatsNew =
      """## Changelog
        |### Version 1.1
        |* Added parameter --foo
        |* Added cowsay easter-egg
        |### Version 1.0
        |* Initial version""".stripMargin

    val path = pathFromBasename("compiler", "add_app_meta.wdl")
    val extraPath = pathFromBasename("compiler/extras", "extras_license.json")

    val appId = Main.compile(
        path.toString
        //:: "--verbose"
          :: "--extras" :: extraPath.toString :: cFlags
    ) match {
      case Success(x) => x
      case other      => throw new Exception(s"Unexpected result ${other}")
    }

    val dxApplet = DX_API.applet(appId)
    val desc = dxApplet.describe(
        Set(
            Field.Description,
            Field.Details,
            Field.DeveloperNotes,
            Field.Properties,
            Field.Summary,
            Field.Tags,
            Field.Title,
            Field.Types
        )
    )

    desc.description shouldBe Some(
        "Adds two int together. This app adds together two integers and returns the sum"
    )
    desc.details match {
      case Some(JsObject(fields)) =>
        fields.foreach {
          case ("contactEmail", JsString(value))    => value shouldBe "joe@dev.com"
          case ("upstreamVersion", JsString(value)) => value shouldBe "1.0"
          case ("upstreamAuthor", JsString(value))  => value shouldBe "Joe Developer"
          case ("upstreamUrl", JsString(value))     => value shouldBe "https://dev.com/joe"
          case ("upstreamLicenses", JsArray(array)) => array shouldBe Vector(JsString("MIT"))
          case ("upstreamProjects", array: JsArray) =>
            array shouldBe expectedUpstreamProjects
          case ("whatsNew", JsString(value))   => value shouldBe expectedWhatsNew
          case ("instanceTypeDB", JsString(_)) => () // ignore
          case ("runtimeAttrs", JsObject(_))   => () // ignore
          case ("womSourceCode", JsString(_))  => () // ignore
          case ("wdlSourceCode", JsString(_))  => () // ignore
          case other                           => throw new Exception(s"Unexpected result ${other}")
        }
      case other => throw new Exception(s"Unexpected result ${other}")
    }
    desc.developerNotes shouldBe Some("Check out my sick bash expression! Three dolla signs!!!")
    desc.properties match {
      case Some(m) =>
        (m -- Set(VERSION_PROP, CHECKSUM_PROP)) shouldBe Map("foo" -> "bar")
      case _ => throw new Exception("No properties")
    }
    desc.summary shouldBe Some("Adds two int together")
    desc.tags shouldBe Some(Vector("add", "ints", "dxWDL"))
    desc.title shouldBe Some("Add Ints")
    desc.types shouldBe Some(Vector("Adder"))
  }

  it should "be able to include runtime hints" in {
    val path = pathFromBasename("compiler", "add_runtime_hints.wdl")

    val appId = Main.compile(
        path.toString :: cFlags
    ) match {
      case Success(x) => x
      case other      => throw new Exception(s"Unexpected result ${other}")
    }

    val dxApplet = DX_API.applet(appId)
    val desc = dxApplet.describe(
        Set(
            Field.Access,
            Field.IgnoreReuse,
            Field.RunSpec
        )
    )

    desc.runSpec match {
      case Some(JsObject(fields)) =>
        fields("executionPolicy") shouldBe JsObject(
            Map(
                "restartOn" -> JsObject(
                    Map(
                        "*" -> JsNumber(1),
                        "UnresponsiveWorker" -> JsNumber(2),
                        "ExecutionError" -> JsNumber(2)
                    )
                ),
                "maxRestarts" -> JsNumber(5)
            )
        )
        fields("timeoutPolicy") shouldBe JsObject(
            Map(
                "*" -> JsObject(
                    Map(
                        "days" -> JsNumber(0),
                        "hours" -> JsNumber(12),
                        "minutes" -> JsNumber(30)
                    )
                )
            )
        )
      case _ => throw new Exception("Missing runSpec")
    }
    desc.access shouldBe Some(
        JsObject(
            Map(
                "network" -> JsArray(Vector(JsString("*"))),
                "developer" -> JsBoolean(true)
            )
        )
    )
    desc.ignoreReuse shouldBe Some(true)
  }

  it should "be able to include runtime hints and override extras global" in {
    val path = pathFromBasename("compiler", "add_runtime_hints.wdl")
    val extraPath = pathFromBasename("compiler/extras", "short_timeout.json")

    val appId = Main.compile(
        path.toString
        //:: "--verbose"
          :: "--extras" :: extraPath.toString :: cFlags
    ) match {
      case Success(x) => x
      case other      => throw new Exception(s"Unexpected result ${other}")
    }

    val dxApplet = DX_API.applet(appId)
    val desc = dxApplet.describe(
        Set(
            Field.RunSpec
        )
    )

    desc.runSpec match {
      case Some(JsObject(fields)) =>
        fields("timeoutPolicy") shouldBe JsObject(
            Map(
                "*" -> JsObject(
                    Map(
                        "days" -> JsNumber(0),
                        "hours" -> JsNumber(12),
                        "minutes" -> JsNumber(30)
                    )
                )
            )
        )
      case _ => throw new Exception("Missing runSpec")
    }
  }

  it should "be able to include runtime hints with extras per-task override" in {
    val path = pathFromBasename("compiler", "add_runtime_hints.wdl")
    val extraPath = pathFromBasename("compiler/extras", "task_specific_short_timeout.json")

    val appId = Main.compile(
        path.toString
        //:: "--verbose"
          :: "--extras" :: extraPath.toString :: cFlags
    ) match {
      case Success(x) => x
      case other      => throw new Exception(s"Unexpected result ${other}")
    }

    val dxApplet = DX_API.applet(appId)
    val desc = dxApplet.describe(
        Set(
            Field.RunSpec
        )
    )

    // Sometimes the API only returns the fields with non-zero values
    def fillOut(obj: JsValue): JsObject = {
      obj match {
        case JsObject(fields) =>
          val defaults = Map(
              "days" -> JsNumber(0),
              "hours" -> JsNumber(0),
              "minutes" -> JsNumber(0)
          )
          JsObject(fields.view.mapValues {
            case JsObject(inner) => JsObject(defaults ++ inner)
            case _               => throw new Exception("Expected JsObject")
          }.toMap)
        case _ => throw new Exception("Expected JsObject")
      }
    }

    desc.runSpec match {
      case Some(JsObject(fields)) =>
        fillOut(fields("timeoutPolicy")) shouldBe JsObject(
            Map(
                "*" -> JsObject(
                    Map(
                        "days" -> JsNumber(0),
                        "hours" -> JsNumber(3),
                        "minutes" -> JsNumber(0)
                    )
                )
            )
        )
      case _ => throw new Exception("Missing runSpec")
    }
  }

  it should "be able to include information from workflow meta" in {
    val path = pathFromBasename("compiler", "wf_meta.wdl")

    val wfId = Main.compile(
        path.toString :: cFlags
    ) match {
      case Success(x) => x
      case other      => throw new Exception(s"Unexpected result ${other}")
    }

    val dxWorkflow = DX_API.workflow(wfId)
    val desc = dxWorkflow.describe(
        Set(
            Field.Description,
            Field.Details,
            Field.Properties,
            Field.Summary,
            Field.Tags,
            Field.Title,
            Field.Types
        )
    )

    desc.description shouldBe Some("This is a workflow that defines some metadata")
    desc.details match {
      case Some(JsObject(fields)) =>
        fields.foreach {
          case ("whatsNew", JsString(value))               => value shouldBe "v1.0: First release"
          case ("womSourceCode", JsString(_))              => ()
          case ("wdlSourceCode", JsString(_))              => ()
          case ("delayWorkspaceDestruction", JsBoolean(_)) => ()
          case ("link_inc", JsObject(_))                   => ()
          case ("link_mul", JsObject(_))                   => ()
          case ("execTree", JsString(_))                   => ()
          case other                                       => throw new Exception(s"Unexpected result ${other}")
        }
      case other => throw new Exception(s"Unexpected result ${other}")
    }
    desc.properties match {
      case Some(m) =>
        (m -- Set(VERSION_PROP, CHECKSUM_PROP)) shouldBe Map("foo" -> "bar")
      case _ => throw new Exception("No properties")
    }
    desc.summary shouldBe Some("A workflow that defines some metadata")
    desc.tags shouldBe Some(Vector("foo", "bar", "dxWDL"))
    desc.title shouldBe Some("Workflow with metadata")
    desc.types shouldBe Some(Vector("calculator"))
  }

  it should "be able to include information from workflow parameter meta" in {
    val path = pathFromBasename("compiler", "wf_param_meta.wdl")

    val wfId = Main.compile(
        path.toString :: cFlags
    ) match {
      case Success(x) => x
      case other      => throw new Exception(s"Unexpected result ${other}")
    }

    val dxWorkflow = DX_API.workflow(wfId)
    val desc = dxWorkflow.describe(Set(Field.Inputs))
    val (x, y) = desc.inputs match {
      case Some(s) => (s(0), s(1))
      case other   => throw new Exception(s"Unexpected result ${other}")
    }
    x.label shouldBe Some("Left-hand side")
    x.default shouldBe Some(IOParameterDefaultNumber(3))
    y.label shouldBe Some("Right-hand side")
    y.default shouldBe Some(IOParameterDefaultNumber(5))
  }

  it should "deep nesting" taggedAs NativeTestXX in {
    val path = pathFromBasename("compiler", "environment_passing_deep_nesting.wdl")
    Main.compile(
        path.toString
        /*                :: "--verbose"
                :: "--verboseKey" :: "Native"
                :: "--verboseKey" :: "GenerateIR"*/
          :: cFlags
    ) shouldBe a[Success]
  }

  it should "make default task timeout 48 hours" taggedAs NativeTestXX in {
    val path = pathFromBasename("compiler", "add_timeout.wdl")
    val appId = Main.compile(
        path.toString :: "--force" :: cFlags
    ) match {
      case Success(x) => x
      case _          => throw new Exception("sanity")
    }

    // make sure the timeout is what it should be
    val (stdout, _) = Util.execCommand(s"dx describe ${dxTestProject.getId}:${appId} --json")

    val timeout = stdout.parseJson.asJsObject.fields.get("runSpec") match {
      case Some(JsObject(x)) =>
        x.get("timeoutPolicy") match {
          case None    => throw new Exception("No timeout policy set")
          case Some(s) => s
        }
      case other => throw new Exception(s"Unexpected result ${other}")
    }
    timeout shouldBe JsObject(
        "*" -> JsObject("days" -> JsNumber(2), "hours" -> JsNumber(0), "minutes" -> JsNumber(0))
    )
  }

  it should "timeout can be overriden from the extras file" taggedAs NativeTestXX in {
    val path = pathFromBasename("compiler", "add_timeout_override.wdl")
    val extraPath = pathFromBasename("compiler/extras", "short_timeout.json")
    val appId = Main.compile(
        path.toString
          :: "--extras" :: extraPath.toString :: cFlags
    ) match {
      case Success(x) => x
      case _          => throw new Exception("sanity")
    }

    // make sure the timeout is what it should be
    val (stdout, _) = Util.execCommand(s"dx describe ${dxTestProject.getId}:${appId} --json")

    val timeout = stdout.parseJson.asJsObject.fields.get("runSpec") match {
      case Some(JsObject(x)) =>
        x.get("timeoutPolicy") match {
          case None    => throw new Exception("No timeout policy set")
          case Some(s) => s
        }
      case other => throw new Exception(s"Unexpected result ${other}")
    }
    timeout shouldBe JsObject("*" -> JsObject("hours" -> JsNumber(3)))

  }

  it should "allow choosing GPU instances" taggedAs NativeTestXX in {
    val path = pathFromBasename("compiler", "GPU2.wdl")

    val appId = Main.compile(path.toString :: cFlags) match {
      case Success(x) => x
      case _          => throw new Exception("sanity")
    }

    // make sure the timeout is what it should be
    val (stdout, _) = Util.execCommand(s"dx describe ${dxTestProject.getId}:${appId} --json")
    val obj = stdout.parseJson.asJsObject
    val obj2 = obj.fields("runSpec").asJsObject
    val obj3 = obj2.fields("systemRequirements").asJsObject
    val obj4 = obj3.fields("main").asJsObject
    val instanceType = obj4.fields.get("instanceType") match {
      case Some(JsString(x)) => x
      case other             => throw new Exception(s"Unexpected result ${other}")
    }

    //System.out.println(s"instanceType = ${instanceType}")
    instanceType should include("_gpu")
  }

  it should "Compile a workflow with subworkflows on the platform with the reorg app" in {
    val path = pathFromBasename("subworkflows", basename = "trains_station.wdl")
    val appletId = getAppletId(s"/${unitTestsPath}/applets/functional_reorg_test")
    val extrasContent =
      s"""|{
          | "custom_reorg" : {
          |    "app_id" : "${appletId}",
          |    "conf" : null
          |  }
          |}
          |""".stripMargin

    val tmpFile = createExtras(extrasContent)
    // remove locked workflow flag
    val retval = Main.compile(
        path.toString :: "-extras" :: tmpFile :: cFlagsReorg
    )
    retval shouldBe a[Success]
    val wfId: String = retval match {
      case Success(id) => id
      case _           => throw new Exception("sanity")
    }

    val wf = DX_API.workflow(wfId)
    val wfDesc = wf.describe(Set(Field.Stages))
    val wfStages = wfDesc.stages.get

    // there should be 4 stages: 1) common 2) train_stations 3) outputs 4) reorg
    wfStages.size shouldBe 4
    val reorgStage = wfStages.last

    reorgStage.id shouldBe "stage-reorg"
    reorgStage.executable shouldBe appletId

    // There should be 3 inputs, the output from output stage and the custom reorg config file.
    val reorgInput: JsObject = reorgStage.input match {
      case JsObject(x) => JsObject(x)
      case _           => throw new Exception("sanity")
    }

    // no reorg conf input. only status.
    reorgInput.fields.size shouldBe 1
    reorgInput.fields.keys shouldBe Set(core.REORG_STATUS)
  }

  // ignore for now as the test will fail in staging
  it should "Compile a workflow with subworkflows on the platform with the reorg app with config file in the input" in {
    // This works in conjunction with "Compile a workflow with subworkflows on the platform with the reorg app".
    val path = pathFromBasename("subworkflows", basename = "trains_station.wdl")
    val appletId = getAppletId(s"/${unitTestsPath}/applets/functional_reorg_test")
    // upload random file
    val (uploadOut, _) = Util.execCommand(
        s"dx upload ${path.toString} --destination /reorg_tests --brief"
    )
    val fileId = uploadOut.trim
    val extrasContent =
      s"""|{
          | "custom_reorg" : {
          |    "app_id" : "${appletId}",
          |    "conf" : "dx://$fileId"
          |  }
          |}
          |""".stripMargin

    val tmpFile = createExtras(extrasContent)
    // remove locked workflow flag
    val retval = Main.compile(
        path.toString :: "-extras" :: tmpFile :: cFlagsReorg
    )

    retval shouldBe a[Success]
    val wfId: String = retval match {
      case Success(wfId) => wfId
      case _             => throw new Exception("sanity")
    }

    val wf = DX_API.workflow(wfId)
    val wfDesc = wf.describe(Set(Field.Stages))
    val wfStages = wfDesc.stages.get
    val reorgStage = wfStages.last

    // There should be 3 inputs, the output from output stage and the custom reorg config file.
    val reorgInput: JsObject = reorgStage.input match {
      case JsObject(x) => JsObject(x)
      case _           => throw new Exception("sanity")
    }
    // no reorg conf input. only status.
    reorgInput.fields.size shouldBe 2
    reorgInput.fields.keys shouldBe Set(core.REORG_STATUS, REORG_CONFIG)
  }

  it should "Checks subworkflow with custom reorg app do not contain reorg attribute" in {
    // This works in conjunction with "Compile a workflow with subworkflows on the platform with the reorg app".
    val path = pathFromBasename("subworkflows", basename = "trains_station.wdl")
    val appletId = getAppletId(s"/${unitTestsPath}/applets/functional_reorg_test")
    // upload random file
    val extrasContent =
      s"""|{
          | "custom_reorg" : {
          |    "app_id" : "${appletId}",
          |    "conf" : null
          |  }
          |}
          |""".stripMargin

    val tmpFile = createExtras(extrasContent)

    // remove compile mode
    val retval = Main.compile(
        path.toString :: "-extras" :: tmpFile :: "-compileMode" :: "IR" :: cFlagsReorg.drop(2)
    )
    retval shouldBe a[SuccessIR]

    val bundle = retval match {
      case SuccessIR(bundle, _) => bundle
      case _                    => throw new Exception("sanity")
    }

    // this is a subworkflow so there is no reorg_status___ added.
    val trainsOutputVector: IR.Callable = bundle.allCallables("trains")
    trainsOutputVector.outputVars.size shouldBe 1
  }

  it should "Set job-reuse flag" taggedAs NativeTestXX in {
    val path = pathFromBasename("compiler", "add_timeout.wdl")
    val extrasContent =
      """|{
         |  "ignoreReuse": true
         |}
         |""".stripMargin
    val extrasPath = createExtras(extrasContent)

    // compile the task while
    val retval = Main.compile(
        path.toString :: "--extras" :: extrasPath :: cFlags
    )
    retval shouldBe a[Success]

    val appletId = retval match {
      case Success(x) => x
      case _          => throw new Exception("sanity")
    }

    // make sure the job reuse flag is set
    val (stdout, _) =
      Util.execCommand(s"dx describe ${dxTestProject.getId}:${appletId} --json")
    val ignoreReuseFlag = stdout.parseJson.asJsObject.fields.get("ignoreReuse")
    ignoreReuseFlag shouldBe Some(JsBoolean(true))
  }

  it should "set job-reuse flag on workflow" taggedAs NativeTestXX in {
    val path = pathFromBasename("subworkflows", basename = "trains_station.wdl")
    val extrasContent =
      """|{
         |  "ignoreReuse": true
         |}
         |""".stripMargin
    val extrasPath = createExtras(extrasContent)

    // remove compile mode
    val retval = Main.compile(
        path.toString :: "-extras" :: extrasPath :: cFlags
    )
    retval shouldBe a[Success]

    val wfId = retval match {
      case Success(x) => x
      case _          => throw new Exception("sanity")
    }

    // make sure the job reuse flag is set
    val (stdout, _) =
      Util.execCommand(s"dx describe ${dxTestProject.getId}:${wfId} --json")
    val ignoreReuseFlag = stdout.parseJson.asJsObject.fields.get("ignoreReuse")
    ignoreReuseFlag shouldBe Some(JsArray(JsString("*")))
  }

  it should "set delayWorkspaceDestruction on applet" taggedAs NativeTestXX in {
    val path = pathFromBasename("compiler", "add_timeout.wdl")
    val extrasContent =
      """|{
         |  "delayWorkspaceDestruction": true
         |}
         |""".stripMargin
    val extrasPath = createExtras(extrasContent)

    val retval = Main.compile(
        path.toString :: "-extras" :: extrasPath :: "--force" :: cFlags
    )
    retval shouldBe a[Success]

    val appletId = retval match {
      case Success(x) => x
      case _          => throw new Exception("sanity")
    }

    // make sure the delayWorkspaceDestruction flag is set
    val (stdout, _) =
      Util.execCommand(s"dx describe ${dxTestProject.getId}:${appletId} --json")
    val details = stdout.parseJson.asJsObject.fields("details")
    val delayWD = details.asJsObject.fields.get("delayWorkspaceDestruction")
    delayWD shouldBe Some(JsTrue)
  }

  it should "set delayWorkspaceDestruction on workflow" taggedAs NativeTestXX in {
    val path = pathFromBasename("subworkflows", basename = "trains_station.wdl")
    val extrasContent =
      """|{
         |  "delayWorkspaceDestruction": true
         |}
         |""".stripMargin
    val extrasPath = createExtras(extrasContent)

    val retval = Main.compile(
        path.toString :: "-extras" :: extrasPath :: "--force" :: cFlags
    )
    retval shouldBe a[Success]

    val wfId = retval match {
      case Success(x) => x
      case _          => throw new Exception("sanity")
    }

    // make sure the flag is set on the resulting workflow
    val (stdout, _) =
      Util.execCommand(s"dx describe ${dxTestProject.getId}:${wfId} --json")
    val details = stdout.parseJson.asJsObject.fields("details")
    val delayWD = details.asJsObject.fields.get("delayWorkspaceDestruction")
    delayWD shouldBe Some(JsTrue)
  }

}
