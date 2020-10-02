package dx.compiler

import java.io.File
import java.nio.file.{Path, Paths}

import dx.Assumptions.{isLoggedIn, toolkitCallable}
import dx.Tags.NativeTest
import dx.api._
import dx.compiler.Main.SuccessIR
import dx.core.Constants
import dx.core.ir.Callable
import dx.core.CliUtils.{Success, Termination}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import spray.json._
import wdlTools.util.{FileUtils, Logger, SysUtils}

// This test module requires being logged in to the platform.
// It compiles WDL scripts without the runtime library.
// This tests the compiler Native mode, however, it creates
// dnanexus applets and workflows that are not runnable.

class CompilerTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  assume(isLoggedIn)
  assume(toolkitCallable)
  private val logger = Logger.Quiet
  private val dxApi = DxApi(logger)

  private def pathFromBasename(dir: String, basename: String): Path = {
    val p = getClass.getResource(s"/${dir}/${basename}").getPath
    Paths.get(p)
  }

  val testProject = "dxWDL_playground"

  private lazy val dxTestProject =
    try {
      dxApi.resolveProject(testProject)
    } catch {
      case _: Exception =>
        throw new Exception(
            s"""|Could not find project ${testProject}, you probably need to be logged into
                |the platform""".stripMargin
        )
    }

  private lazy val username = System.getProperty("user.name")
  private lazy val unitTestsPath = s"unit_tests/${username}"
  private lazy val cFlagsBase: List[String] = List(
      "-project",
      dxTestProject.id,
      "-quiet",
      "-force"
  )
  private lazy val cFlags: List[String] = cFlagsBase ++ List("-compileMode",
                                                             "NativeWithoutRuntimeAsset",
                                                             "-folder",
                                                             s"/${unitTestsPath}",
                                                             "-locked")
  private lazy val cFlagsReorgIR: List[String] = cFlagsBase ++
    List("-compileMode", "IR", "-folder", "/reorg_tests")
  private lazy val cFlagsReorgCompile: List[String] = cFlagsBase ++
    List("-compileMode", "NativeWithoutRuntimeAsset", "-folder", "/reorg_tests")

//  val irArgs = path.toString :: "--extras" :: extraPath.toString :: (cFlagsBase ++ List(
//    "-compileMode",
//    "IR",
//    "-folder",
//    s"/${unitTestsPath}",
//    "-locked"
//  ))
//  val bundle = Main.compile(irArgs.toVector) match {
//    case SuccessIR(x, _) => x
//    case other           => throw new Exception(s"Unexpected result ${other}")
//  }
//  val task = bundle.primaryCallable match {
//    case Some(task: Application) => task
//    case _                       => throw new Exception("boo")
//  }
//  println(task.requirements)
  private val reorgAppletFolder = s"/${unitTestsPath}/reorg_applets/"
  private val reorgAppletPath = s"${reorgAppletFolder}/functional_reorg_test"

  override def beforeAll(): Unit = {
    // build the directory with the native applets
    dxTestProject.removeFolder(reorgAppletFolder, recurse = true)
    dxTestProject.newFolder(reorgAppletFolder, parents = true)
    // building necessary applets before starting the tests
    val nativeApplets = Vector("functional_reorg_test")
    val topDir = Paths.get(System.getProperty("user.dir"))
    nativeApplets.foreach { app =>
      try {
        SysUtils.execCommand(
            s"dx build $topDir/test/applets/$app --destination ${testProject}:${reorgAppletFolder}",
            logger = Logger.Quiet
        )
      } catch {
        case _: Throwable =>
      }
    }
  }

  private def getAppletId(path: String): String = {
    val folder = Paths.get(path).getParent.toAbsolutePath.toString
    val basename = Paths.get(path).getFileName.toString
    val results = DxFindDataObjects(dxApi, Some(10)).apply(
        Some(dxTestProject),
        Some(folder),
        recurse = false,
        classRestriction = None,
        withTags = Vector.empty,
        nameConstraints = Vector(basename),
        withInputOutputSpec = false,
        Vector.empty,
        Set.empty
    )
    results.size shouldBe 1
    val desc = results.values.head
    desc.id
  }

  private lazy val reorgAppletId = getAppletId(reorgAppletPath)

  private object WithExtras {
    def apply(extrasContent: String)(f: String => Termination): Termination = {
      val tmpExtras = File.createTempFile("reorg-", ".json")
      FileUtils.writeFileContent(tmpExtras.toPath, extrasContent)
      try {
        f(tmpExtras.toString)
      } finally {
        tmpExtras.delete()
      }
    }
  }

  it should "Native compile a linear WDL workflow" taggedAs NativeTest in {
    val path = pathFromBasename("compiler", "wf_linear.wdl")
    val args = path.toString :: cFlags
    val retval = Main.compile(args.toVector)
    retval shouldBe a[Success]
  }

  it should "Native compile a workflow with a scatter without a call" taggedAs NativeTest in {
    val path = pathFromBasename("compiler", "scatter_no_call.wdl")
    val args = path.toString :: cFlags
    Main.compile(args.toVector) shouldBe a[Success]
  }

  it should "Native compile a draft2 workflow" taggedAs NativeTest in {
    val path = pathFromBasename("draft2", "shapes.wdl")
    val args = path.toString :: cFlags
    Main.compile(args.toVector) shouldBe a[Success]
  }

  it should "handle various conditionals" taggedAs NativeTest in {
    val path = pathFromBasename("draft2", "conditionals_base.wdl")
    val args = path.toString :: cFlags
    /*                :: "--verbose"
            :: "--verboseKey" :: "Native"
            :: "--verboseKey" :: "GenerateIR"*/
    Main.compile(args.toVector) shouldBe a[Success]
  }

  ignore should "be able to include pattern information in inputSpec" in {
    val path = pathFromBasename("compiler", "pattern_params.wdl")
    val args = path.toString :: cFlags
    val appId = Main.compile(args.toVector) match {
      case Success(x) => x
      case _          => throw new Exception("unexpected")

    }

    val dxApplet = dxApi.applet(appId)
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
    val args = path.toString :: cFlags
    val appId = Main.compile(args.toVector) match {
      case Success(x) => x
      case _          => throw new Exception("unexpected")
    }

    val dxApplet = dxApi.applet(appId)
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
    val args = path.toString :: cFlags
    val appId = Main.compile(args.toVector) match {
      case Success(x) => x
      case _          => throw new Exception("unexpected")
    }

    val dxApplet = dxApi.applet(appId)
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
                value = dxApi.file("file-Fg5PgBQ0ffP7B8bg3xqB115G")
            ),
            IOParameterChoiceFile(
                name = None,
                value = dxApi.file("file-Fg5PgBj0ffPP0Jjv3zfv0yxq")
            )
        )
    )
  }

  it should "be able to include annotated choices information in inputSpec" in {
    val path = pathFromBasename("compiler", "choice_obj_values.wdl")
    val args = path.toString :: cFlags
    val appId = Main.compile(args.toVector) match {
      case Success(x) => x
      case other      => throw new Exception(s"Unexpected result ${other}")
    }

    val dxApplet = dxApi.applet(appId)
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
                value = dxApi.file("file-Fg5PgBQ0ffP7B8bg3xqB115G")
            ),
            IOParameterChoiceFile(
                name = Some("file2"),
                value = dxApi.file("file-Fg5PgBj0ffPP0Jjv3zfv0yxq")
            )
        )
    )
  }

  it should "be able to include suggestion information in inputSpec" in {
    val path = pathFromBasename("compiler", "suggestion_values.wdl")
    val args = path.toString :: cFlags
    val appId = Main.compile(args.toVector) match {
      case Success(x) => x
      case _          => throw new Exception("unexpected")
    }

    val dxApplet = dxApi.applet(appId)
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
                value = Some(dxApi.file("file-Fg5PgBQ0ffP7B8bg3xqB115G")),
                project = None,
                path = None
            ),
            IOParameterSuggestionFile(
                name = None,
                value = Some(dxApi.file("file-Fg5PgBj0ffPP0Jjv3zfv0yxq")),
                project = None,
                path = None
            )
        )
    )
  }

  it should "be able to include annotated suggestion information in inputSpec" in {
    val path = pathFromBasename("compiler", "suggestion_obj_values.wdl")
    val args = path.toString :: cFlags
    val appId = Main.compile(args.toVector) match {
      case Success(x) => x
      case other      => throw new Exception(s"Unexpected result ${other}")
    }

    val dxApplet = dxApi.applet(appId)
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
                value = Some(dxApi.file("file-Fg5PgBQ0ffP7B8bg3xqB115G")),
                project = None,
                path = None
            ),
            IOParameterSuggestionFile(
                name = Some("file2"),
                value = None,
                project = Some(dxApi.project("project-FGpfqjQ0ffPF1Q106JYP2j3v")),
                path = Some("/test_data/f2.txt.gz")
            )
        )
    )
  }

  it should "be able to include dx_type information in inputSpec" in {
    val path = pathFromBasename("compiler", "add_dx_type.wdl")
    val args = path.toString :: cFlags
    val appId = Main.compile(args.toVector) match {
      case Success(x) => x
      case other      => throw new Exception(s"Unexpected result ${other}")
    }

    val dxApplet = dxApi.applet(appId)
    val inputSpec = dxApplet.describe(Set(Field.InputSpec))
    val (file_a, file_b) = inputSpec.inputSpec match {
      case Some(x) => (x(0), x(1))
      case other   => throw new Exception(s"Unexpected result ${other}")
    }
    file_a.dx_type shouldBe Some(IOParameterTypeConstraintString("fastq"))
    file_b.dx_type shouldBe Some(
        IOParameterTypeConstraintOper(
            ConstraintOper.And,
            Vector(
                IOParameterTypeConstraintString("fastq"),
                IOParameterTypeConstraintOper(
                    ConstraintOper.Or,
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
    val args = path.toString :: cFlags
    val appId = Main.compile(args.toVector) match {
      case Success(x) => x
      case other      => throw new Exception(s"Unexpected result ${other}")
    }

    val dxApplet = dxApi.applet(appId)
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
    val args = path.toString :: cFlags
    val appId = Main.compile(args.toVector) match {
      case Success(x) => x
      case _          => throw new Exception("unexpected")
    }

    val dxApplet = dxApi.applet(appId)
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
    val args = path.toString :: cFlags
    val appId = Main.compile(args.toVector) match {
      case Success(x) => x
      case _          => throw new Exception("unexpected")
    }

    val dxApplet = dxApi.applet(appId)
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
    val args = path.toString :: cFlags
    val appId = Main.compile(args.toVector) match {
      case Success(x) => x
      case _          => throw new Exception("unexpected")
    }

    val dxApplet = dxApi.applet(appId)
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
    val args = path.toString :: "--extras" :: extraPath.toString :: cFlags
    //:: "--verbose"
    val appId = Main.compile(args.toVector) match {
      case Success(x) => x
      case other      => throw new Exception(s"Unexpected result ${other}")
    }

    val dxApplet = dxApi.applet(appId)
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
          case ("whatsNew", JsString(value))              => value shouldBe expectedWhatsNew
          case (Constants.InstanceTypeDb, JsString(_))    => () // ignore
          case (Constants.Language, JsString(_))          => () // ignore
          case (Constants.RuntimeAttributes, JsObject(_)) => () // ignore
          case (Constants.Version, JsString(_))           => () // ignore
          case (Constants.Checksum, JsString(_))          => () // ignore
          case (Constants.SourceCode, JsString(_))        => () // ignore
          // old values for sourceCode - can probalby delete these
          case ("womSourceCode", JsString(_)) => () // ignore
          case ("wdlSourceCode", JsString(_)) => () // ignore
          case other                          => throw new Exception(s"Unexpected result ${other}")
        }
      case other => throw new Exception(s"Unexpected result ${other}")
    }
    desc.developerNotes shouldBe Some("Check out my sick bash expression! Three dolla signs!!!")
    desc.properties match {
      case Some(m) => m shouldBe Map("foo" -> "bar")
      case _       => throw new Exception("No properties")
    }
    desc.summary shouldBe Some("Adds two int together")
    desc.tags shouldBe Some(Set("add", "ints", Constants.CompilerTag))
    desc.title shouldBe Some("Add Ints")
    desc.types shouldBe Some(Vector("Adder"))
  }

  it should "be able to include runtime hints" in {
    val path = pathFromBasename("compiler", "add_runtime_hints.wdl")
    val args = path.toString :: cFlags
    val appId = Main.compile(args.toVector) match {
      case Success(x) => x
      case other      => throw new Exception(s"Unexpected result ${other}")
    }

    val dxApplet = dxApi.applet(appId)
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
    val args = path.toString :: "--extras" :: extraPath.toString :: cFlags

    //:: "--verbose"
    val appId = Main.compile(args.toVector) match {
      case Success(x) => x
      case other      => throw new Exception(s"Unexpected result ${other}")
    }

    val dxApplet = dxApi.applet(appId)
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
    val args = path.toString :: "--extras" :: extraPath.toString :: cFlags
    //:: "--verbose"
    val appId = Main.compile(args.toVector) match {
      case Success(x) => x
      case other      => throw new Exception(s"Unexpected result ${other}")
    }

    val dxApplet = dxApi.applet(appId)
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
    val args = path.toString :: cFlags
    val wfId = Main.compile(args.toVector) match {
      case Success(x) => x
      case other      => throw new Exception(s"Unexpected result ${other}")
    }

    val dxWorkflow = dxApi.workflow(wfId)
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
          case ("delayWorkspaceDestruction", JsBoolean(_)) => ()
          case ("link_inc", JsObject(_))                   => ()
          case ("link_mul", JsObject(_))                   => ()
          case ("execTree", JsString(_))                   => ()
          case (Constants.Version, JsString(_))            => () // ignore
          case (Constants.Checksum, JsString(_))           => () // ignore
          case (Constants.SourceCode, JsString(_))         => () // ignore
          // old values for sourceCode - can probalby delete these
          case ("womSourceCode", JsString(_)) => ()
          case ("wdlSourceCode", JsString(_)) => ()
          case other                          => throw new Exception(s"Unexpected result ${other}")
        }
      case other => throw new Exception(s"Unexpected result ${other}")
    }
    desc.properties match {
      case Some(m) => m shouldBe Map("foo" -> "bar")
      case _       => throw new Exception("No properties")
    }
    desc.summary shouldBe Some("A workflow that defines some metadata")
    desc.tags shouldBe Some(Vector("foo", "bar", Constants.CompilerTag))
    desc.title shouldBe Some("Workflow with metadata")
    desc.types shouldBe Some(Vector("calculator"))
  }

  it should "be able to include information from workflow parameter meta" in {
    val path = pathFromBasename("compiler", "wf_param_meta.wdl")
    val args = path.toString :: cFlags
    val wfId = Main.compile(args.toVector) match {
      case Success(x) => x
      case other      => throw new Exception(s"Unexpected result ${other}")
    }

    val dxWorkflow = dxApi.workflow(wfId)
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

  it should "deep nesting" taggedAs NativeTest in {
    val path = pathFromBasename("compiler", "environment_passing_deep_nesting.wdl")
    val args = path.toString :: cFlags
    /*                :: "--verbose"
            :: "--verboseKey" :: "Native"
            :: "--verboseKey" :: "GenerateIR"*/
    Main.compile(args.toVector) shouldBe a[Success]
  }

  it should "make default task timeout 48 hours" taggedAs NativeTest in {
    val path = pathFromBasename("compiler", "add_timeout.wdl")
    val args = path.toString :: cFlags
    val appId = Main.compile(args.toVector) match {
      case Success(x) => x
      case _          => throw new Exception("unexpected")
    }

    // make sure the timeout is what it should be
    val (_, stdout, _) = SysUtils.execCommand(s"dx describe ${dxTestProject.id}:${appId} --json")

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

  it should "timeout can be overriden from the extras file" taggedAs NativeTest in {
    val path = pathFromBasename("compiler", "add_timeout_override.wdl")
    val extraPath = pathFromBasename("compiler/extras", "short_timeout.json")
    val args = path.toString :: "--extras" :: extraPath.toString :: cFlags
    val appId = Main.compile(args.toVector) match {
      case Success(x) => x
      case _          => throw new Exception("unexpected")
    }

    // make sure the timeout is what it should be
    val (_, stdout, _) = SysUtils.execCommand(s"dx describe ${dxTestProject.id}:${appId} --json")

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

  it should "allow choosing GPU instances" taggedAs NativeTest in {
    val path = pathFromBasename("compiler", "GPU2.wdl")
    val args = path.toString :: cFlags
    val appId = Main.compile(args.toVector) match {
      case Success(x) => x
      case _          => throw new Exception("unexpected")
    }

    // make sure the timeout is what it should be
    val (_, stdout, _) = SysUtils.execCommand(s"dx describe ${dxTestProject.id}:${appId} --json")
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
    val extrasContent =
      s"""|{
          | "custom_reorg" : {
          |    "app_id" : "${reorgAppletId}",
          |    "conf" : null
          |  }
          |}
          |""".stripMargin
    val retval = WithExtras(extrasContent) { extrasPath =>
      val args = path.toString :: "-extras" :: extrasPath :: cFlagsReorgCompile
      Main.compile(args.toVector)
    }
    retval shouldBe a[Success]
    val wfId: String = retval match {
      case Success(id) => id
      case _           => throw new Exception("unexpected")
    }

    val wf = dxApi.workflow(wfId)
    val wfDesc = wf.describe(Set(Field.Stages))
    val wfStages = wfDesc.stages.get

    // there should be 4 stages: 1) common 2) train_stations 3) outputs 4) reorg
    wfStages.size shouldBe 4
    val reorgStage = wfStages.last

    reorgStage.id shouldBe "stage-reorg"
    reorgStage.executable shouldBe reorgAppletId

    // There should be 3 inputs, the output from output stage and the custom reorg config file.
    val reorgInput: JsObject = reorgStage.input match {
      case JsObject(x) => JsObject(x)
      case _           => throw new Exception("unexpected")
    }

    // no reorg conf input. only status.
    reorgInput.fields.size shouldBe 1
    reorgInput.fields.keys shouldBe Set(Constants.ReorgStatus)
  }

  // ignore for now as the test will fail in staging
  it should "Compile a workflow with subworkflows on the platform with the reorg app with config file in the input" in {
    // This works in conjunction with "Compile a workflow with subworkflows on the platform with the reorg app".
    val path = pathFromBasename("subworkflows", basename = "trains_station.wdl")
    // upload random file
    val (_, uploadOut, _) = SysUtils.execCommand(
        s"dx upload ${path.toString} --destination /reorg_tests --brief"
    )
    val fileId = uploadOut.trim
    val extrasContent =
      s"""|{
          | "custom_reorg" : {
          |    "app_id" : "${reorgAppletId}",
          |    "conf" : "dx://$fileId"
          |  }
          |}
          |""".stripMargin
    val retval = WithExtras(extrasContent) { extrasPath =>
      val args = path.toString :: "-extras" :: extrasPath :: cFlagsReorgCompile
      Main.compile(args.toVector)
    }
    retval shouldBe a[Success]
    val wfId: String = retval match {
      case Success(wfId) => wfId
      case _             => throw new Exception("unexpected")
    }

    val wf = dxApi.workflow(wfId)
    val wfDesc = wf.describe(Set(Field.Stages))
    val wfStages = wfDesc.stages.get
    val reorgStage = wfStages.last

    // There should be 3 inputs, the output from output stage and the custom reorg config file.
    val reorgInput: JsObject = reorgStage.input match {
      case JsObject(x) => JsObject(x)
      case _           => throw new Exception("unexpected")
    }
    // no reorg conf input. only status.
    reorgInput.fields.size shouldBe 2
    reorgInput.fields.keys shouldBe Set(Constants.ReorgStatus, Constants.ReorgConfig)
  }

  it should "ensure subworkflow with custom reorg app does not contain reorg attribute" in {
    val path = pathFromBasename("subworkflows", basename = "trains_station.wdl")
    // upload random file
    val extrasContent =
      s"""|{
          | "custom_reorg" : {
          |    "app_id" : "${reorgAppletId}",
          |    "conf" : null
          |  }
          |}
          |""".stripMargin
    val retval = WithExtras(extrasContent) { extrasPath =>
      val args = path.toString :: "-extras" :: extrasPath :: cFlagsReorgIR
      Main.compile(args.toVector)
    }
    retval shouldBe a[SuccessIR]

    val bundle = retval match {
      case SuccessIR(bundle, _) => bundle
      case _                    => throw new Exception("unexpected")
    }

    // this is a subworkflow so there is no reorg_status___ added.
    val trainsOutputVector: Callable = bundle.allCallables("trains")
    trainsOutputVector.outputVars.size shouldBe 1
  }

  it should "Set job-reuse flag" taggedAs NativeTest in {
    val path = pathFromBasename("compiler", "add_timeout.wdl")
    val extrasContent =
      """|{
         |  "ignoreReuse": true
         |}
         |""".stripMargin
    val retval = WithExtras(extrasContent) { extrasPath =>
      val args = path.toString :: "--extras" :: extrasPath :: cFlags
      Main.compile(args.toVector)
    }
    retval shouldBe a[Success]

    val appletId = retval match {
      case Success(x) => x
      case _          => throw new Exception("unexpected")
    }

    // make sure the job reuse flag is set
    val (_, stdout, _) =
      SysUtils.execCommand(s"dx describe ${dxTestProject.id}:${appletId} --json")
    val ignoreReuseFlag = stdout.parseJson.asJsObject.fields.get("ignoreReuse")
    ignoreReuseFlag shouldBe Some(JsBoolean(true))
  }

  it should "set job-reuse flag on workflow" taggedAs NativeTest in {
    val path = pathFromBasename("subworkflows", basename = "trains_station.wdl")
    val extrasContent =
      """|{
         |  "ignoreReuse": true
         |}
         |""".stripMargin
    val retval = WithExtras(extrasContent) { extrasPath =>
      val args = path.toString :: "-extras" :: extrasPath :: cFlags
      Main.compile(args.toVector)
    }
    retval shouldBe a[Success]

    val wfId = retval match {
      case Success(x) => x
      case _          => throw new Exception("unexpected")
    }

    // make sure the job reuse flag is set
    val (_, stdout, _) =
      SysUtils.execCommand(s"dx describe ${dxTestProject.id}:${wfId} --json")
    val ignoreReuseFlag = stdout.parseJson.asJsObject.fields.get("ignoreReuse")
    ignoreReuseFlag shouldBe Some(JsArray(JsString("*")))
  }

  it should "set delayWorkspaceDestruction on applet" taggedAs NativeTest in {
    val path = pathFromBasename("compiler", "add_timeout.wdl")
    val extrasContent =
      """|{
         |  "delayWorkspaceDestruction": true
         |}
         |""".stripMargin
    val retval = WithExtras(extrasContent) { extrasPath =>
      val args = path.toString :: "-extras" :: extrasPath :: cFlags
      Main.compile(args.toVector)
    }
    retval shouldBe a[Success]

    val appletId = retval match {
      case Success(x) => x
      case _          => throw new Exception("unexpected")
    }

    // make sure the delayWorkspaceDestruction flag is set
    val (_, stdout, _) =
      SysUtils.execCommand(s"dx describe ${dxTestProject.id}:${appletId} --json")
    val details = stdout.parseJson.asJsObject.fields("details")
    val delayWD = details.asJsObject.fields.get("delayWorkspaceDestruction")
    delayWD shouldBe Some(JsTrue)
  }

  it should "set delayWorkspaceDestruction on workflow" taggedAs NativeTest in {
    val path = pathFromBasename("subworkflows", basename = "trains_station.wdl")
    val extrasContent =
      """|{
         |  "delayWorkspaceDestruction": true
         |}
         |""".stripMargin
    val retval = WithExtras(extrasContent) { extrasPath =>
      val args = path.toString :: "-extras" :: extrasPath :: cFlags
      Main.compile(args.toVector)
    }
    retval shouldBe a[Success]

    val wfId = retval match {
      case Success(x) => x
      case _          => throw new Exception("unexpected")
    }

    // make sure the flag is set on the resulting workflow
    val (_, stdout, _) =
      SysUtils.execCommand(s"dx describe ${dxTestProject.id}:${wfId} --json")
    val details = stdout.parseJson.asJsObject.fields("details")
    val delayWD = details.asJsObject.fields.get("delayWorkspaceDestruction")
    delayWD shouldBe Some(JsTrue)
  }
}
