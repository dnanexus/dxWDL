package dxWDL.compiler

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.{Path, Paths}

import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Inside._

import scala.io.Source
import dxWDL.Main
import dxWDL.Main.SuccessfulTermination
import dxWDL.base.{Utils, Verbose}
import dxWDL.dx._
import dxWDL.util.ParseWomSourceFile
import spray.json._

// This test module requires being logged in to the platform.
// It compiles WDL scripts without the runtime library.
// This tests the compiler Native mode, however, it creates
// dnanexus applets and workflows that are not runnable.

class NativeTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  private def pathFromBasename(dir: String, basename: String): Path = {
    val p = getClass.getResource(s"/${dir}/${basename}").getPath
    Paths.get(p)
  }

  val TEST_PROJECT = "dxWDL_playground"

  lazy val dxTestProject =
    try {
      DxPath.resolveProject(TEST_PROJECT)
    } catch {
      case e: Exception =>
        throw new Exception(
            s"""|Could not find project ${TEST_PROJECT}, you probably need to be logged into
                |the platform""".stripMargin
        )
    }

  lazy val username = System.getProperty("user.name")
  lazy val unitTestsPath = s"unit_tests/${username}"
  lazy val cFlags = List("-compileMode",
                         "NativeWithoutRuntimeAsset",
                         "-project",
                         dxTestProject.getId,
                         "-folder",
                         "/" + unitTestsPath,
                         "-force",
                         "-locked",
                         "-quiet")

  lazy private val cFlagsReorg = List("-compileMode",
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
        val (stdout, stderr) = Utils.execCommand(
            s"dx build $topDir/test/applets/$app --destination ${TEST_PROJECT}:/${unitTestsPath}/applets/",
            quiet = true
        )
      } catch {
        case _: Throwable =>
      }
    }
  }

  private def getAppletId(path: String): String = {
    val folder = Paths.get(path).getParent().toAbsolutePath().toString()
    val basename = Paths.get(path).getFileName().toString()
    val verbose = Verbose(false, true, Set.empty)
    val results = DxFindDataObjects(Some(10), verbose).apply(dxTestProject,
                                                             Some(folder),
                                                             recurse = false,
                                                             klassRestriction = None,
                                                             withProperties = Vector.empty,
                                                             nameConstraints = Vector(basename),
                                                             withInputOutputSpec = false)
    results.size shouldBe (1)
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

  it should "Native compile a single WDL task" taggedAs (NativeTestXX) in {
    val path = pathFromBasename("compiler", "add.wdl")
    val retval = Main.compile(
        path.toString :: "--execTree" :: "json" :: cFlags
    )
    retval shouldBe a[Main.SuccessfulTerminationTree]
    inside(retval) {
      case Main.SuccessfulTerminationTree(pretty) =>
        pretty match {
          case Left(str) => false // should not be the pretty string version
          case Right(treeJs) => {
            treeJs.asJsObject.getFields("name", "kind") match {
              case Seq(JsString(name), JsString(kind)) =>
                name shouldBe ("add")
                kind shouldBe ("Task")
              case other =>
                throw new Exception(s"tree representation is wrong ${treeJs}")
            }
          }
        }
    }
  }

  // linear workflow
  it should "Native compile a linear WDL workflow without expressions" taggedAs (NativeTestXX) in {
    val path = pathFromBasename("compiler", "wf_linear_no_expr.wdl")
    val retval = Main.compile(path.toString :: "--execTree" :: "json" :: cFlags)
    retval shouldBe a[Main.SuccessfulTerminationTree]

    inside(retval) {
      case Main.SuccessfulTerminationTree(pretty) =>
        pretty match {
          case Left(str) => false // should not be the pretty string version
          case Right(treeJs) => {
            treeJs.asJsObject.getFields("name", "kind", "stages") match {
              case Seq(JsString(name), JsString(kind), JsArray(stages)) =>
                name shouldBe ("wf_linear_no_expr")
                kind shouldBe ("workflow")
                stages.size shouldBe (3)
              case other =>
                throw new Exception(s"tree representation is wrong ${treeJs}")
            }
          }
        }
    }
  }

  // linear workflow
  it should "Native compile a linear WDL workflow wtih execTree in details" taggedAs (NativeTestXX) in {
    val path = pathFromBasename("compiler", "wf_linear_no_expr.wdl")
    val retval = Main.compile(path.toString :: cFlags)
    retval shouldBe a[Main.SuccessfulTermination]

    val wf: DxWorkflow = retval match {
      case Main.SuccessfulTermination(id) => DxWorkflow(id, Some(dxTestProject))
      case _                              => throw new Exception("sanity")
    }

    val description = wf.describe(Set(Field.Details))
    val details: Map[String, JsValue] = description.details match {
      case Some(x: JsValue) => x.asJsObject.fields
      case _                => throw new Exception("Expect details to be set for workflow")
    }
    // the compiled wf should at least have womSourceCode and execTree
    details.contains("womSourceCode") shouldBe true
    details.contains("execTree") shouldBe true

    val execString = details("execTree") match {
      case JsString(x) => x
      case other =>
        throw new Exception(
            s"Expected execTree to be JsString got ${other} instead."
        )
    }

    val treeJs = Utils.base64DecodeAndGunzip(execString).parseJson

    treeJs.asJsObject.getFields("name", "kind", "stages") match {
      case Seq(JsString(name), JsString(kind), JsArray(stages)) =>
        name shouldBe ("wf_linear_no_expr")
        kind shouldBe ("workflow")
        stages.size shouldBe (3)
      case other =>
        throw new Exception(s"tree representation is wrong ${treeJs}")
    }
  }

  it should "Native compile a linear WDL workflow" taggedAs (NativeTestXX) in {
    val path = pathFromBasename("compiler", "wf_linear.wdl")
    val retval = Main.compile(
        path.toString
          :: cFlags
    )
    retval shouldBe a[Main.SuccessfulTermination]
  }

  it should "Native compile a workflow with a scatter without a call" taggedAs (NativeTestXX) in {
    val path = pathFromBasename("compiler", "scatter_no_call.wdl")
    Main.compile(
        path.toString :: cFlags
    ) shouldBe a[Main.SuccessfulTermination]
  }

  it should "Native compile a draft2 workflow" taggedAs (NativeTestXX) in {
    val path = pathFromBasename("draft2", "shapes.wdl")
    Main.compile(
        path.toString :: "--force" :: cFlags
    ) shouldBe a[Main.SuccessfulTermination]
  }

  it should "Native compile a workflow with one level nesting" taggedAs (NativeTestXX) in {
    val path = pathFromBasename("nested", "two_levels.wdl")
    val retval = Main.compile(
        path.toString :: "--force" :: "--execTree" :: "json" :: cFlags
    )
    retval shouldBe a[Main.SuccessfulTerminationTree]

    inside(retval) {
      case Main.SuccessfulTerminationTree(pretty) =>
        pretty match {
          case Left(str) => false // should not produce a pretty string
          case Right(treeJs) => {
            treeJs.asJsObject.getFields("name", "kind", "stages") match {
              case Seq(JsString(name), JsString(kind), JsArray(stages)) =>
                name shouldBe ("two_levels")
                kind shouldBe ("workflow")
                stages.size shouldBe (3)
              case other =>
                throw new Exception(s"tree representation is wrong ${treeJs}")
            }
          }
        }
    }
  }

  // Can't strip out the escape characters that make the strings colored
  // TODO: add a pretty print nocolor option?
  ignore should "Display pretty print of tree with deep nesting" taggedAs (NativeTestXX) in {
    val path = pathFromBasename("nested", "four_levels.wdl")
    val controlCode: (Char) => Boolean = (c: Char) => (c <= 32 || c == 127)
    val retval = Main.compile(
        path.toString :: "--force" :: "--execTree" :: "pretty" :: cFlags
    )
    retval shouldBe a[Main.SuccessfulTerminationTree]

    inside(retval) {
      case Main.SuccessfulTerminationTree(pretty) =>
        pretty match {
          case Left(str) =>
            str.filterNot(controlCode) shouldBe """Workflow: four_levels
                                                  |├───App Inputs: common
                                                  |├───App Fragment: if ((username == "a"))
                                                  |│   └───Workflow: four_levels_block_0
                                                  |│       ├───App Task: c1
                                                  |│       └───App Task: c2
                                                  |├───App Fragment: scatter (i in [1, 4, 9])
                                                  |│   └───App Fragment: four_levels_frag_4
                                                  |│       └───Workflow: four_levels_block_1_0
                                                  |│           ├───App Fragment: if ((j == "john"))
                                                  |│           │   └───App Task: concat
                                                  |│           └───App Fragment: if ((j == "clease"))
                                                  |└───App Outputs: outputs""".stripMargin
          case Right(treeJs) => false // should not go down this road
        }
    }
  }

  it should "handle various conditionals" taggedAs (NativeTestXX) in {
    val path = pathFromBasename("draft2", "conditionals_base.wdl")
    Main.compile(
        path.toString
        /*                :: "--verbose"
                :: "--verboseKey" :: "Native"
                :: "--verboseKey" :: "GenerateIR"*/
          :: cFlags
    ) shouldBe a[Main.SuccessfulTermination]
  }

  it should "be able to build interfaces to native applets" taggedAs (NativeTestXX, EdgeTest) in {
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
    ) shouldBe a[Main.SuccessfulTermination]

    // check that the generated file contains the correct tasks
    val content = Source.fromFile(outputPath).getLines.mkString("\n")

    val tasks: Map[String, String] =
      ParseWomSourceFile(false).scanForTasks(content)

    tasks.keys shouldBe (Set(
        "native_sum",
        "native_sum_012",
        "functional_reorg_test",
        "native_mk_list",
        "native_diff",
        "native_concat"
    ))
  }

  it should "be able to build an interface to a specific applet" taggedAs (NativeTestXX, EdgeTest) in {
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
    ) shouldBe a[Main.SuccessfulTermination]

    // check that the generated file contains the correct tasks
    val content = Source.fromFile(outputPath).getLines.mkString("\n")

    val tasks: Map[String, String] =
      ParseWomSourceFile(false).scanForTasks(content)

    tasks.keys shouldBe (Set("native_sum"))
  }

  it should "build an interface to an applet specified by ID" taggedAs (NativeTestXX, EdgeTest) in {
    val dxObj = DxPath.resolveDxPath(
        s"${Utils.DX_URL_PREFIX}${dxTestProject.id}:/${unitTestsPath}/applets/native_sum"
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
    ) shouldBe a[Main.SuccessfulTermination]

    // check that the generated file contains the correct tasks
    val content = Source.fromFile(outputPath).getLines.mkString("\n")

    val tasks: Map[String, String] =
      ParseWomSourceFile(false).scanForTasks(content)

    tasks.keys shouldBe (Set("native_sum"))
  }

  it should "be able to include pattern information in inputSpec" in {
    val path = pathFromBasename("compiler", "pattern_params.wdl")

    val appId = Main.compile(
        path.toString :: cFlags
    ) match {
      case SuccessfulTermination(x) => x
      case _                        => throw new Exception("sanity")

    }

    val dxApplet = DxApplet.getInstance(appId)
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
    // out_file would be part of the outputSpec, but wom currently doesn't
    // support parameter_meta for output vars
    //out_file.pattern shouldBe Some(Vector("*.txt", "*.tsv"))
  }

  it should "be able to include pattern object information in inputSpec" in {
    val path = pathFromBasename("compiler", "pattern_obj_params.wdl")

    val appId = Main.compile(
        path.toString :: cFlags
    ) match {
      case SuccessfulTermination(x) => x
      case _                        => throw new Exception("sanity")

    }

    val dxApplet = DxApplet.getInstance(appId)
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
    // out_file would be part of the outputSpec, but wom currently doesn't
    // support parameter_meta for output vars
    //out_file.pattern shouldBe Some(Vector("*.txt", "*.tsv"))
  }

  it should "be able to include choices information in inputSpec" in {
    val path = pathFromBasename("compiler", "choice_values.wdl")

    val appId = Main.compile(
        path.toString :: cFlags
    ) match {
      case SuccessfulTermination(x) => x
      case _                        => throw new Exception("sanity")

    }

    val dxApplet = DxApplet.getInstance(appId)
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
                value = DxFile.getInstance("file-Fg5PgBQ0ffP7B8bg3xqB115G")
            ),
            IOParameterChoiceFile(
                name = None,
                value = DxFile.getInstance("file-Fg5PgBj0ffPP0Jjv3zfv0yxq")
            )
        )
    )
  }

  it should "be able to include annotated choices information in inputSpec" in {
    val path = pathFromBasename("compiler", "choice_obj_values.wdl")

    val appId = Main.compile(
        path.toString :: cFlags
    ) match {
      case SuccessfulTermination(x) => x
      case other                    => throw new Exception(s"Unexpected result ${other}")
    }

    val dxApplet = DxApplet.getInstance(appId)
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
                value = DxFile.getInstance("file-Fg5PgBQ0ffP7B8bg3xqB115G")
            ),
            IOParameterChoiceFile(
                name = Some("file2"),
                value = DxFile.getInstance("file-Fg5PgBj0ffPP0Jjv3zfv0yxq")
            )
        )
    )
  }

  it should "be able to include suggestion information in inputSpec" in {
    val path = pathFromBasename("compiler", "suggestion_values.wdl")

    val appId = Main.compile(
        path.toString :: cFlags
    ) match {
      case SuccessfulTermination(x) => x
      case _                        => throw new Exception("sanity")

    }

    val dxApplet = DxApplet.getInstance(appId)
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
                value = Some(DxFile.getInstance("file-Fg5PgBQ0ffP7B8bg3xqB115G")),
                project = None,
                path = None
            ),
            IOParameterSuggestionFile(
                name = None,
                value = Some(DxFile.getInstance("file-Fg5PgBj0ffPP0Jjv3zfv0yxq")),
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
      case SuccessfulTermination(x) => x
      case other                    => throw new Exception(s"Unexpected result ${other}")
    }

    val dxApplet = DxApplet.getInstance(appId)
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
                value = Some(DxFile.getInstance("file-Fg5PgBQ0ffP7B8bg3xqB115G")),
                project = None,
                path = None
            ),
            IOParameterSuggestionFile(
                name = Some("file2"),
                value = None,
                project = Some(DxProject("project-FGpfqjQ0ffPF1Q106JYP2j3v")),
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
      case SuccessfulTermination(x) => x
      case other                    => throw new Exception(s"Unexpected result ${other}")
    }

    val dxApplet = DxApplet.getInstance(appId)
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
      case SuccessfulTermination(x) => x
      case other                    => throw new Exception(s"Unexpected result ${other}")
    }

    val dxApplet = DxApplet.getInstance(appId)
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
      case SuccessfulTermination(x) => x
      case _                        => throw new Exception("sanity")

    }

    val dxApplet = DxApplet.getInstance(appId)
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
      case SuccessfulTermination(x) => x
      case _                        => throw new Exception("sanity")

    }

    val dxApplet = DxApplet.getInstance(appId)
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
      case SuccessfulTermination(x) => x
      case _                        => throw new Exception("sanity")

    }

    val dxApplet = DxApplet.getInstance(appId)
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
      case SuccessfulTermination(x) => x
      case other                    => throw new Exception(s"Unexpected result ${other}")
    }

    val dxApplet = DxApplet.getInstance(appId)
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
          case ("whatsNew", JsString(value))       => value shouldBe expectedWhatsNew
          case ("instanceTypeDB", JsString(value)) => Unit // ignore
          case ("runtimeAttrs", JsObject(fields))  => Unit // ignore
          case ("womSourceCode", JsString(value))  => Unit // ignore
          case other                               => throw new Exception(s"Unexpected result ${other}")
        }
      case other => throw new Exception(s"Unexpected result ${other}")
    }
    desc.developerNotes shouldBe Some("Check out my sick bash expression! Three dolla signs!!!")
    desc.properties match {
      case Some(m) =>
        (m -- Set(Utils.VERSION_PROP, Utils.CHECKSUM_PROP)) shouldBe Map("foo" -> "bar")
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
      case SuccessfulTermination(x) => x
      case other                    => throw new Exception(s"Unexpected result ${other}")
    }

    val dxApplet = DxApplet.getInstance(appId)
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
      case SuccessfulTermination(x) => x
      case other                    => throw new Exception(s"Unexpected result ${other}")
    }

    val dxApplet = DxApplet.getInstance(appId)
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
      case SuccessfulTermination(x) => x
      case other                    => throw new Exception(s"Unexpected result ${other}")
    }

    val dxApplet = DxApplet.getInstance(appId)
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
          JsObject(fields.mapValues {
            case JsObject(inner) => JsObject(defaults ++ inner)
            case _               => throw new Exception("Expected JsObject")
          })
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
      case SuccessfulTermination(x) => x
      case other                    => throw new Exception(s"Unexpected result ${other}")
    }

    val dxWorkflow = DxWorkflow.getInstance(wfId)
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
          case ("whatsNew", JsString(value))                   => value shouldBe "v1.0: First release"
          case ("womSourceCode", JsString(value))              => Unit // ignore
          case ("delayWorkspaceDestruction", JsBoolean(value)) => Unit // ignore
          case ("link_inc", JsObject(fields))                  => Unit // ignore
          case ("link_mul", JsObject(fields))                  => Unit // ignore
          case other                                           => throw new Exception(s"Unexpected result ${other}")
        }
      case other => throw new Exception(s"Unexpected result ${other}")
    }
    desc.properties match {
      case Some(m) =>
        (m -- Set(Utils.VERSION_PROP, Utils.CHECKSUM_PROP)) shouldBe Map("foo" -> "bar")
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
      case SuccessfulTermination(x) => x
      case other                    => throw new Exception(s"Unexpected result ${other}")
    }

    val dxWorkflow = DxWorkflow.getInstance(wfId)
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

  it should "deep nesting" taggedAs (NativeTestXX) in {
    val path = pathFromBasename("compiler", "environment_passing_deep_nesting.wdl")
    Main.compile(
        path.toString
        /*                :: "--verbose"
                :: "--verboseKey" :: "Native"
                :: "--verboseKey" :: "GenerateIR"*/
          :: cFlags
    ) shouldBe a[Main.SuccessfulTermination]
  }

  it should "make default task timeout 48 hours" taggedAs (NativeTestXX) in {
    val path = pathFromBasename("compiler", "add_timeout.wdl")
    val appId = Main.compile(
        path.toString :: "--force" :: cFlags
    ) match {
      case SuccessfulTermination(x) => x
      case _                        => throw new Exception("sanity")
    }

    // make sure the timeout is what it should be
    val (stdout, stderr) = Utils.execCommand(s"dx describe ${dxTestProject.getId}:${appId} --json")

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

  it should "timeout can be overriden from the extras file" taggedAs (NativeTestXX) in {
    val path = pathFromBasename("compiler", "add_timeout_override.wdl")
    val extraPath = pathFromBasename("compiler/extras", "short_timeout.json")
    val appId = Main.compile(
        path.toString
          :: "--extras" :: extraPath.toString :: cFlags
    ) match {
      case SuccessfulTermination(x) => x
      case _                        => throw new Exception("sanity")
    }

    // make sure the timeout is what it should be
    val (stdout, stderr) = Utils.execCommand(s"dx describe ${dxTestProject.getId}:${appId} --json")

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

  it should "allow choosing GPU instances" taggedAs (NativeTestXX) in {
    val path = pathFromBasename("compiler", "GPU2.wdl")

    val appId = Main.compile(path.toString :: cFlags) match {
      case SuccessfulTermination(x) => x
      case _                        => throw new Exception("sanity")
    }

    // make sure the timeout is what it should be
    val (stdout, stderr) = Utils.execCommand(s"dx describe ${dxTestProject.getId}:${appId} --json")
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
    retval shouldBe a[Main.SuccessfulTermination]
    val wfId: String = retval match {
      case Main.SuccessfulTermination(id) => id
      case _                              => throw new Exception("sanity")
    }

    val wf = DxWorkflow.getInstance(wfId)
    val wfDesc = wf.describe(Set(Field.Stages))
    val wfStages = wfDesc.stages.get

    // there should be 4 stages: 1) common 2) train_stations 3) outputs 4) reorg
    wfStages.size shouldBe 4
    val reorgStage = wfStages.last

    reorgStage.id shouldBe ("stage-reorg")
    reorgStage.executable shouldBe (appletId)

    // There should be 3 inputs, the output from output stage and the custom reorg config file.
    val reorgInput: JsObject = reorgStage.input match {
      case JsObject(x) => JsObject(x)
      case _           => throw new Exception("sanity")
    }

    // no reorg conf input. only status.
    reorgInput.fields.size shouldBe 1
    reorgInput.fields.keys shouldBe Set(Utils.REORG_STATUS)
  }

  // ignore for now as the test will fail in staging
  it should "Compile a workflow with subworkflows on the platform with the reorg app with config file in the input" in {
    // This works in conjunction with "Compile a workflow with subworkflows on the platform with the reorg app".
    val path = pathFromBasename("subworkflows", basename = "trains_station.wdl")
    val appletId = getAppletId(s"/${unitTestsPath}/applets/functional_reorg_test")
    // upload random file
    val (uploadOut, uploadErr) = Utils.execCommand(
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

    retval shouldBe a[Main.SuccessfulTermination]
    val wfId: String = retval match {
      case Main.SuccessfulTermination(wfId) => wfId
      case _                                => throw new Exception("sanity")
    }

    val wf = DxWorkflow.getInstance(wfId)
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
    reorgInput.fields.keys shouldBe Set(Utils.REORG_STATUS, Utils.REORG_CONFIG)
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
    retval shouldBe a[Main.SuccessfulTerminationIR]

    val bundle = retval match {
      case Main.SuccessfulTerminationIR(bundle) => bundle
      case _                                    => throw new Exception("sanity")
    }

    // this is a subworkflow so there is no reorg_status___ added.
    val trainsOutputVector: IR.Callable = bundle.allCallables("trains")
    trainsOutputVector.outputVars.size shouldBe 1
  }

  it should "Set job-reuse flag" taggedAs (NativeTestXX) in {
    val path = pathFromBasename("compiler", "add_timeout.wdl")
    val extrasContent =
      """|{
         |  "ignoreReuse": true
         |}
         |""".stripMargin
    val extrasPath = createExtras(extrasContent)

    // compile the task while
    val retval = Main.compile(
        path.toString :: "--extras" :: extrasPath.toString :: cFlags
    )
    retval shouldBe a[Main.SuccessfulTermination]

    val appletId = retval match {
      case SuccessfulTermination(x) => x
      case _                        => throw new Exception("sanity")
    }

    // make sure the job reuse flag is set
    val (stdout, stderr) =
      Utils.execCommand(s"dx describe ${dxTestProject.getId}:${appletId} --json")
    val ignoreReuseFlag = stdout.parseJson.asJsObject.fields.get("ignoreReuse")
    ignoreReuseFlag shouldBe Some(JsBoolean(true))
  }

  it should "set job-reuse flag on workflow" taggedAs (NativeTestXX) in {
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
    retval shouldBe a[Main.SuccessfulTermination]

    val wfId = retval match {
      case Main.SuccessfulTermination(x) => x
      case _                             => throw new Exception("sanity")
    }

    // make sure the job reuse flag is set
    val (stdout, stderr) =
      Utils.execCommand(s"dx describe ${dxTestProject.getId}:${wfId} --json")
    val ignoreReuseFlag = stdout.parseJson.asJsObject.fields.get("ignoreReuse")
    ignoreReuseFlag shouldBe Some(JsArray(JsString("*")))
  }

  it should "set delayWorkspaceDestruction on applet" taggedAs (NativeTestXX) in {
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
    retval shouldBe a[Main.SuccessfulTermination]

    val appletId = retval match {
      case SuccessfulTermination(x) => x
      case _                        => throw new Exception("sanity")
    }

    // make sure the delayWorkspaceDestruction flag is set
    val (stdout, stderr) =
      Utils.execCommand(s"dx describe ${dxTestProject.getId}:${appletId} --json")
    val details = stdout.parseJson.asJsObject.fields.get("details").get
    val delayWD = details.asJsObject.fields.get("delayWorkspaceDestruction")
    delayWD shouldBe Some(JsTrue)
  }

  it should "set delayWorkspaceDestruction on workflow" taggedAs (NativeTestXX) in {
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
    retval shouldBe a[Main.SuccessfulTermination]

    val wfId = retval match {
      case Main.SuccessfulTermination(x) => x
      case _                             => throw new Exception("sanity")
    }

    // make sure the flag is set on the resulting workflow
    val (stdout, stderr) =
      Utils.execCommand(s"dx describe ${dxTestProject.getId}:${wfId} --json")
    val details = stdout.parseJson.asJsObject.fields.get("details").get
    val delayWD = details.asJsObject.fields.get("delayWorkspaceDestruction")
    delayWD shouldBe Some(JsTrue)
  }

}
