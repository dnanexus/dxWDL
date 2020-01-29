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
    Utils.execCommand(s"dx mkdir -p ${TEST_PROJECT}:${unitTestsPath}", quiet = true)
    Utils.execCommand(s"dx rm -r ${TEST_PROJECT}:/${unitTestsPath}", quiet = true)
    Utils.execCommand(s"dx mkdir -p ${TEST_PROJECT}:/${unitTestsPath}/applets/", quiet = true)

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

  it should "Native compile a single WDL task" taggedAs (NativeTestXX, EdgeTest) in {
    val path = pathFromBasename("compiler", "add.wdl")
    val retval = Main.compile(
        path.toString :: "--execTree" :: cFlags
    )
    retval shouldBe a[Main.SuccessfulTerminationTree]
    inside(retval) {
      case Main.SuccessfulTerminationTree(treeJs, treePretty) =>
        treeJs.asJsObject.getFields("name", "kind") match {
          case Seq(JsString(name), JsString(kind)) =>
            name shouldBe ("add")
            kind shouldBe ("Task")
          //System.out.println(treePretty)
          case other =>
            throw new Exception(s"tree representation is wrong ${treeJs}")
        }
        println(treePretty)
    }
  }

  // linear workflow
  it should "Native compile a linear WDL workflow without expressions" taggedAs (NativeTestXX, EdgeTest) in {
    val path = pathFromBasename("compiler", "wf_linear_no_expr.wdl")
    val retval = Main.compile(path.toString :: "--execTree" :: cFlags)
    retval shouldBe a[Main.SuccessfulTerminationTree]

    inside(retval) {
      case Main.SuccessfulTerminationTree(treeJs, treePretty) =>
        treeJs.asJsObject.getFields("name", "kind", "stages") match {
          case Seq(JsString(name), JsString(kind), JsArray(stages)) =>
            name shouldBe ("wf_linear_no_expr")
            kind shouldBe ("workflow")
            stages.size shouldBe (3)
          //System.out.println(treePretty)
          case other =>
            throw new Exception(s"tree representation is wrong ${treeJs}")
        }
        println(treePretty)
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

  it should "Native compile a workflow with one level nesting" taggedAs (NativeTestXX, EdgeTest) in {
    val path = pathFromBasename("nested", "two_levels.wdl")
    val retval = Main.compile(
        path.toString :: "--force" :: "--execTree" :: cFlags
    )
    retval shouldBe a[Main.SuccessfulTerminationTree]

    inside(retval) {
      case Main.SuccessfulTerminationTree(treeJs, treePretty) =>
        treeJs.asJsObject.getFields("name", "kind", "stages") match {
          case Seq(JsString(name), JsString(kind), JsArray(stages)) =>
            name shouldBe ("two_levels")
            kind shouldBe ("workflow")
            stages.size shouldBe (3)
          //System.out.println(treePretty)
          case other =>
            throw new Exception(s"tree representation is wrong ${treeJs}")
        }
        println(treePretty)
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

  it should "be able to build interfaces to native applets" taggedAs (NativeTestXX) in {
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

  it should "be able to build an interface to a specific applet" taggedAs (NativeTestXX) in {
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
    in_file.patterns shouldBe Some(Vector("*.txt", "*.tsv"))
    // out_file would be part of the outputSpec, but wom currently doesn't
    // support parameter_meta for output vars
    //out_file.pattern shouldBe Some(Vector("*.txt", "*.tsv"))
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
    val (a, b) = inputSpec.inputSpec match {
      case Some(x) => (x(0), x(1))
      case other   => throw new Exception(s"Unexpected result ${other}")
    }
    a.help shouldBe Some("lefthand side")
    b.help shouldBe Some("righthand side")
  }

  it should "be able to include license information in details" in {
    val expected =
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

    val path = pathFromBasename("compiler", "add.wdl")
    val extraPath = pathFromBasename("compiler/extras", "extras_license.json")

    val appId = Main.compile(
        path.toString
        //:: "--verbose"
          :: "--extras" :: extraPath.toString :: cFlags
    ) match {
      case SuccessfulTermination(x) => x
      case _                        => throw new Exception("sanity")

    }

    val dxApplet = DxApplet.getInstance(appId)
    val desc = dxApplet.describe(Set(Field.Details))
    val license = desc.details match {
      case Some(JsObject(x)) =>
        x.get("upstreamProjects") match {
          case None    => List.empty
          case Some(s) => s

        }
      case other => throw new Exception(s"Unexpected result ${other}")
    }

    license shouldBe expected
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
}
