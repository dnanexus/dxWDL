package dx.compiler

import java.nio.file.{Path, Paths}

import dx.Assumptions.isLoggedIn
import dx.Tags.NativeTest
import dx.api._
import dx.compiler.Main.{SuccessJsonTree, SuccessPrettyTree}
import dx.core.Native
import dx.core.util.MainUtils.{Failure, Success}
import dx.core.util.CompressionUtils
import org.scalatest.Inside._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import spray.json._
import wdlTools.util.Logger

// This test module requires being logged in to the platform.
// It compiles WDL scripts without the runtime library.
// This tests the compiler Native mode, however, it creates
// dnanexus applets and workflows that are not runnable.

class ExecTreeTest extends AnyFlatSpec with Matchers {
  assume(isLoggedIn)
  private def pathFromBasename(dir: String, basename: String): Path = {
    val p = getClass.getResource(s"/${dir}/${basename}").getPath
    Paths.get(p)
  }

  private val dxApi = DxApi(Logger.Quiet)
  private val testProject = "dxWDL_playground"

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
  private lazy val cFlagsUnlocked = Vector("-compileMode",
                                           "NativeWithoutRuntimeAsset",
                                           "-project",
                                           dxTestProject.getId,
                                           "-folder",
                                           "/" + unitTestsPath,
                                           "-force",
                                           "-quiet")
  private lazy val cFlagsLocked = cFlagsUnlocked :+ "-locked"

  // linear workflow
  it should "Native compile a linear WDL workflow without expressions" taggedAs NativeTest in {
    val path = pathFromBasename("compiler", "wf_linear_no_expr.wdl")
    val args = Vector(path.toString, "-execTree", "json") ++ cFlagsLocked
    val retval = Main.compile(args)
    val treeJs = retval match {
      case SuccessJsonTree(treeJs: JsValue) => treeJs
      case Failure(msg, Some(exception)) =>
        throw new Exception(s"Unable to compile workflow: ${msg}", exception)
      case Failure(msg, None) =>
        throw new Exception(s"Unable to compile workflow: ${msg}")
      case _ =>
        throw new Exception(s"Unexpected result ${retval}")
    }

    treeJs.asJsObject.getFields("name", "kind", "stages") match {
      case Seq(JsString(name), JsString(kind), JsArray(_)) =>
        name shouldBe "wf_linear_no_expr"
        kind shouldBe "workflow"
    }
  }

  // linear workflow
  it should "Native compile a linear WDL workflow with execTree in details" taggedAs NativeTest in {
    val path = pathFromBasename("compiler", "wf_linear_no_expr.wdl")
    val args = path.toString +: cFlagsLocked
    val retval = Main.compile(args)
    val wf = retval match {
      case Success(id) => DxWorkflow(dxApi, id, Some(dxTestProject))
      case Failure(msg, Some(exception)) =>
        throw new Exception(s"Unable to compile workflow: ${msg}", exception)
      case Failure(msg, None) =>
        throw new Exception(s"Unable to compile workflow: ${msg}")
      case _ =>
        throw new Exception(s"Unexpected result ${retval}")
    }

    val description = wf.describe(Set(Field.Details))
    val details: Map[String, JsValue] = description.details match {
      case Some(x: JsValue) => x.asJsObject.fields
      case _                => throw new Exception("Expect details to be set for workflow")
    }
    // the compiled wf should at least have wdlSourceCode and execTree
    details should contain key Native.SourceCode
    details should contain key "execTree"

    val execString = details("execTree") match {
      case JsString(x) => x
      case other =>
        throw new Exception(
            s"Expected execTree to be JsString got ${other} instead."
        )
    }

    val treeJs = CompressionUtils.base64DecodeAndGunzip(execString).parseJson

    treeJs.asJsObject.getFields("name", "kind", "stages") match {
      case Seq(JsString(name), JsString(kind), JsArray(stages)) =>
        name shouldBe "wf_linear_no_expr"
        kind shouldBe "workflow"
        stages.size shouldBe 3
      case _ =>
        throw new Exception(s"tree representation is wrong ${treeJs}")
    }
  }

  //able to describe linear workflow using Tree
  it should "Get execTree from a compiled workflow" taggedAs NativeTest in {
    val path = pathFromBasename("compiler", "wf_linear_no_expr.wdl")
    val args = path.toString +: cFlagsLocked
    val retval = Main.compile(args)
    val wf = retval match {
      case Success(id) => DxWorkflow(dxApi, id, Some(dxTestProject))
      case Failure(msg, Some(exception)) =>
        throw new Exception(s"Unable to compile workflow: ${msg}", exception)
      case Failure(msg, None) =>
        throw new Exception(s"Unable to compile workflow: ${msg}")
      case _ =>
        throw new Exception(s"Unexpected result ${retval}")
    }
    val treeJs = ExecutableTree.fromDxWorkflow(wf)
    treeJs.asJsObject.getFields("id", "name", "kind", "stages") match {
      case Seq(JsString(id), JsString(name), JsString(kind), JsArray(stages)) =>
        id shouldBe wf.id
        name shouldBe "wf_linear_no_expr"
        kind shouldBe "workflow"
        stages.size shouldBe 3
      case _ =>
        throw new Exception(s"tree representation is wrong ${treeJs}")
    }
  }

  it should "Native compile a workflow with one level nesting" taggedAs NativeTest in {
    val path = pathFromBasename("nested", "two_levels.wdl")
    val args = Vector(path.toString, "-execTree", "json") ++ cFlagsLocked
    val retval = Main.compile(args)
    val treeJs = retval match {
      case SuccessJsonTree(treeJs: JsValue) => treeJs
      case Failure(msg, Some(exception)) =>
        throw new Exception(s"Unable to compile workflow: ${msg}", exception)
      case Failure(msg, None) =>
        throw new Exception(s"Unable to compile workflow: ${msg}")
      case _ =>
        throw new Exception(s"Unexpected result ${retval}")
    }

    treeJs.asJsObject.getFields("name", "kind", "stages") match {
      case Seq(JsString(name), JsString(kind), JsArray(stages)) =>
        name shouldBe "two_levels"
        kind shouldBe "workflow"
        println(stages.mkString("\n\n"))
        stages.size shouldBe 4
    }
  }

  it should "Convert JS Tree to Pretty" taggedAs NativeTest in {
    val path = pathFromBasename("nested", "four_levels.wdl")
    // remove -locked flag to create common stage
    // "-verbose", "-verboseKey", "GenerateIR"
    val args = Vector(path.toString, "-execTree", "json") ++ cFlagsUnlocked
    val retval = Main.compile(args)
    val treeJs = retval match {
      case SuccessJsonTree(treeJs: JsValue) => treeJs
      case Failure(msg, Some(exception)) =>
        throw new Exception(s"Unable to compile workflow: ${msg}", exception)
      case Failure(msg, None) =>
        throw new Exception(s"Unable to compile workflow: ${msg}")
      case _ =>
        throw new Exception(s"Unexpected result ${retval}")
    }
    val prettyTree = ExecutableTree.prettyPrint(treeJs.asJsObject)
    val results = prettyTree.replaceAll("\u001B\\[[;\\d]*m", "")
    results shouldBe """Workflow: four_levels
                       |├───App Inputs: common
                       |├───App Fragment: if (username == "a")
                       |│   └───Workflow: four_levels_block_0
                       |│       ├───App Task: c1
                       |│       └───App Task: c2
                       |├───App Fragment: scatter (i in [1, 4, 9])
                       |│   └───App Fragment: four_levels_frag_DxWorkflowStage(stage-6)
                       |│       └───App Fragment: four_levels_frag_DxWorkflowStage(stage-4)
                       |│           └───App Task: concat
                       |└───App Outputs: outputs""".stripMargin
  }

  it should "return a execTree in json when using describe with CLI" taggedAs NativeTest in {
    val path = pathFromBasename("nested", "four_levels.wdl")
    val args = path.toString +: cFlagsUnlocked
    val retval = Main.compile(args)
    val wfID = retval match {
      case Success(wfID) => wfID
      case Failure(msg, Some(exception)) =>
        throw new Exception(s"Unable to compile workflow: ${msg}", exception)
      case Failure(msg, None) =>
        throw new Exception(s"Unable to compile workflow: ${msg}")
      case _ =>
        throw new Exception(s"Unexpected result ${retval}")
    }

    val describeRet = Main.describe(Vector(wfID))
    describeRet shouldBe a[SuccessJsonTree]

    inside(describeRet) {
      case SuccessJsonTree(treeJs) =>
        treeJs.asJsObject.getFields("name", "kind", "stages", "id") match {
          case Seq(JsString(name), JsString(kind), JsArray(stages), JsString(id)) =>
            name shouldBe "four_levels"
            kind shouldBe "workflow"
            stages.size shouldBe 4
            id shouldBe wfID
        }
    }
  }

  // The generated tree is now different
  it should "return a execTree in PrettyTree when using describe with CLI" taggedAs NativeTest in {
    val path = pathFromBasename("nested", "four_levels.wdl")
    val args = path.toString +: cFlagsUnlocked
    val retval = Main.compile(args)
    val wfID = retval match {
      case Success(wfID) => wfID
      case Failure(msg, Some(exception)) =>
        throw new Exception(s"Unable to compile workflow: ${msg}", exception)
      case Failure(msg, None) =>
        throw new Exception(s"Unable to compile workflow: ${msg}")
      case _ =>
        throw new Exception(s"Unexpected result ${retval}")
    }

    val describeRet = Main.describe(Vector(wfID, "-pretty"))
    val pretty = describeRet match {
      case SuccessPrettyTree(pretty) => pretty
      case Failure(msg, Some(exception)) =>
        throw new Exception(s"Unable to compile workflow: ${msg}", exception)
      case Failure(msg, None) =>
        throw new Exception(s"Unable to compile workflow: ${msg}")
      case _ =>
        throw new Exception(s"Unexpected result ${retval}")
    }

    // remove colours
    pretty.replaceAll("\u001B\\[[;\\d]*m", "") shouldBe """Workflow: four_levels
                                                          |├───App Inputs: common
                                                          |├───App Fragment: if (username == "a")
                                                          |│   └───Workflow: four_levels_block_0
                                                          |│       ├───App Task: c1
                                                          |│       └───App Task: c2
                                                          |├───App Fragment: scatter (i in [1, 4, 9])
                                                          |│   └───App Fragment: four_levels_frag_DxWorkflowStage(stage-6)
                                                          |│       └───App Fragment: four_levels_frag_DxWorkflowStage(stage-4)
                                                          |│           └───App Task: concat
                                                          |└───App Outputs: outputs""".stripMargin
  }

  it should "Display pretty print of tree with deep nesting" taggedAs NativeTest in {
    val path = pathFromBasename("nested", "four_levels.wdl")
    val args = Vector(path.toString, "-execTree", "pretty") ++ cFlagsUnlocked
    val retval = Main.compile(args)
    val pretty = retval match {
      case SuccessPrettyTree(pretty) => pretty
      case Failure(msg, Some(exception)) =>
        throw new Exception(s"Unable to compile workflow: ${msg}", exception)
      case Failure(msg, None) =>
        throw new Exception(s"Unable to compile workflow: ${msg}")
      case _ =>
        throw new Exception(s"Unexpected result ${retval}")
    }

    // remove colours
    pretty.replaceAll("\u001B\\[[;\\d]*m", "") shouldBe """Workflow: four_levels
                                                          |├───App Inputs: common
                                                          |├───App Fragment: if (username == "a")
                                                          |│   └───Workflow: four_levels_block_0
                                                          |│       ├───App Task: c1
                                                          |│       └───App Task: c2
                                                          |├───App Fragment: scatter (i in [1, 4, 9])
                                                          |│   └───App Fragment: four_levels_frag_DxWorkflowStage(stage-6)
                                                          |│       └───App Fragment: four_levels_frag_DxWorkflowStage(stage-4)
                                                          |│           └───App Task: concat
                                                          |└───App Outputs: outputs""".stripMargin
  }
}
