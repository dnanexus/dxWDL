package dxWDL.compiler

import java.nio.file.{Path, Paths}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.Inside._

import dxWDL.Main
import dxWDL.base.Utils
import dxWDL.dx._
import spray.json._

// This test module requires being logged in to the platform.
// It compiles WDL scripts without the runtime library.
// This tests the compiler Native mode, however, it creates
// dnanexus applets and workflows that are not runnable.

class ExecTreeTest extends AnyFlatSpec with Matchers {
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

  // linear workflow
  it should "Native compile a linear WDL workflow without expressions" in {
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
  it should "Native compile a linear WDL workflow with execTree in details" in {
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

  //able to describe linear workflow using Tree
  it should "Get execTree from a compiled workflow" in {
    val path = pathFromBasename("compiler", "wf_linear_no_expr.wdl")
    val retval = Main.compile(path.toString :: cFlags)
    retval shouldBe a[Main.SuccessfulTermination]

    val wf: DxWorkflow = retval match {
      case Main.SuccessfulTermination(id) => DxWorkflow(id, Some(dxTestProject))
      case _                              => throw new Exception("sanity")
    }

    val treeJs = Tree.formDXworkflow(wf)
    treeJs.asJsObject.getFields("id", "name", "kind", "stages") match {
      case Seq(JsString(id), JsString(name), JsString(kind), JsArray(stages)) =>
        id shouldBe (wf.id)
        name shouldBe ("wf_linear_no_expr")
        kind shouldBe ("workflow")
        stages.size shouldBe (3)
      case other =>
        throw new Exception(s"tree representation is wrong ${treeJs}")
    }
  }

  it should "Native compile a workflow with one level nesting" in {
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

  ignore should "Convert JS Tree to Pretty" in {
    val path = pathFromBasename("nested", "four_levels.wdl")
    // remove -locked flag to create common stage
    val nonLocked = cFlags.filterNot(x => x == "-locked")
    val retval = Main.compile(
      path.toString
        :: "--force" :: "--execTree" :: "json"
        :: "--verbose" :: "--verboseKey" :: "GenerateIR"
        :: nonLocked
    )
    retval shouldBe a[Main.SuccessfulTerminationTree]
    val treeJs: JsValue = retval match {
      case Main.SuccessfulTerminationTree(pretty) =>
        pretty match {
          case Left(str) =>
            throw new Exception(s"tree representation is wrong ${str}") // should not produce a pretty string
          case Right(treeJs) => treeJs
        }
      case other => throw new Exception(s"tree representation is wrong")
    }

    val prettyTree = Tree.generateTreeFromJson(treeJs.asJsObject)

    // This generated tree now looks different. Also, the Tree code
    // doesn't give good names to the scatter nested below "(i in [1, 4, 9])".
    // This is inspite the fact that the stage names are human readable.
    //
    /*    val results = prettyTree.replaceAll("\u001B\\[[;\\d]*m", "")
    results shouldBe """Workflow: four_levels
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
 */
    System.out.println(prettyTree)
  }

  it should "return a execTree in json when using describe with CLI" in {
    val path = pathFromBasename("nested", "four_levels.wdl")
    // remove -locked flag to create common stage
    val nonLocked = cFlags.filterNot(x => x == "-locked")
    val retval = Main.compile(
        path.toString :: "--force" :: nonLocked
    )
    val wfID = retval match {
      case Main.SuccessfulTermination(wfID) => wfID
      case _                                => throw new Exception("Unable to compile workflow.")
    }

    val describeRet = Main.describe(Seq(wfID))
    describeRet shouldBe a[Main.SuccessfulTerminationTree]

    inside(describeRet) {
      case Main.SuccessfulTerminationTree(pretty) =>
        pretty match {
          case Left(str) => false // should not produce a pretty string
          case Right(treeJs) => {
            treeJs.asJsObject.getFields("name", "kind", "stages", "id") match {
              case Seq(JsString(name), JsString(kind), JsArray(stages), JsString(id)) =>
                name shouldBe ("four_levels")
                kind shouldBe ("workflow")
                stages.size shouldBe (4)
                id shouldBe wfID
              case other =>
                throw new Exception(s"tree representation is wrong ${treeJs}")
            }
          }
        }
    }
  }

  // The generated tree is now different
  ignore should "return a execTree in PrettyTree when using describe with CLI" in {
    val path = pathFromBasename("nested", "four_levels.wdl")
    // remove -locked flag to create common stage
    val nonLocked = cFlags.filterNot(x => x == "-locked")
    val retval = Main.compile(
        path.toString :: "--force" :: nonLocked
    )
    val wfID = retval match {
      case Main.SuccessfulTermination(wfID) => wfID
      case _                                => throw new Exception("Unable to compile workflow.")
    }

    val describeRet = Main.describe(Seq(wfID, "-pretty"))
    describeRet shouldBe a[Main.SuccessfulTerminationTree]

    inside(describeRet) {
      case Main.SuccessfulTerminationTree(pretty) =>
        pretty match {
          case Right(_)  => false
          case Left(str) =>
            // remove colours
            str.replaceAll("\u001B\\[[;\\d]*m", "") shouldBe """Workflow: four_levels
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

        }
    }
  }

  ignore should "Display pretty print of tree with deep nesting" in {
    val path = pathFromBasename("nested", "four_levels.wdl")
    // remove -locked flag to create common stage
    val nonLocked = cFlags.filterNot(x => x == "-locked")
    val retval = Main.compile(
        path.toString :: "--force" :: "--execTree" :: "pretty" :: nonLocked
    )
    retval shouldBe a[Main.SuccessfulTerminationTree]

    inside(retval) {
      case Main.SuccessfulTerminationTree(pretty) =>
        pretty match {
          case Left(str) =>
            // remove colours
            str.replaceAll("\u001B\\[[;\\d]*m", "") shouldBe """Workflow: four_levels
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
}
