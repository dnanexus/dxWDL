package dx.dxni

import java.nio.file.{Files, Path, Paths}

import dx.Assumptions.{isLoggedIn, toolkitCallable}
import dx.Tags.NativeTest
import dx.api.{DxApi, DxApplet, DxPath}
import dx.compiler.Main
import dx.core.languages.wdl.parseWdlTasks
import dx.core.util.MainUtils.Success
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import wdlTools.types.{TypedAbstractSyntax => TAT}
import wdlTools.util.{FileUtils, Logger, SysUtils}

class DxNativeInterfaceTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  assume(isLoggedIn)
  assume(toolkitCallable)
  private val logger = Logger.Quiet
  private val dxApi = DxApi(logger)

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

  private val username = System.getProperty("user.name")
  private val unitTestsPath = s"unit_tests/${username}"
  private val folderPath = s"/${unitTestsPath}/applets/"

  override def beforeAll(): Unit = {
    // build the directory with the native applets
    dxTestProject.removeFolder(folderPath, recurse = true)
    dxTestProject.newFolder(folderPath, parents = true)
    // building necessary applets before starting the tests
    val nativeApplets = Vector(
        "native_concat",
        "native_diff",
        "native_mk_list",
        "native_sum",
        "native_sum_012"
    )
    val topDir = Paths.get(System.getProperty("user.dir"))
    nativeApplets.foreach { app =>
      SysUtils.execCommand(
          s"dx build $topDir/test/applets/$app --destination ${testProject}:${folderPath}",
          logger = Logger.Quiet
      )
    }
  }

  private def runDxni(args: Vector[String]): Map[String, TAT.Task] = {
    val outputPath: Path = Files.createTempFile("dx_extern_one", ".wdl")
    try {
      Main.dxni(args ++ Vector("-output", outputPath.toString)) shouldBe a[Success]
      // check that the generated file contains the correct tasks
      val content = FileUtils.readFileContent(outputPath)
      val (tasks, _, _) = parseWdlTasks(content)
      tasks
    } finally {
      if (Files.exists(outputPath)) {
        Files.delete(outputPath)
      }
    }
  }

  it should "be able to build interfaces to native applets" taggedAs NativeTest in {
    val args = Vector("-force",
                      "-quiet",
                      "-folder",
                      folderPath,
                      "-project",
                      dxTestProject.getId,
                      "-language",
                      "wdl_1_0",
                      "-apps",
                      "exclude")
    val tasks = runDxni(args)
    tasks.keySet shouldBe Set(
        "native_sum",
        "native_sum_012",
        "native_mk_list",
        "native_diff",
        "native_concat"
    )
  }

  it should "be able to build an interface to a specific applet" taggedAs NativeTest in {
    val args = Vector(
        "-force",
        "-quiet",
        "-path",
        s"${folderPath}/native_sum",
        "-project",
        dxTestProject.getId,
        "-language",
        "wdl_1_0",
        "-apps",
        "exclude"
    )
    val tasks = runDxni(args)
    tasks.keySet shouldBe Set("native_sum")
  }

  it should "build an interface to an applet specified by ID" taggedAs NativeTest in {
    val applet = dxApi.resolveOnePath(
        s"${DxPath.DxUriPrefix}${dxTestProject.id}:${folderPath}/native_sum"
    ) match {
      case applet: DxApplet => applet
      case other            => throw new Exception(s"${other} not an applet")
    }
    val args = Vector("-force",
                      "-quiet",
                      "-path",
                      applet.id,
                      "-project",
                      dxTestProject.getId,
                      "-language",
                      "wdl_1_0",
                      "-apps",
                      "exclude")
    val tasks = runDxni(args)
    tasks.keySet shouldBe Set("native_sum")
  }
}
