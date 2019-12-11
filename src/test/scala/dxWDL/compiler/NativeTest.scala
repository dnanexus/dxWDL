package dxWDL.compiler

import java.nio.file.{Path, Paths}

import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.BeforeAndAfterAll

import scala.io.Source
import dxWDL.Main
import dxWDL.Main.SuccessfulTermination
import dxWDL.base.Utils
import dxWDL.dx.DxPath
import dxWDL.util.ParseWomSourceFile
import spray.json._

// This test module requires being logged in to the platform.
// It compiles WDL scripts without the runtime library.
// This tests the compiler Native mode, however, it creates
// dnanexus applets and workflows that are not runnable.

class NativeTest extends FlatSpec with Matchers with BeforeAndAfterAll {
    private def pathFromBasename(dir: String, basename: String) : Path = {
        val p = getClass.getResource(s"/${dir}/${basename}").getPath
        Paths.get(p)
    }

    val TEST_PROJECT = "dxWDL_playground"
    lazy val dxTestProject =
        try {
            DxPath.resolveProject(TEST_PROJECT)
        } catch {
            case e : Exception =>
                throw new Exception(s"""|Could not find project ${TEST_PROJECT}, you probably need to be logged into
                                        |the platform""".stripMargin)
        }
    lazy val cFlags = List("-compileMode", "NativeWithoutRuntimeAsset",
                           "-project", dxTestProject.getId,
                           "-folder", "/unit_tests",
                           "-force",
                           "-locked",
                           "-quiet")

    override def beforeAll() : Unit = {
        // build the directory with the native applets
        Utils.execCommand(
            s"dx mkdir -p ${TEST_PROJECT}:/unit_tests/applets/",
                quiet=true)

        // building necessary applets before starting the tests
        val native_applets = Vector("native_concat",
                                    "native_diff",
                                    "native_mk_list",
                                    "native_sum",
                                    "native_sum_012")
        val topDir = Paths.get(System.getProperty("user.dir"))
        native_applets.foreach { app =>
            try {
                val (stdout, stderr) = Utils.execCommand(
                    s"dx build $topDir/test/applets/$app --destination ${TEST_PROJECT}:/unit_tests/applets/",
                    quiet=true)
            } catch {
                case _: Throwable =>
            }
        }
    }


    it should "Native compile a single WDL task" taggedAs(NativeTestXX) in {
        val path = pathFromBasename("compiler", "add.wdl")
        val retval = Main.compile(path.toString
//                                      :: "--verbose"
                                      :: cFlags)
        retval shouldBe a [Main.SuccessfulTermination]
    }

    // linear workflow
    it  should "Native compile a linear WDL workflow without expressions" taggedAs(NativeTestXX) in {
        val path = pathFromBasename("compiler", "wf_linear_no_expr.wdl")
        val retval = Main.compile(path.toString :: cFlags)
        retval shouldBe a [Main.SuccessfulTermination]
    }

    it  should "Native compile a linear WDL workflow" taggedAs(NativeTestXX) in {
        val path = pathFromBasename("compiler", "wf_linear.wdl")
        val retval = Main.compile(path.toString
                                      :: cFlags)
        retval shouldBe a [Main.SuccessfulTermination]
    }

    it should "Native compile a workflow with a scatter without a call" taggedAs(NativeTestXX) in {
        val path = pathFromBasename("compiler", "scatter_no_call.wdl")
        Main.compile(
            path.toString :: cFlags
        ) shouldBe a [Main.SuccessfulTermination]
    }


    it should "Native compile a draft2 workflow" taggedAs(NativeTestXX) in {
        val path = pathFromBasename("draft2", "shapes.wdl")
        Main.compile(
            path.toString :: "--force" :: cFlags
        ) shouldBe a [Main.SuccessfulTermination]
    }

    it should "Native compile a workflow with one level nesting" taggedAs(NativeTestXX) in {
        val path = pathFromBasename("nested", "two_levels.wdl")
        Main.compile(
            path.toString :: "--force" :: cFlags
        ) shouldBe a [Main.SuccessfulTermination]
    }

    it should "handle various conditionals" taggedAs(NativeTestXX) in {
        val path = pathFromBasename("draft2", "conditionals_base.wdl")
        Main.compile(
            path.toString
/*                :: "--verbose"
                :: "--verboseKey" :: "Native"
                :: "--verboseKey" :: "GenerateIR"*/
                :: cFlags
        ) shouldBe a [Main.SuccessfulTermination]
    }

    it should "be able to build interfaces to native applets" taggedAs(NativeTestXX, EdgeTest) in {
        val outputPath = "/tmp/dx_extern.wdl"
        Main.dxni(
            List("--force", "--quiet",
                 "--folder", "/unit_tests/applets",
                 "--project", dxTestProject.getId,
                 "--language", "wdl_draft2",
                 "--output", outputPath)
        ) shouldBe a [Main.SuccessfulTermination]

        // check that the generated file contains the correct tasks
        val content = Source.fromFile(outputPath).getLines.mkString("\n")

        val tasks : Map[String, String] = ParseWomSourceFile(false).scanForTasks(content)
        tasks.keys shouldBe(Set("native_sum", "native_sum_012", "native_mk_list", "native_diff", "native_concat"))
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
        val extraPath = pathFromBasename("compiler/extras",  "extras_license.json")

        val appId = Main.compile(
            path.toString
                /*:: "--verbose" :: "--verboseKey" :: "EdgeTest" */
                :: "--extras" :: extraPath.toString :: cFlags
        ) match {
            case SuccessfulTermination(x) => x
            case _ => throw new Exception("sanity")

        }

        val (stdout, stderr) = Utils.execCommand(s"dx describe ${dxTestProject.getId}:${appId} --json")

        val license = stdout.parseJson.asJsObject.fields.get("details") match {
            case Some(JsObject(x)) => x.get("upstreamProjects") match {
                case None => List.empty
                case Some(s) => s

            }
            case other => throw new Exception(s"Unexpected result ${other}")
        }

        license shouldBe expected
    }

    it should "deep nesting" taggedAs(NativeTestXX) in {
        val path = pathFromBasename("compiler", "environment_passing_deep_nesting.wdl")
        Main.compile(
            path.toString
/*                :: "--verbose"
                :: "--verboseKey" :: "Native"
                :: "--verboseKey" :: "GenerateIR"*/
                :: cFlags
        ) shouldBe a [Main.SuccessfulTermination]
    }

    it should "make default task timeout 48 hours" taggedAs(NativeTestXX) in {
        val path = pathFromBasename("compiler", "add_timeout.wdl")
        val appId = Main.compile(
            path.toString :: "--force" :: cFlags
        ) match {
            case SuccessfulTermination(x) => x
            case _ => throw new Exception("sanity")
        }

        // make sure the timeout is what it should be
        val (stdout, stderr) = Utils.execCommand(
            s"dx describe ${dxTestProject.getId}:${appId} --json")

        val timeout = stdout.parseJson.asJsObject.fields.get("runSpec") match {
            case Some(JsObject(x)) => x.get("timeoutPolicy") match {
                case None => throw new Exception("No timeout policy set")
                case Some(s) => s
            }
            case other => throw new Exception(s"Unexpected result ${other}")
        }
        timeout shouldBe JsObject("*" -> JsObject(
                                      "days" -> JsNumber(2),
                                      "hours" -> JsNumber(0),
                                      "minutes" -> JsNumber(0)))
    }

    it should "timeout can be overriden from the extras file" taggedAs(NativeTestXX) in {
        val path = pathFromBasename("compiler", "add_timeout_override.wdl")
        val extraPath = pathFromBasename("compiler/extras",  "short_timeout.json")
        val appId = Main.compile(
            path.toString
                :: "--extras" :: extraPath.toString :: cFlags
        ) match {
            case SuccessfulTermination(x) => x
            case _ => throw new Exception("sanity")
        }

        // make sure the timeout is what it should be
        val (stdout, stderr) = Utils.execCommand(
            s"dx describe ${dxTestProject.getId}:${appId} --json")

        val timeout = stdout.parseJson.asJsObject.fields.get("runSpec") match {
            case Some(JsObject(x)) => x.get("timeoutPolicy") match {
                case None => throw new Exception("No timeout policy set")
                case Some(s) => s
            }
            case other => throw new Exception(s"Unexpected result ${other}")
        }
        timeout shouldBe JsObject("*" -> JsObject("hours" -> JsNumber(3)))

    }

    it should "allow choosing GPU instances" taggedAs(NativeTestXX) in {
        val path = pathFromBasename("compiler", "GPU2.wdl")

        val appId = Main.compile(path.toString :: cFlags) match {
            case SuccessfulTermination(x) => x
            case _ => throw new Exception("sanity")
        }

        // make sure the timeout is what it should be
        val (stdout, stderr) = Utils.execCommand(
            s"dx describe ${dxTestProject.getId}:${appId} --json")
        val obj = stdout.parseJson.asJsObject
        val obj2 = obj.fields("runSpec").asJsObject
        val obj3 = obj2.fields("systemRequirements").asJsObject
        val obj4 = obj3.fields("main").asJsObject
        val instanceType = obj4.fields.get("instanceType") match {
            case Some(JsString(x)) => x
            case other => throw new Exception(s"Unexpected result ${other}")
        }

        //System.out.println(s"instanceType = ${instanceType}")
        instanceType should include ("_gpu")
    }
}
