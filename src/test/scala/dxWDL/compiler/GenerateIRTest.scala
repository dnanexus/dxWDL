package dxWDL.compiler

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.{Path, Paths}

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatest.Inside._
import wom.callable.CallableTaskDefinition
import wom.callable.MetaValueElement
import dxWDL.Main
import dxWDL.base.Utils
import dxWDL.dx.{DxPath, DxUtils}
import spray.json._

// These tests involve compilation -without- access to the platform.
//
class GenerateIRTest extends FlatSpec with Matchers with BeforeAndAfterAll {

    val TEST_PROJECT = "dxWDL_playground"

    lazy val dxTestProject =
        try {
            DxPath.resolveProject(TEST_PROJECT)
        } catch {
            case e : Exception =>
                throw new Exception(s"""|Could not find project ${TEST_PROJECT}, you probably need to be logged into
                                        |the platform""".stripMargin)
        }

    override def beforeAll() : Unit = {
        // building necessary applets before starting the tests
        val native_applets = Vector("functional_reorg_test")
        val topDir = Paths.get(System.getProperty("user.dir"))
        native_applets.foreach { app =>
            try {
                val (stdout, stderr) = Utils.execCommand(
                    s"dx build $topDir/test/applets/$app --destination /unit_tests/applets/ -f",
                    quiet = true)
            } catch {
                case _: Throwable =>
            }
        }
    }

    private def pathFromBasename(dir: String, basename: String) : Path = {
        val p = getClass.getResource(s"/${dir}/${basename}").getPath
        Paths.get(p)
    }

    private def getAppletId(path: String): String = {
        val (app_stdout, app_stderr) = Utils.execCommand(
            s"dx describe $path --json")

        app_stdout.parseJson.asJsObject.fields.get("id") match {
            case Some(JsString(x)) => x
            case other => throw new Exception(s"Unexpected result ${other}")
        }
    }

    private def createExtras(extrasContent: String): String = {

        val tmp_extras = File.createTempFile("reorg-", ".json")
        tmp_extras.deleteOnExit()

        val bw = new BufferedWriter(new FileWriter(tmp_extras))
        bw.write(extrasContent)
        bw.close()

        tmp_extras.toString
    }

    private val dxProject = DxUtils.dxEnv.getProjectContext()
    if (dxProject == null)
        throw new Exception("Must be logged in to run this test")

    // task compilation
    private val cFlags = List("--compileMode", "ir",
                              "-quiet",
                              "-fatalValidationWarnings",
                              "--locked",
                              "--project", dxProject.getId)

    private val cFlagsReorg = List(
        "--project", dxProject.getId,
        "--folder", "/reorg_tests")

    private val cFlagsUnlocked = List("--compileMode", "ir",
                                      "-quiet",
                                      "-fatalValidationWarnings",
                                      "--project", dxProject.getId)
    val dbgFlags = List("--compileMode", "ir",
                        "--verbose",
                        "--verboseKey", "GenerateIR",
                        "--locked",
                        "--project", DxUtils.dxEnv.getProjectContext().getId)


    it should "IR compile a single WDL task" in {
        val path = pathFromBasename("compiler", "add.wdl")
        Main.compile(path.toString :: cFlags) shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "IR compile a task with docker" in {
        val path = pathFromBasename("compiler", "BroadGenomicsDocker.wdl")
        Main.compile(path.toString :: cFlags) shouldBe a [Main.SuccessfulTerminationIR]
    }

    // workflow compilation
    it should "IR compile a linear WDL workflow without expressions" in {
        val path = pathFromBasename("compiler", "wf_linear_no_expr.wdl")
        Main.compile(path.toString :: cFlags) shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "IR compile a linear WDL workflow" in {
        val path = pathFromBasename("compiler", "wf_linear.wdl")
        Main.compile(
            path.toString :: cFlags
        ) shouldBe a [Main.SuccessfulTerminationIR]
    }


    it should "IR compile unlocked workflow" in {
        val path = pathFromBasename("compiler", "wf_linear.wdl")
        Main.compile(
            path.toString :: cFlagsUnlocked
        ) shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "IR compile a non trivial linear workflow with variable coercions" in {
        val path = pathFromBasename("compiler", "cast.wdl")
        Main.compile(path.toString :: cFlags) shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "IR compile a workflow with two consecutive calls" in {
        val path = pathFromBasename("compiler", "strings.wdl")
        Main.compile(path.toString :: cFlags) shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "IR compile a workflow with a scatter without a call" in {
        val path = pathFromBasename("compiler", "scatter_no_call.wdl")
        Main.compile(
            path.toString :: cFlags
        ) shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "IR compile optionals" in {
        val path = pathFromBasename("compiler", "optionals.wdl")
        Main.compile(
            path.toString
//                :: "--verbose"
//                :: "--verboseKey" :: "GenerateIR"
                :: cFlags
        ) shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "support imports" in {
        val path = pathFromBasename("compiler", "check_imports.wdl")
        Main.compile(
            path.toString :: cFlags
        ) shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "IR compile a draft2 workflow" in {
        val path = pathFromBasename("draft2", "shapes.wdl")
        val retval = Main.compile(
            path.toString :: cFlags
        )
        retval shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "expressions in an output block" in {
        val path = pathFromBasename("compiler", "expr_output_block.wdl")
        Main.compile(
            path.toString :: cFlags
        ) shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "scatters over maps" in {
        val path = pathFromBasename("compiler", "dict2.wdl")
        Main.compile(
            path.toString :: cFlags
        ) shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "skip missing optional arguments" in {
        val path = pathFromBasename("util", "missing_inputs_to_direct_call.wdl")
        Main.compile(
            path.toString :: cFlags
        ) shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "handle calling subworkflows" in {
        val path = pathFromBasename("subworkflows", "trains.wdl")
        val retval = Main.compile(
            path.toString :: cFlags
        )
        retval shouldBe a [Main.SuccessfulTerminationIR]
        val irwf = retval match {
            case Main.SuccessfulTerminationIR(irwf) => irwf
            case _ => throw new Exception("sanity")
        }
        val primaryWf : IR.Workflow = irwf.primaryCallable match {
            case Some(wf : IR.Workflow) => wf
            case _ => throw new Exception("sanity")
        }
        primaryWf.stages.size shouldBe(2)
    }

    it should "compile a sub-block with several calls" in {
        val path = pathFromBasename("compiler", "subblock_several_calls.wdl")
        Main.compile(
            path.toString :: cFlags
        ) shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "missing workflow inputs" in {
        val path = pathFromBasename("input_file", "missing_args.wdl")
        Main.compile(
            path.toString :: List("--compileMode", "ir", "--quiet",
                                  "--project", DxUtils.dxEnv.getProjectContext().getId)
        ) shouldBe a [Main.SuccessfulTerminationIR]
    }

    // Nested blocks
    it should "compile two level nested workflow" in {
        val path = pathFromBasename("nested", "two_levels.wdl")
        Main.compile(
            path.toString :: cFlags
        ) shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "handle passing closure arguments to nested blocks" in {
        val path = pathFromBasename("nested", "param_passing.wdl")
        Main.compile(
            path.toString :: cFlags
        ) shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "compile a workflow calling a subworkflow as a direct call" in {
        val path = pathFromBasename("draft2", "movies.wdl")
        val bundle : IR.Bundle = Main.compile(path.toString :: cFlags) match {
            case Main.SuccessfulTerminationIR(bundle) => bundle
            case other =>
                Utils.error(other.toString)
                throw new Exception(s"Failed to compile ${path}")
        }
        val wf : IR.Workflow = bundle.primaryCallable match {
            case Some(wf: IR.Workflow) =>
                wf
            case _ => throw new Exception("bad value in bundle")
        }
        val stage = wf.stages.head
        stage.description shouldBe ("review")
    }

    it should "compile a workflow calling a subworkflow as a direct call with development version" in {
        val path = pathFromBasename("development", "movies.wdl")
        val bundle : IR.Bundle = Main.compile(path.toString :: cFlags) match {
            case Main.SuccessfulTerminationIR(bundle) => bundle
            case other =>
                Utils.error(other.toString)
                throw new Exception(s"Failed to compile ${path}")
        }
        val wf : IR.Workflow = bundle.primaryCallable match {
            case Some(wf: IR.Workflow) =>
                wf
            case _ => throw new Exception("bad value in bundle")
        }
        val stage = wf.stages.head
        stage.description shouldBe ("review")
    }


    it should "compile a workflow calling a subworkflow with native DNANexus applet as a direct call with development version" in {
        val path = pathFromBasename("development", "call_dnanexus_applet.wdl")
        val bundle : IR.Bundle = Main.compile(path.toString :: cFlags) match {
            case Main.SuccessfulTerminationIR(bundle) => bundle
            case other =>
                Utils.error(other.toString)
                throw new Exception(s"Failed to compile ${path}")
        }
        val wf : IR.Workflow = bundle.primaryCallable match {
            case Some(wf: IR.Workflow) =>
                wf
            case _ => throw new Exception("bad value in bundle")
        }
        val stage = wf.stages.head
        stage.description shouldBe ("native_sum_wf")
    }

    it should "three nesting levels" in {
        val path = pathFromBasename("nested", "three_levels.wdl")
        val retval = Main.compile(
            path.toString :: cFlags
        )
        retval shouldBe a [Main.SuccessfulTerminationIR]
        val bundle = retval match {
            case Main.SuccessfulTerminationIR(ir) => ir
            case _ => throw new Exception("sanity")
        }
        val primary : IR.Callable = bundle.primaryCallable.get
        val wf = primary match {
            case wf : IR.Workflow => wf
            case _ => throw new Exception("sanity")
        }
        wf.stages.size shouldBe(1)

        val level2 = bundle.allCallables(wf.name)
        level2 shouldBe a[IR.Workflow]
        val wfLevel2 = level2.asInstanceOf[IR.Workflow]
        wfLevel2.stages.size shouldBe(1)
    }


    it should "four nesting levels" in {
        val path = pathFromBasename("nested", "four_levels.wdl")
        val retval = Main.compile(
            path.toString :: cFlags
        )
/*        inside(retval) {
            case Main.UnsuccessfulTermination(errMsg) =>
                errMsg should include ("nested scatter")
 }*/
        retval shouldBe a [Main.SuccessfulTerminationIR]
    }

    private def getTaskByName(name: String,
                              bundle: IR.Bundle) : CallableTaskDefinition = {
        val applet = bundle.allCallables(name) match {
            case a : IR.Applet => a
            case _ => throw new Exception(s"${name} is not an applet")
        }
        val task: CallableTaskDefinition = applet.kind match {
            case IR.AppletKindTask(x) => x
            case _ => throw new Exception(s"${name} is not a task")
        }
        task
    }

    it should "handle streaming files" in {
        val path = pathFromBasename("compiler", "streaming_files.wdl")
        val retval = Main.compile(
            path.toString :: cFlags
        )
        retval shouldBe a [Main.SuccessfulTerminationIR]
        val bundle = retval match {
            case Main.SuccessfulTerminationIR(ir) => ir
            case _ => throw new Exception("sanity")
        }

        val cgrepTask = getTaskByName("cgrep", bundle)
        cgrepTask.parameterMeta shouldBe (Map("in_file" -> MetaValueElement.MetaValueElementString("stream")))
        val iDef = cgrepTask.inputs.find(_.name == "in_file").get
        iDef.parameterMeta shouldBe (Some(MetaValueElement.MetaValueElementString("stream")))

        val diffTask = getTaskByName("diff", bundle)
        diffTask.parameterMeta shouldBe (Map("a" -> MetaValueElement.MetaValueElementString("stream"),
                                             "b" -> MetaValueElement.MetaValueElementString("stream")))
    }

    it should "recognize the streaming object annotation" in {
        val path = pathFromBasename("compiler", "streaming_files_obj.wdl")
        val retval = Main.compile(
            path.toString :: cFlags
        )
        retval shouldBe a [Main.SuccessfulTerminationIR]
        val bundle = retval match {
            case Main.SuccessfulTerminationIR(ir) => ir
            case _ => throw new Exception("sanity")
        }

        val cgrepTask = getTaskByName("cgrep", bundle)
        cgrepTask.parameterMeta shouldBe (Map("in_file" -> MetaValueElement.MetaValueElementObject(Map("stream" -> MetaValueElement.MetaValueElementBoolean(true)))))
        val iDef = cgrepTask.inputs.find(_.name == "in_file").get
        iDef.parameterMeta shouldBe (Some(MetaValueElement.MetaValueElementObject(Map("stream" -> MetaValueElement.MetaValueElementBoolean(true)))))

        val diffTask = getTaskByName("diff", bundle)
        diffTask.parameterMeta shouldBe (Map("a" -> MetaValueElement.MetaValueElementObject(Map("stream" -> MetaValueElement.MetaValueElementBoolean(true))),
                                             "b" -> MetaValueElement.MetaValueElementObject(Map("stream" -> MetaValueElement.MetaValueElementBoolean(true)))))
    }

    it should "recognize the streaming annotation for wdl draft2" in {
        val path = pathFromBasename("draft2", "streaming.wdl")
        val retval = Main.compile(
            path.toString :: cFlags
        )
        retval shouldBe a [Main.SuccessfulTerminationIR]
        val bundle = retval match {
            case Main.SuccessfulTerminationIR(ir) => ir
            case _ => throw new Exception("sanity")
        }
        val diffTask = getTaskByName("diff", bundle)
        diffTask.parameterMeta shouldBe (Map("a" -> MetaValueElement.MetaValueElementString("stream"),
                                             "b" -> MetaValueElement.MetaValueElementString("stream")))
    }

    it should "handle an empty workflow" in {
        val path = pathFromBasename("util", "empty_workflow.wdl")
        val retval = Main.compile(
            path.toString :: cFlags
        )
        retval shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "handle structs" in {
        val path = pathFromBasename("struct", "Person.wdl")
        val retval = Main.compile(
            path.toString :: cFlags
        )
        retval shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "recognize that an argument with a default can be omitted at the call site" in {
        val path = pathFromBasename("compiler", "call_level2.wdl")
        val retval = Main.compile(path.toString :: cFlags)
        retval shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "check for reserved symbols" in {
        val path = pathFromBasename("compiler", "reserved.wdl")
        val retval = Main.compile(path.toString :: cFlags)

        inside(retval) {
            case Main.UnsuccessfulTermination(errMsg) =>
                errMsg should include ("reserved substring ___")
        }
    }

    it should "do nested scatters" in {
        val path = pathFromBasename("compiler", "nested_scatter.wdl")
        val retval = Main.compile(path.toString :: cFlags)
        retval shouldBe a[Main.SuccessfulTerminationIR]
    }

    it should "handle struct imported several times" in {
        val path = pathFromBasename("struct/struct_imported_twice", "file3.wdl")
        val retval = Main.compile(path.toString :: cFlags)
        retval shouldBe a[Main.SuccessfulTerminationIR]
    }

    it should "handle file constants in a workflow" in {
        val path = pathFromBasename("compiler", "wf_constants.wdl")
        val retval = Main.compile(path.toString :: cFlags)
        retval shouldBe a[Main.SuccessfulTerminationIR]
    }

    it should "respect import flag" in {
        val path = pathFromBasename("compiler/imports", "A.wdl")
        val libraryPath = path.getParent.resolve("lib")
        val retval = Main.compile(path.toString :: "--imports" :: libraryPath.toString :: cFlags)
        retval shouldBe a[Main.SuccessfulTerminationIR]
    }

    it should "respect import -p flag" in {
        val path = pathFromBasename("compiler/imports", "A.wdl")
        val libraryPath = path.getParent.resolve("lib")
        val retval = Main.compile(path.toString :: "--p" :: libraryPath.toString :: cFlags)
        retval shouldBe a[Main.SuccessfulTerminationIR]
    }

    it should "pass environment between deep stages" in {
        val path = pathFromBasename("compiler", "environment_passing_deep_nesting.wdl")
        val retval = Main.compile(path.toString :: cFlags)
        retval shouldBe a[Main.SuccessfulTerminationIR]
    }

    it should "handle multiple struct definitions" in {
        val path = pathFromBasename("struct/DEVEX-1196-struct-resolution-wrong-order",
                                    "file3.wdl")
        val retval = Main.compile(path.toString :: cFlags)
        retval shouldBe a[Main.SuccessfulTerminationIR]
    }

    it should "retain all characters in a WDL task" in {
        val path = pathFromBasename("bugs", "missing_chars_in_task.wdl")
        val retval = Main.compile(path.toString
//                                      :: "--verbose"
//                                      :: "--verboseKey" :: "GenerateIR"
                                      :: cFlags)
        retval shouldBe a[Main.SuccessfulTerminationIR]

        val commandSection =
            """|  command {
               |  echo 1 hello world | sed 's/world/wdl/'
               |  echo 2 hello \
               |  world \
               |  | sed 's/world/wdl/'
               |  echo 3 hello \
               |  world | \
               |  sed 's/world/wdl/'
               |  }
               |""".stripMargin

        inside(retval) {
            case Main.SuccessfulTerminationIR(bundle) =>
                bundle.allCallables.size shouldBe(1)
                val (_, callable) = bundle.allCallables.head
                callable shouldBe a[IR.Applet]
                val task = callable.asInstanceOf[IR.Applet]
                task.womSourceCode should include (commandSection)
        }
    }

    it should "correctly flatten a workflow with imports" in {
        val path = pathFromBasename("compiler", "wf_to_flatten.wdl")
        val retval = Main.compile(path.toString :: cFlags)
        retval shouldBe a[Main.SuccessfulTerminationIR]
    }

    it should "detect a request for GPU" taggedAs(EdgeTest) in {
        val path = pathFromBasename("compiler", "GPU.wdl")
        val retval = Main.compile(path.toString
//                                      :: "--verbose"
//                                      :: "--verboseKey" :: "GenerateIR"
                                      :: cFlags)
        retval shouldBe a[Main.SuccessfulTerminationIR]

        inside(retval) {
            case Main.SuccessfulTerminationIR(bundle) =>
                bundle.allCallables.size shouldBe(1)
                val (_, callable) = bundle.allCallables.head
                callable shouldBe a[IR.Applet]
                val task = callable.asInstanceOf[IR.Applet]
                task.instanceType shouldBe(IR.InstanceTypeConst(Some("mem3_ssd1_gpu_x8"), None, None, None, None))
        }
    }

    // ignore for now as the test will fail in staging
    ignore should "Compile a workflow with a custom reorg applet" taggedAs(ProdTest) in {
        val path = pathFromBasename("compiler", basename="wf_custom_reorg.wdl")

        val extrasPath = pathFromBasename("compiler/extras", basename="extras_custom_reorg.json")

        val retval = Main.compile(
            path.toString :: "-extras" :: extrasPath.toString ::
              List("--compileMode", "ir", "--project", dxProject.getId)
        )

        retval shouldBe a [Main.SuccessfulTerminationIR]
        val bundle = retval match {
            case Main.SuccessfulTerminationIR(ir) => ir
            case _ => throw new Exception("sanity")
        }

        val wf: IR.Workflow =  bundle.primaryCallable.get match {
            case wf: IR.Workflow => wf
            case _ => throw new Exception("sanity")
        }

        wf.stages.size shouldBe(4)
        wf.stages(3).calleeName shouldBe "applet-Fg623fj0jy8q7jjv9xV6q5fQ"
    }

    // ignore for now as the test will fail in staging
    it should "Compile a workflow with subworkflows on the platform with the reorg app" taggedAs(EdgeTest) in {
        val path = pathFromBasename("subworkflows", basename="trains_station.wdl")
        val appletId = getAppletId("/unit_tests/applets/functional_reorg_test")
        val extrasContent =
            s"""|{
                | "custom_reorg" : {
                |    "app_id" : "${appletId}",
                |    "conf" : null
                |  }
                |}
                |""".stripMargin

        val tmpFile= createExtras(extrasContent)
        // remove locked workflow flag
        val retval = Main.compile(
            path.toString :: "-extras" :: tmpFile :: cFlagsReorg
        )
        retval shouldBe a [Main.SuccessfulTermination]
        val wfId: String = retval match {
            case Main.SuccessfulTermination(ir) => ir
            case _ => throw new Exception("sanity")
        }

        val (stdout, stderr) = Utils.execCommand(s"dx describe ${wfId} --json")

        val wfStages = stdout.parseJson.asJsObject.fields.get("stages") match {
            case Some(JsArray(x)) => x.toVector
            case other => throw new Exception(s"Unexpected result ${other}")
        }

        // there should be 4 stages: 1) common 2) train_stations 3) outputs 4) reorg
        wfStages.size shouldBe 4

        val reorgStage = wfStages.last

        // if its not a JsObject return empty string and test will fail
        val reorgDetails = reorgStage match {
            case JsObject(x) => JsObject(x)
            case _ => throw new Exception("sanity")
        }

        reorgDetails.getFields("id", "executable") shouldBe Seq(
            JsString("stage-reorg"), JsString(s"${appletId}")
        )
        // There should be 3 inputs, the output from output stage and the custom reorg config file.
        val reorgInput: JsObject = reorgDetails.fields("input") match {
            case JsObject(x) => JsObject(x)
            case _ => throw new Exception("sanity")
        }

        // no reorg conf input. only status.
        reorgInput.fields.size shouldBe 1
        reorgInput.fields.keys shouldBe Set(Utils.REORG_STATUS)
    }

    // ignore for now as the test will fail in staging
    it should "Compile a workflow with subworkflows on the platform with the reorg app with config file in the input" taggedAs(EdgeTest) in {
        // This works in conjunction with "Compile a workflow with subworkflows on the platform with the reorg app".
        val path = pathFromBasename("subworkflows", basename="trains_station.wdl")
        val appletId = getAppletId("/unit_tests/applets/functional_reorg_test")
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

        val tmpFile= createExtras(extrasContent)
        // remove locked workflow flag
        val retval = Main.compile(
            path.toString :: "-extras" :: tmpFile :: cFlagsReorg
        )

        retval shouldBe a [Main.SuccessfulTermination]
        val wfId: String = retval match {
            case Main.SuccessfulTermination(wfId) => wfId
            case _ => throw new Exception("sanity")
        }

        val (stdout, stderr) = Utils.execCommand(s"dx describe ${wfId} --json")

        val wfStages = stdout.parseJson.asJsObject.fields.get("stages") match {
            case Some(JsArray(x)) => x.toVector
            case other => throw new Exception(s"Unexpected result ${other}")
        }

        val reorgStage = wfStages.last

        val reorgDetails = reorgStage match {
            case JsObject(x) => JsObject(x)
            case _ => throw new Exception("sanity")
        }

        // There should be 3 inputs, the output from output stage and the custom reorg config file.
        val reorgInput: JsObject = reorgDetails.fields("input") match {
            case JsObject(x) => JsObject(x)
            case _ => throw new Exception("sanity")
        }
        // no reorg conf input. only status.
        reorgInput.fields.size shouldBe 2
        reorgInput.fields.keys shouldBe Set(Utils.REORG_STATUS, Utils.REORG_CONFIG)
    }

    it should "Checks subworkflow with custom reorg app do not contain reorg attribute" taggedAs(EdgeTest) in {
        // This works in conjunction with "Compile a workflow with subworkflows on the platform with the reorg app".
        val path = pathFromBasename("subworkflows", basename = "trains_station.wdl")
        val appletId = getAppletId("/unit_tests/applets/functional_reorg_test")
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
        // remove locked workflow flag
        val retval = Main.compile(
            path.toString :: "-extras" :: tmpFile :: "-compileMode" :: "IR" :: cFlagsReorg
        )
        retval shouldBe a[Main.SuccessfulTerminationIR]

        val bundle = retval match {
            case Main.SuccessfulTerminationIR(bundle) => bundle
            case _ => throw new Exception("sanity")
        }

        // this is a subworkflow so there is no reorg_status___ added.
        val trainsOutputVector: IR.Callable = bundle.allCallables("trains")
        trainsOutputVector.outputVars.size shouldBe 1
    }
}
