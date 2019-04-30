package dxWDL.compiler

import java.nio.file.{Path, Paths}
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.Inside._
import wom.callable.{CallableTaskDefinition, MetaValueElement}

import dxWDL.Main
import dxWDL.util.Utils

// These tests involve compilation -without- access to the platform.
//
class GenerateIRTest extends FlatSpec with Matchers {
    private def pathFromBasename(dir: String, basename: String) : Path = {
        val p = getClass.getResource(s"/${dir}/${basename}").getPath
        Paths.get(p)
    }

    // task compilation
    private val cFlags = List("--compileMode", "ir", "-quiet", "-fatalValidationWarnings",
                              "--locked")
    private val cFlagsUnlocked = List("--compileMode", "ir", "-quiet", "-fatalValidationWarnings")
    val dbgFlags = List("--compileMode", "ir",
                        "--verbose",
                        "--verboseKey", "GenerateIR",
                        "--locked")

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

    ignore should "compile scatters over maps --- doesn't work in cromwell v37,v38" in {
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
            path.toString :: List("--compileMode", "ir", "--quiet")
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
        inside(retval) {
            case Main.UnsuccessfulTermination(errMsg) =>
                errMsg should include ("nested scatter")
        }
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
        cgrepTask.parameterMeta shouldBe (Map("in_file" -> "stream"))
        val iDef = cgrepTask.inputs.find(_.name == "in_file").get
        iDef.parameterMeta shouldBe (Some(MetaValueElement.MetaValueElementString("stream")))

        val diffTask = getTaskByName("diff", bundle)
        diffTask.parameterMeta shouldBe (Map("a" -> "stream", "b" -> "stream"))
    }

    it should "emit warning for streaming on non files I" in {
        val path = pathFromBasename("compiler", "streaming_files_error1.wdl")
        val retval = Main.compile(
            path.toString :: cFlags
        )
        inside(retval) {
            case Main.UnsuccessfulTermination(errMsg) =>
                errMsg should include ("Only files that are task inputs can be declared streaming")
        }
    }

    it should "emit warning for streaming on non files II" in {
        val path = pathFromBasename("compiler", "streaming_files_error2.wdl")
        val retval = Main.compile(
            path.toString :: cFlags
        )
        inside(retval) {
            case Main.UnsuccessfulTermination(errMsg) =>
                errMsg should include ("Only files that are task inputs can be declared streaming")
        }
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
        diffTask.parameterMeta shouldBe (Map("a" -> "stream", "b" -> "stream"))

        // This doesn't work on draft2
        //val iDef = diffTask.inputs.find(_.name == "a").get
        //iDef.parameterMeta shouldBe (Some(MetaValueElement.MetaValueElementString("stream")))
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

    it should "check for reserved symbols" taggedAs(EdgeTest) in {
        val path = pathFromBasename("compiler", "reserved.wdl")
        val retval = Main.compile(path.toString :: cFlags)

        inside(retval) {
            case Main.UnsuccessfulTermination(errMsg) =>
                errMsg should include ("reserved substring ___")
        }
    }

    ignore should "do nested scatters" in {
        val path = pathFromBasename("compiler", "nested_scatter.wdl")
        val retval = Main.compile(path.toString :: cFlags)
        retval shouldBe a[Main.SuccessfulTerminationIR]
    }
}
