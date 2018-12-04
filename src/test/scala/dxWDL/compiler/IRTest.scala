package dxWDL.compiler

import dxWDL.Main
import java.nio.file.{Path, Paths}
import org.scalatest.{FlatSpec, Matchers}


// These tests involve compilation -without- access to the platform.
//
class IRTest extends FlatSpec with Matchers {
    lazy val currentWorkDir:Path = Paths.get(System.getProperty("user.dir"))

    private def pathFromBasename(basename: String) : Path = {
        currentWorkDir.resolve(s"src/test/resources/compiler/${basename}")
    }

    // task compilation
    /*
    it should "IR compile a single WDL task" in {
        val path = pathFromBasename("add.wdl")
        Main.compile(
            List(path.toString, "--compileMode", "ir", "-quiet", "-fatalValidationWarnings")
        ) shouldBe a [Main.SuccessfulTerminationIR]
    }
     */

    // workflow compilation
    it should "IR compile a simple WDL workflow" in {
        val path = pathFromBasename("wf_two_inputs.wdl")
        Main.compile(
            List(path.toString, "--compileMode", "ir", "-quiet", "-fatalValidationWarnings")
        ) shouldBe a [Main.SuccessfulTerminationIR]
    }

/*    it should "disallow call with missing compulsory arguments" in {
        val path = pathFromBasename("unbound_arg.wdl")
        val retval = Main.compile(
            List(path.toString, "--compileMode", "ir", "-quiet", "-fatalValidationWarnings")
        )
        inside(retval) {
            case Main.UnsuccessfulTermination(errMsg) =>
                errMsg should include ("Namespace validation error")
        }
    }

    it should "allow unbound arguments in a subworkflow" in {
        val path = pathFromBasename("toplevel_unbound_arg.wdl")
        Main.compile(
            List(path.toString, "--compileMode", "ir", "-quiet")
        ) shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "Handle array access" in {
        val path = pathFromBasename("file_array.wdl")
        val retval = Main.compile(
            List(path.toString, "--compileMode", "ir", "--locked")
        )
        retval shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "Accept weird  call names" in {
        val path = pathFromBasename("weird_call_name.wdl")
        Main.compile(
            List(path.toString, "--compileMode", "ir", "--locked", "--quiet")
        ) shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "Allow using the same import name twice" in {
        val path = pathFromBasename("three_levels/top.wdl")
        Main.compile(
            List(path.toString, "--compileMode", "ir", "--locked", "--quiet")
        ) shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "handle closures across code blocks" in {
        val path = pathFromBasename("closure1.wdl")
        Main.compile(
            List(path.toString, "--compileMode", "ir", "--locked", "--quiet")
        ) shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "handle a call closure " in {
        val path = pathFromBasename("closure2.wdl")
        Main.compile(
            List(path.toString, "--compileMode", "ir", "--locked", "--quiet")
        ) shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "handle weird tasks " in {
        val path = pathFromBasename("task_bug.wdl")
        Main.compile(
            List(path.toString, "--compileMode", "ir", "--locked", "--quiet")
        ) shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "pass workflow input to task call" in {
        val path = pathFromBasename("closure3.wdl")
        Main.compile(
            List(path.toString, "--compileMode", "ir", "--locked", "--quiet")
        ) shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "handle pair left/right" in {
        val path = pathFromBasename("pairs.wdl")
        Main.compile(
            List(path.toString, "--compileMode", "ir", "--locked", "--quiet")
        ) shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "handle stdlib functions used as variable names" in {
        val path = pathFromBasename("stdlib_variables.wdl")
        Main.compile(
            List(path.toString, "--compileMode", "ir", "--locked", "--quiet")
        ) shouldBe a [Main.SuccessfulTerminationIR]
    }

    it should "allow last to be used in a variable name" in {
        val path = pathFromBasename("last.wdl")
        Main.compile(
            List(path.toString, "--compileMode", "ir", "--locked", "--quiet")
        ) shouldBe a [Main.SuccessfulTerminationIR]
    }
 */

    // Specific dxWDL extensions. These will be supported later.
    /*
    it should "respect variables passed through the extras mechanisms" in {
        val extraOptions =
            """|{
               |   "default_runtime_attributes" : {
               |      "docker" : "quay.io/encode-dcc/atac-seq-pipeline:v1"
               |   }
               |}""".stripMargin
        val extrasFile:Path = Paths.get("/tmp/extraOptions.json")
        Utils.writeFileContent(extrasFile, extraOptions)

        val path = pathFromBasename("extras.wdl")
        val retval = Main.compile(
            List(path.toString, "--compileMode", "ir", "--locked", "--quiet",
                 "--extras", extrasFile.toString)
        )
        retval shouldBe a [Main.SuccessfulTerminationIR]

        // verify that the intermediate code includes the correct docker image
        val ir: dxWDL.compiler.IR.Namespace = retval match {
            case Main.SuccessfulTerminationIR(x) => x
            case _ => throw new Exception("sanity")
        }
        val (_,mulApl) = ir.applets.head
        mulApl.docker should equal(dxWDL.compiler.IR.DockerImageNetwork)


        // Verify that if we -do not- give the extra options, there is no
        // docker image set.
        val retval2 = Main.compile(
            List(path.toString, "--compileMode", "ir", "--locked", "--quiet")
        )
        retval2 shouldBe a [Main.SuccessfulTerminationIR]
        val ir2: dxWDL.compiler.IR.Namespace = retval2 match {
            case Main.SuccessfulTerminationIR(x) => x
            case _ => throw new Exception("sanity")
        }
        val (_,mulApl2) = ir2.applets.head
        mulApl2.docker should equal(dxWDL.compiler.IR.DockerImageNone)
    }

    it should "insist on an empty runtime block for native calls" in {
        val path = pathFromBasename("native_call.wdl")
        val retval = Main.compile(
            List(path.toString, "--compileMode", "ir", "--locked", "--quiet")
        )
        inside(retval) {
            case Main.UnsuccessfulTermination(errMsg) =>
                errMsg should include ("empty runtime section")
                errMsg should include ("native task")
        }
    } */
}
