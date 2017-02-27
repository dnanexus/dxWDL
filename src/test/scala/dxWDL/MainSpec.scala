package dxWDL

import better.files._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import dxWDL.MainSpec._
import dxWDL.SampleWdl.{EmptyInvalid, EmptyTask, EmptyWorkflow, ThreeStep}
import net.jcazevedo.moultingyaml._
import net.jcazevedo.moultingyaml.DefaultYamlProtocol._
import wdl4s.{AstTools, WdlExpression, WdlNamespaceWithWorkflow}
import wdl4s.AstTools.EnhancedAstNode

// JSON parsing library
import spray.json._

class MainSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

    import Main._

    behavior of "Main"

    val threeStep = ThreeStep.wdlSource()

    it should "print usage" in {
        Main.dispatchCommand(Seq.empty[String]) shouldBe BadUsageTermination("")
    }

    it should "validate" in {
        testWdl(ThreeStep) { wdlAndInputs =>
            Main.dispatchCommand(Seq("validate", wdlAndInputs.wdl)) shouldBe SuccessfulTermination("")
        }
    }

    it should "not validate invalid wdl" in {
        testWdl(EmptyInvalid) { wdlAndInputs =>
            val res = Main.dispatchCommand(Seq("validate", wdlAndInputs.wdl))
            assert(res.isInstanceOf[UnsuccessfulTermination])
            res.output should include("Finished parsing without consuming all tokens")
        }
    }

    it should "parse" in {
        testWdl(ThreeStep) { wdlAndInputs =>
            val res = Main.dispatchCommand(Seq("parse", wdlAndInputs.wdl))
            assert(res.isInstanceOf[SuccessfulTermination])
            //res.output should include("(Document:")
        }
    }

    it should "return inputs" in {
        testWdl(ThreeStep) { wdlAndInputs =>
            val res = Main.dispatchCommand(Seq("inputs", wdlAndInputs.wdl))
            assert(res.isInstanceOf[SuccessfulTermination])
            res.output should include("\"three_step.cgrep.pattern\"")
        }
    }

    it should "not return inputs when there is no workflow" in {
        testWdl(EmptyTask) { wdlAndInputs =>
            val res = Main.dispatchCommand(Seq("inputs", wdlAndInputs.wdl))
            assert(res.isInstanceOf[UnsuccessfulTermination])
            res.output should include("does not have a local workflow")
        }
    }

    it should "produce YAML tree" in {
        testWdl(ThreeStep) { wdlAndInputs =>
            val expectedYaml =
                """
          |tasks:
          |- name: ps
          |  outputs:
          |  - type: File
          |    name: procs
          |    expression: stdout()
          |  declarations: []
          |  commandTemplate: ps
          |  runtime: {}
          |- name: cgrep
          |  outputs:
          |  - type: Int
          |    name: count
          |    expression: read_int(stdout())
          |  declarations:
          |  - type: String
          |    name: pattern
          |  - type: File
          |    name: in_file
          |  commandTemplate: grep '${pattern}' ${in_file} | wc -l
          |  runtime: {}
          |- name: wc
          |  outputs:
          |  - type: Int
          |    name: count
          |    expression: read_int(stdout())
          |  declarations:
          |  - type: File
          |    name: in_file
          |  commandTemplate: cat ${in_file} | wc -l
          |  runtime: {}
          |workflow:
          |  name: three_step
          |  declarations: []
          |  children:
          |  - call:
          |      name: ps
          |      task: ps
          |      inputMappings: {}
          |  - call:
          |      name: cgrep
          |      task: cgrep
          |      inputMappings:
          |        in_file: ps.procs
          |  - call:
          |      name: wc
          |      task: wc
          |      inputMappings:
          |        in_file: ps.procs
          """.stripMargin.trim
            val res = Main.dispatchCommand(Seq("yaml", wdlAndInputs.wdl))
            assert(res.isInstanceOf[SuccessfulTermination])
            res.output should include(expectedYaml)
        }
    }

    // NOTE:
    // We do not run compilation tests inside the unit-test framework,
    // because they perform platform calls. These tests can be invoked
    // from the top level run_tests.py script.
}

object MainSpec {
    /**
      * Tests running a sample wdl, providing the inputs, and cleaning up the temp files only if no exceptions occur.
      *
      * @param sampleWdl The sample wdl to run.
      * @param optionsJson Optional json for the options file.
      * @param block The block provided the inputs, returning some value.
      * @tparam T The return type of the block.
      * @return The result of running the block.
      */
    def testWdl[T](sampleWdl: SampleWdl, optionsJson: String = "{}")(block: WdlAndInputs => T): T = {
        val wdlAndInputs = WdlAndInputs(sampleWdl, optionsJson)
        val result = block(wdlAndInputs)
        wdlAndInputs.deleteTempFiles()
        result
    }

    /**
      * Create a temporary wdl file and inputs for the sampleWdl.
      * When the various properties are lazily accessed, they are also registered for deletion after the suite completes.
    */
  case class WdlAndInputs(sampleWdl: SampleWdl, optionsJson: String = "{}") {
    // Track all the temporary files we create, and delete them after the test.
    private var tempFiles = Vector.empty[File]

    lazy val wdlFile = {
      val path = File.newTemporaryFile(s"${sampleWdl.name}.", ".wdl")
      tempFiles :+= path
      path write sampleWdl.wdlSource("")
      path
    }

    lazy val wdl = wdlFile.pathAsString

    lazy val inputsFile = {
      val path = swapExt(wdlFile, ".wdl", ".inputs")
      tempFiles :+= path
      path write sampleWdl.wdlJson
      path
    }

    lazy val inputs = inputsFile.pathAsString

    lazy val optionsFile = {
      val path = swapExt(wdlFile, ".wdl", ".options")
      tempFiles :+= path
      path write optionsJson
      path
    }

    lazy val options = optionsFile.pathAsString

    lazy val metadataFile = {
      val path = swapExt(wdlFile, ".wdl", ".metadata.json")
      tempFiles :+= path
      path
    }

    lazy val metadata = metadataFile.pathAsString

    def deleteTempFiles() = tempFiles.foreach(_.delete(swallowIOExceptions = true))
  }

  def swapExt(filePath: File, oldExt: String, newExt: String): File = {
    File(filePath.toString.stripSuffix(oldExt) + newExt)
  }
}
