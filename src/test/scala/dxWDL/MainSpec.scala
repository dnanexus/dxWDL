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
