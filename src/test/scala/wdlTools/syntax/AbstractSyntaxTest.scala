package wdlTools.syntax

import AbstractSyntax._
import java.nio.file.Paths

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import wdlTools.syntax.v1.ParseAll
import wdlTools.util.{Options, SourceCode, Util, Verbosity}

class AbstractSyntaxTest extends AnyFlatSpec with Matchers {
  private val tasksDir = Paths.get(getClass.getResource("/wdlTools/syntax/v1/tasks").getPath)
  private val workflowsDir = Paths.get(getClass.getResource("/wdlTools/syntax/v1/workflows").getPath)
  private val opts =
    Options(antlr4Trace = false,
            localDirectories = Vector(tasksDir, workflowsDir),
            verbosity = Verbosity.Quiet)
  private val parser = ParseAll(opts)

  private def getTaskSource(fname: String): SourceCode = {
    SourceCode.loadFrom(Util.pathToUrl(tasksDir.resolve(fname)))
  }

  private def getWorkflowSource(fname: String): SourceCode = {
    SourceCode.loadFrom(Util.pathToUrl(workflowsDir.resolve(fname)))
  }

  it should "handle import statements" in {
    val doc = parser.parseDocument(getWorkflowSource("imports.wdl"))

    doc.version.value shouldBe WdlVersion.V1

    val imports = doc.elements.collect {
      case x: ImportDoc => x
    }
    imports.size shouldBe 2

    doc.workflow should not be empty
  }

  it should "handle optionals" in {
    val doc = parser.parseDocument(getTaskSource("missing_type_bug.wdl"))
    doc.version.value shouldBe WdlVersion.V1
  }

  it should "parse GATK tasks" in {
    val url =
      "https://raw.githubusercontent.com/gatk-workflows/gatk4-germline-snps-indels/master/tasks/JointGenotypingTasks-terra.wdl"
    val sourceCode = SourceCode.loadFrom(Util.getUrl(url))
    val doc = parser.parseDocument(sourceCode)

    doc.version.value shouldBe WdlVersion.V1
  }

  it should "parse GATK joint genotyping workflow" taggedAs Edge in {
    val url =
      "https://raw.githubusercontent.com/gatk-workflows/gatk4-germline-snps-indels/master/JointGenotyping-terra.wdl"
    val sourceCode = SourceCode.loadFrom(Util.getUrl(url))
    val doc = parser.parseDocument(sourceCode)

    doc.version.value shouldBe WdlVersion.V1
  }

  it should "handle the meta section" taggedAs Edge in {
    val doc = parser.parseDocument(getTaskSource("meta_null_value.wdl"))
    doc.version.value shouldBe WdlVersion.V1
    doc.elements.size shouldBe 1
    val task = doc.elements.head.asInstanceOf[Task]

    task.parameterMeta.get shouldBe a[ParameterMetaSection]
    task.parameterMeta.get.kvs.size shouldBe 1
    val mpkv = task.parameterMeta.get.kvs.head
    mpkv should matchPattern {
      case MetaKV("i", ExprIdentifier("null", _), _) =>
    }
  }

  it should "complex meta values" taggedAs Edge in {
    val doc = parser.parseDocument(getTaskSource("meta_section_compound.wdl"))
    doc.version.value shouldBe WdlVersion.V1
    doc.elements.size shouldBe 1
    val task = doc.elements.head.asInstanceOf[Task]

    task.parameterMeta.get shouldBe a[ParameterMetaSection]
    task.parameterMeta.get.kvs.size shouldBe 3
  }

  it should "report errors in meta section" taggedAs Edge in {
    assertThrows[SyntaxException] {
      parser.parseDocument(getTaskSource("meta_section_error.wdl"))
    }
  }

  it should "report duplicate key in runtime section " taggedAs Edge in {
    assertThrows[SyntaxException] {
      parser.parseDocument(getTaskSource("runtime_section_duplicate_key.wdl"))
    }
  }
}
