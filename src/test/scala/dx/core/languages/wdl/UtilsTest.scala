package dx.core.languages.wdl

import dx.Tags.EdgeTest
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import wdlTools.types.{WdlTypes, TypedAbstractSyntax => TAT}
import wdlTools.util.{Bindings, FileSourceResolver}

class UtilsTest extends AnyFlatSpec with Matchers {
  private def validateTaskMeta(task: TAT.Task): Unit = {
    val kvs = task.meta match {
      case Some(TAT.MetaSection(kvs, _)) => kvs
      case _                             => throw new Exception("unexpected")
    }
    kvs.get("type") should matchPattern {
      case Some(TAT.MetaValueString("native", _)) =>
    }
    kvs.get("id") should matchPattern {
      case Some(TAT.MetaValueString("applet-xxxx", _)) =>
    }
  }

  private def parseAndCheckSingleTask(
      sourceCode: String,
      fileResolver: FileSourceResolver = FileSourceResolver.get
  ): (TAT.Task, Bindings[String, WdlTypes.T_Struct], TAT.Document) = {
    val (doc, typeAliases) = WdlUtils.parseAndCheckSourceString(sourceCode, fileResolver)
    if (doc.workflow.isDefined) {
      throw new Exception("a workflow shouldn't be a member of this document")
    }
    val tasks = doc.elements.collect {
      case task: TAT.Task => task.name -> task
    }.toMap
    if (tasks.isEmpty) {
      throw new Exception("no tasks in this WDL program")
    }
    if (tasks.size > 1) {
      throw new Exception("More than one task in this WDL program")
    }
    (tasks.values.head, typeAliases, doc)
  }

  it should "parse the meta section in wdl draft2" in {
    val srcCode =
      """|task native_sum_012 {
         |  Int? a
         |  Int? b
         |  command {}
         |  output {
         |    Int result = 0
         |  }
         |  meta {
         |     type : "native"
         |     id : "applet-xxxx"
         |  }
         |}
         |
         |""".stripMargin

    val (task, _, _) = parseAndCheckSingleTask(srcCode)
    validateTaskMeta(task)
  }

  it should "parse the meta section in wdl 1.0" in {
    val srcCode =
      """|version 1.0
         |
         |task native_sum_012 {
         |  input {
         |    Int? a
         |    Int? b
         |  }
         |  command {}
         |  output {
         |    Int result = 0
         |  }
         |  meta {
         |     type : "native"
         |     id : "applet-xxxx"
         |  }
         |}
         |
         |""".stripMargin

    val (task, _, _) = parseAndCheckSingleTask(srcCode)
    validateTaskMeta(task)
  }

  ignore should "parse the meta section in wdl 2.0" taggedAs EdgeTest in {
    val srcCode =
      """|version 2.0
         |
         |task add {
         |  input {
         |    Int a
         |    Int b
         |  }
         |  command {}
         |  output {
         |    Int result = a + b
         |  }
         |  meta {
         |     type : "native"
         |     id : "applet-xxxx"
         |  }
         |}
         |
         |""".stripMargin

    val (task, _, _) = parseAndCheckSingleTask(srcCode)
    validateTaskMeta(task)
  }
}
