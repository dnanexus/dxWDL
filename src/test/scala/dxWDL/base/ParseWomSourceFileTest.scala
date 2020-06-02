package dxWDL.base

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.Inside._
import wdlTools.types.{TypedAbstractSyntax => TAT}

// These tests involve compilation -without- access to the platform.
//
class ParseWomSourceFileTest extends AnyFlatSpec with Matchers {
  private val parseWomSourceFile = ParseWomSourceFile(false)

  private def normalize(s: String): String = {
    s.replaceAll("(?s)\\s+", " ").trim
  }

  it should "find task sources" in {
    val srcCode =
      """|task hello {
         |   Milo is selling the mess hall chairs!
         |}
         |""".stripMargin

    val taskDir = parseWomSourceFile.scanForTasks(srcCode)
    taskDir.size should equal(1)
    val helloTask = taskDir.get("hello")
    inside(helloTask) {
      case Some(x) =>
        normalize(x) should equal(normalize(srcCode))
    }
  }

  it should "find task source in complex WDL task" in {
    val srcCode =
      """|task sub {
         |   Int a
         |   Int b
         |   command {
         |      ls -lR
         |   }
         |}
         |""".stripMargin

    val taskDir = parseWomSourceFile.scanForTasks(srcCode)
    taskDir.size should equal(1)
    val subTask = taskDir.get("sub")
    inside(subTask) {
      case Some(x) =>
        normalize(x) should equal(normalize(srcCode))
    }
  }

  it should "find sources in a script with two tasks" in {
    val srcCode =
      """|task sub {
         |   Int a
         |   Int b
         |   command {
         |      ls -lR
         |   }
         |}
         |
         |task major {
         |   major major is up on the tree
         |   { }
         |}
         |""".stripMargin

    val taskDir = parseWomSourceFile.scanForTasks(srcCode)
    taskDir.size should equal(2)
    inside(taskDir.get("sub")) {
      case Some(x) =>
        x should include("ls -lR")
    }
    inside(taskDir.get("major")) {
      case Some(x) =>
        x should include("tree")
        x should include("{ }")
    }
  }

  it should "find source task in a WDL 1.0 script" in {
    val srcCode =
      """|version 1.0
         |
         |task Add {
         |    input {
         |        Int a
         |        Int b
         |    }
         |    command {
         |        echo $((${a} + ${b}))
         |    }
         |    output {
         |        Int result = read_int(stdout())
         |    }
         |}
         |""".stripMargin

    val taskDir = parseWomSourceFile.scanForTasks(srcCode)
    taskDir.size should equal(1)
    inside(taskDir.get("Add")) {
      case Some(x) =>
        x should include("echo $((${a} + ${b}))")
    }
  }

  private def validateTaskMeta(task: TAT.Task): Unit = {
    val kvs = task.meta match {
      case Some(TAT.MetaSection(kvs, _)) => kvs
      case _                             => throw new Exception("sanity")
    }
    kvs.get("type") should matchPattern {
      case Some(TAT.MetaValueString("native", _)) =>
    }
    kvs.get("id") should matchPattern {
      case Some(TAT.MetaValueString("applet-xxxx", _)) =>
    }
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

    val (task: TAT.Task, _, _) = parseWomSourceFile.parseWdlTask(srcCode)
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

    val (task: TAT.Task, _, _) = parseWomSourceFile.parseWdlTask(srcCode)
    validateTaskMeta(task)
  }

  // The scanForTasks method takes apart the source WOM code, and then puts
  // it back together. Check that it isn't discarding the pipe characters ('|'),
  // or anything else.
  it should "not omit symbols in the command section" in {
    val srcCode =
      """|task echo_line_split {
         |
         |  command {
         |  echo 1 hello world | sed 's/world/wdl/'
         |  echo 2 hello \
         |  world \
         |  | sed 's/world/wdl/'
         |  echo 3 hello \
         |  world | \
         |  sed 's/world/wdl/'
         |  }
         |}""".stripMargin

    val taskDir = parseWomSourceFile.scanForTasks(srcCode)
    taskDir.size should equal(1)
    val taskSourceCode: String = taskDir.values.head
    taskSourceCode shouldBe (srcCode)
  }

  ignore should "parse the meta section in wdl 2.0" taggedAs (EdgeTest) in {
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

    val (task: TAT.Task, _, _) = parseWomSourceFile.parseWdlTask(srcCode)
    validateTaskMeta(task)
  }
}
