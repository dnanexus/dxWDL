package dxWDL

import java.nio.charset.StandardCharsets
import java.nio.file.{Path, Paths, Files}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, OneInstancePerTest}
import scala.sys.process._
import spray.json._
import spray.json.DefaultJsonProtocol
import wdl4s.{AstTools, Call, Task, WdlExpression, WdlNamespace, WdlNamespaceWithWorkflow, Workflow}
import wdl4s.AstTools.EnhancedAstNode
import wdl4s.types._
import wdl4s.values._


class Wdl4sTest extends FlatSpec with BeforeAndAfterEach with OneInstancePerTest {

    override def beforeEach() = {
        val metaDir = Utils.getMetaDirPath()
        Utils.deleteRecursive(metaDir.toFile)
    }


    // Look for a call inside a namespace.
    private def getCallFromNamespace(ns : WdlNamespaceWithWorkflow, callName : String ) : Call = {
        val wf: Workflow = ns.workflow
        wf.findCallByName(callName) match {
            case None => throw new AppInternalException(s"Call ${callName} not found in WDL file")
            case Some(call) => call
        }
    }

    // Run a child process and, wait for it to complete, and return the exit code
    private def execBashScript(scriptFile : Path) : Int = {
        val cmds = Seq("/bin/bash", scriptFile.toString())
        val p : Process = Process(cmds).run()
        p.exitValue()
    }

    // Test the heart of the applet run methods
    private def evalCall(call: Call, callInputs: Map[String, WdlValue], goodRetcode: Boolean = true)
            : Seq[(String, WdlType, WdlValue)] = {
        // Clean up the task subdirectory
        val metaDir = Utils.getMetaDirPath()
        val task = Utils.taskOfCall(call)
        RunnerTask.prologCore(task, callInputs)
        val scriptPath = metaDir.resolve("script")
        execBashScript(scriptPath)

        // extract the return code
        val rcPath = metaDir.resolve("rc")
        val rc = Utils.readFileContent(rcPath).trim.toInt
        if (goodRetcode)
            assert(rc == 0)
        else
            assert(rc != 0)
        RunnerTask.epilogCore(task)
    }


    // Fails with wdl4s v0.6, because
    //   '5 * (1 + 1)'  is converted into '5 * 1 + 1'
    //
    it should "serialize expressions without loss" in {
        val s = "5 * (1 + 1)"
        val expr : WdlExpression = WdlExpression.fromString(s)
        val s2 = expr.toWdlString
        assert(s == s2)

    }

    it should "retrieve source code for task" in {
        val wdl = """|task a {
                     |  String prefix
                     |  Array[Int] ints
                     |  command {
                     |    python script.py ${write_lines(ints)} > ${prefix + ".out"}
                     |  }
                     |}
                     |workflow wf {
                     |  call a
                     |}""".stripMargin.trim


        val ns = WdlNamespaceWithWorkflow.load(wdl, Seq.empty).get
        ns.findTask("a") foreach { task =>
            assert(task.name == "a")
        }

        /* Traverse the tree to find all Task definitions */
        AstTools.findAsts(ns.ast, "Task") foreach {ast =>
            assert("a" == ast.getAttribute("name").sourceString)
        }

        // doesn't work, toWdlString is not implemented for values
        //System.out.println(s"ns=${ns.toWdlString}")
    }

    it should "evaluate simple calls" in {
        val wdl = """|task Add {
                     |  Int a
                     |  Int b
                     |
                     |  command {
                     |  }
                     |  output {
                     |    Int sum = a + b
                     |  }
                     |}
                     |
                     |workflow call_expr_deps {
                     |     call Add {
                     |          input: a = 3, b = 2
                     |     }
                     |     call Add as Add2 {
                     |          input: a = 2 * Add.sum, b = 3
                     |     }
                     |     output {
                     |         Add2.sum
                     |     }
                     |}""".stripMargin.trim


        val ns = WdlNamespaceWithWorkflow.load(wdl, Seq.empty).get
        val call : Call = getCallFromNamespace(ns, "Add2")
        val inputs = Map("a" -> WdlInteger(2),
                         "b" -> WdlInteger(3))
        val outputs : Seq[(String, WdlType, WdlValue)] = evalCall(call, inputs)
        assert(outputs.length == 1);
        val result = outputs.head
        assert(result == ("sum",WdlIntegerType,WdlInteger(5)))
    }

    it should "evaluate calls with a string array" in {
        val wdl = """|task Concat {
                     |    Array[String] words
                     |
                     |    command {
                     |        echo ${sep=' INPUT=' words}
                     |    }
                     |    output {
                     |      String result = read_string(stdout())
                     |    }
                     |}
                     |
                     |workflow ar1 {
                     |    Array[String] str_array
                     |
                     |    call Concat {
                     |        input : words=str_array
                     |    }
                     |    output {
                     |        Concat.result
                     |    }
                     |}""".stripMargin.trim

        val ns = WdlNamespaceWithWorkflow.load(wdl, Seq.empty).get
        val call : Call = getCallFromNamespace(ns, "Concat")
        val inputs : Map[String,WdlValue] =
            Map("words" ->
                    WdlArray(WdlArrayType(WdlStringType),
                             List(WdlString("a"), WdlString("b"), WdlString("c"))))

        val outputs : Seq[(String, WdlType, WdlValue)] = evalCall(call, inputs)
        assert(outputs.length == 1)
        // FIXME
        assert(outputs.head == ("result",WdlStringType,WdlString("a INPUT=b INPUT=c")) ||
                   outputs.head == ("result",WdlStringType,WdlString("")))
    }

    it should "evaluate calls with an int array" in {
        val wdl = """|task Concat {
                     |    Array[Int] words
                     |
                     |    command {
                     |        echo ${sep=' I=' words}
                     |    }
                     |    output {
                     |      String result = read_string(stdout())
                     |    }
                     |}
                     |
                     |workflow ar1 {
                     |    Array[Int] int_array
                     |
                     |    call Concat {
                     |        input : words=int_array
                     |    }
                     |    output {
                     |        Concat.result
                     |    }
                     |}""".stripMargin.trim
        val ns = WdlNamespaceWithWorkflow.load(wdl, Seq.empty).get
        val call : Call = getCallFromNamespace(ns, "Concat")
        val inputs : Map[String,WdlValue] =
            Map("words" ->
                    WdlArray(WdlArrayType(WdlIntegerType),
                             List(WdlInteger(1), WdlInteger(2), WdlInteger(3))))
        val outputs : Seq[(String, WdlType, WdlValue)] = evalCall(call, inputs)
        assert(outputs.length == 1)
        // FIXME
        assert(outputs.head == ("result",WdlStringType,WdlString("1 I=2 I=3")) ||
                   outputs.head == ("result",WdlStringType,WdlString("")))
    }

    // Create a file from a string
    def writeStringToFile(path : String, str : String) : Unit = {
        Files.deleteIfExists(Paths.get(path))
        Files.write(Paths.get(path), str.getBytes(StandardCharsets.UTF_8))
    }

    it should "evaluate calls with a file array" in {
        val wdl = """|task wc {
                     |    Array[File] files
                     |
                     |    command {
                     |        wc -l ${sep=' ' files}
                     |    }
                     |    output {
                     |        String result = read_string(stdout())
                     |    }
                     | }
                     |
                     | workflow files1 {
                     |     Array[File] fs
                     |
                     |     call wc {
                     |         input : files=fs
                     |     }
                     |     output {
                     |         wc.result
                     |     }
                     |}""".stripMargin.trim
        val ns = WdlNamespaceWithWorkflow.load(wdl, Seq.empty).get
        val call : Call = getCallFromNamespace(ns, "wc")

        // create a few files
        List("X", "Y", "Z").foreach{ case fName =>
            val path = "/tmp/" ++ fName ++ ".txt"
            writeStringToFile(path, "ABCFD12344")
        }

        val inputs : Map[String,WdlValue] =
            Map("files" -> WdlArray(WdlArrayType(WdlFileType),
                                 List(WdlSingleFile("/tmp/X.txt"),
                                      WdlSingleFile("/tmp/Y.txt"),
                                      WdlSingleFile("/tmp/Z.txt"))))
        val outputs : Seq[(String, WdlType, WdlValue)] = evalCall(call, inputs)
        assert(outputs.length == 1)
        val (fieldName, wdlType, v) = outputs.head
        assert(fieldName == "result")
        assert(wdlType == WdlStringType)
    }

    it should "evaluate calls with primitive array types" in {
        val wdl = """|task Concat {
                     |    Array[Int] ia
                     |    Array[Float] fa
                     |    Array[Boolean] ba
                     |    Array[String] sa
                     |
                     |    command <<<
                     |        echo ${sep=' I=' ia}
                     |        echo ${sep=' F=' fa}
                     |        echo ${sep=' B=' ba}
                     |        echo ${sep=' S=' sa}
                     |    >>>
                     |    output {
                     |        String result = read_string(stdout())
                     |    }
                     | }
                     |
                     | workflow files1 {
                     |    Array[Int] x_ia
                     |    Array[Float] x_fa
                     |    Array[Boolean] x_ba
                     |    Array[String] x_sa
                     |
                     |     call Concat {
                     |         input : ia=x_ia, fa=x_fa, ba=x_ba, sa=x_sa
                     |     }
                     |     output {
                     |         Concat.result
                     |     }
                     |}""".stripMargin.trim
        val ns = WdlNamespaceWithWorkflow.load(wdl, Seq.empty).get
        val call : Call = getCallFromNamespace(ns, "Concat")

        val inputs : Map[String,WdlValue] =
            Map("ia" ->
                    WdlArray(WdlArrayType(WdlIntegerType),
                             List(WdlInteger(1), WdlInteger(2), WdlInteger(3))),
                "fa" ->
                    WdlArray(WdlArrayType(WdlFloatType),
                             List(WdlFloat(1.4), WdlFloat(3.14), WdlFloat(1.618))),
                "ba" ->
                    WdlArray(WdlArrayType(WdlBooleanType),
                             List(WdlBoolean(false), WdlBoolean(true), WdlBoolean(true))),
                "sa" ->
                    WdlArray(WdlArrayType(WdlStringType),
                             List(WdlString("hello"), WdlString("Mrs"), WdlString("Robinson"))))

        val outputs : Seq[(String, WdlType, WdlValue)] = evalCall(call, inputs)
        assert(outputs.length == 1)
        val (fieldName, wdlType, v) = outputs.head
        assert(fieldName == "result")
        assert(wdlType == WdlStringType)

        // FIXME
        assert(v == WdlString("""|1 I=2 I=3
                                 |1.4 F=3.14 F=1.618
                                 |false B=true B=true
                                 |hello S=Mrs S=Robinson
                                 |""".stripMargin.trim) ||
                   v == WdlString(""))
    }

    it should "handle output arrays" in {
        val wdl = """|task prepare {
                     |    command <<<
                     |    python -c "print('one\ntwo\nthree\nfour')"
                     |    >>>
                     |    output {
                     |        Array[String] array = read_lines(stdout())
                     |    }
                     |}
                     |workflow sg1 {
                     |    call prepare
                     |    output {
                     |        prepare.array
                     |    }
                     |}""".stripMargin.trim

        val ns = WdlNamespaceWithWorkflow.load(wdl, Seq.empty).get
        val call : Call = getCallFromNamespace(ns, "prepare")
        val inputs : Map[String,WdlValue] = Map()
        val outputs : Seq[(String, WdlType, WdlValue)] = evalCall(call, inputs)
        assert(outputs.length == 1)
        val (fieldName, wdlType, v) = outputs.head
        assert(fieldName == "array")
        assert(wdlType == WdlArrayType(WdlStringType))
        assert(v ==
                   WdlArray(
                       WdlArrayType(WdlStringType),
                       List(WdlString("one"), WdlString("two"), WdlString("three"), WdlString("four"))) ||
                   v == WdlArray(WdlArrayType(WdlStringType), List(WdlString(""))))
    }

    it should "exit with an error code for a bad shell command" in {
        // We want to test all combinations of the following:
        //  shell command |  docker
        // --------------------------------
        //  success       |  success
        //  failure       |  failure
        //
        // For example, what happens if the shell command succeeds, but docker
        // fails. We want to see an error code in that case.
        //
        // the docker tests have separate wdl files, because they
        // require a dx-docker installation
        {
            val wdl = """|task BadCommand {
                         |  command {
                         |     ls /xx/yyy
                         |  }
                         |  output {
                         |    Int rc = 1
                         |  }
                         |}
                         |
                         |workflow call_expr_deps {
                         |  call BadCommand
                         |  output {
                         |     BadCommand.rc
                         |  }
                         |}""".stripMargin.trim

            val ns = WdlNamespaceWithWorkflow.load(wdl, Seq.empty).get
            val call : Call = getCallFromNamespace(ns, "BadCommand")
            evalCall(call, Map(), goodRetcode=false)
        }

        {
            val wdl = """|task GoodCommand {
                         |  command {
                         |     echo "hello world"
                         |  }
                         |  output {
                         |    String out = read_string(stdout())
                         |  }
                         |}
                         |
                         |workflow call_expr_deps {
                         |  call GoodCommand
                         |  output {
                         |     GoodCommand.out
                         |  }
                         |}""".stripMargin.trim

            val ns = WdlNamespaceWithWorkflow.load(wdl, Seq.empty).get
            val call : Call = getCallFromNamespace(ns, "GoodCommand")
            val outputs : Seq[(String, WdlType, WdlValue)] = evalCall(call, Map())
            assert(outputs.length == 1)
            val (fieldName, wdlType, outstr) = outputs.head
            assert(fieldName == "out")
            assert(wdlType == WdlStringType)
        }
    }

    ignore should "Allow adding unbound argument" in {
        val wdl = """|task mul2 {
                     |    Int i
                     |
                     |    command {
                     |        python -c "print(${i} + ${i})"
                     |    }
                     |    output {
                     |        Int result = read_int(stdout())
                     |    }
                     |}
                     |
                     |workflow optionals {
                     |    Int arg1
                     |
                     |    # A call missing a compulsory argument
                     |    call mul2
                     |    output {
                     |        mul2.result
                     |    }
                     |}""".stripMargin.trim

        val ns = WdlNamespaceWithWorkflow.load(wdl, Seq.empty).get
        val call : Call = getCallFromNamespace(ns, "mul2")
        val inputs : Map[String,WdlValue] = Map("i" -> WdlInteger(3))
        val outputs : Seq[(String, WdlType, WdlValue)] = evalCall(call, inputs)
    }

    "SprayJs" should "marshal optionals" in {
        def marshal(name: String, dxType: String) : JsValue =  {
            s"""{ "name" : "${name}", "class" : "${dxType}" }""".parseJson
        }
        val x: JsValue = marshal("xxx", "array:file")
        System.err.println(s"json=${x.prettyPrint}")

        val m : Map[String, JsValue] = x.asJsObject.fields
        val m2 = m + ("optional" -> JsBoolean(true))
        val x2 = JsObject(m2)
        System.err.println(s"json=${x2.prettyPrint}")
    }

}
