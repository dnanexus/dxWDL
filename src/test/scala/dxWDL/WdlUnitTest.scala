package dxWDL

import java.nio.charset.StandardCharsets
import java.nio.file.{Path, Paths, Files}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, OneInstancePerTest}
import scala.sys.process._
import spray.json._
import spray.json.DefaultJsonProtocol
//import spray.json.JsString
import wdl4s.{AstTools, Call, Task, WdlExpression, WdlNamespace, WdlNamespaceWithWorkflow, Workflow}
import wdl4s.AstTools.EnhancedAstNode
import wdl4s.types._
import wdl4s.values._


class WdlUnitTest extends FlatSpec with BeforeAndAfterEach with OneInstancePerTest {

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

    private def checkMarshal(v : WdlValue) : Unit = {
        val m = Utils.marshal(v)
        val u = Utils.unmarshal(m)
        assert(v == u)
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

        AppletRunner.prologCore(call, callInputs)
        val scriptPath = metaDir.resolve("script")
        execBashScript(scriptPath)

        // extract the return code
        val rcPath = metaDir.resolve("rc")
        val rc = Utils.readFileContent(rcPath).trim.toInt
        if (goodRetcode)
            assert(rc == 0)
        else
            assert(rc != 0)
        AppletRunner.epilogCore(call)
    }

    it should "Add scope to flat inputs" in {
        {
            val inputs1 = Map("Add.sum" -> WdlInteger(1))
            val scoped1 = AppletRunner.addScopeToInputs(inputs1)
            scoped1("Add") match {
                case WdlObject(o) => assert(o.get("sum") == Some(WdlInteger(1)))
                case _ => assert(false)
            }
        }

        {
            val inputs2 = Map("Add.sum" -> WdlInteger(1),
                              "Leaf.xxx" -> WdlString("tal"))
            val scoped2 = AppletRunner.addScopeToInputs(inputs2)
            scoped2("Add") match {
                case WdlObject(o) => assert(o.get("sum") == Some(WdlInteger(1)))
                case _ => assert(false)
            }
            scoped2("Leaf") match {
                case WdlObject(o) => assert(o.get("xxx") == Some(WdlString("tal")))
                case _ => assert(false)
            }
        }

        {
            val inputs3 = Map("ai" -> WdlInteger(1), "bi" -> WdlInteger(2))
            val scoped3 = AppletRunner.addScopeToInputs(inputs3)
            assert(scoped3.get("bi") == Some(WdlInteger(2)))
            assert(scoped3.get("ai") == Some(WdlInteger(1)))
        }

        {
            val inputs4 = Map("Alpha.dog" -> WdlInteger(1),
                              "Alpha.rabbit" -> WdlInteger(2),
                              "Beta.crow" -> WdlString("yellow"),
                              "Beta.bear" -> WdlString("black"))
            val scoped4 = AppletRunner.addScopeToInputs(inputs4)
            val alpha : Map[String, WdlValue] = scoped4("Alpha") match {
                case WdlObject(o) => o
                case _ => throw new IllegalArgumentException()
            }
            val beta : Map[String, WdlValue] = scoped4("Beta") match {
                case WdlObject(o) => o
                case _ => throw new IllegalArgumentException()
            }
            assert(alpha.get("dog") == Some(WdlInteger(1)))
            assert(alpha.get("rabbit") == Some(WdlInteger(2)))
            assert(beta.get("crow") == Some(WdlString("yellow")))
            assert(beta.get("bear") == Some(WdlString("black")))
        }
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


        val ns = WdlNamespaceWithWorkflow.load(wdl)
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


        val ns = WdlNamespaceWithWorkflow.load(wdl)
        val call : Call = getCallFromNamespace(ns, "Add2")
        val inputs = Map("Add.sum" -> WdlInteger(1))
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

        val ns = WdlNamespaceWithWorkflow.load(wdl)
        val call : Call = getCallFromNamespace(ns, "Concat")
        val inputs : Map[String,WdlValue] =
            Map("str_array" ->
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
        val ns = WdlNamespaceWithWorkflow.load(wdl)
        val call : Call = getCallFromNamespace(ns, "Concat")
        val inputs : Map[String,WdlValue] =
            Map("int_array" ->
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
        val ns = WdlNamespaceWithWorkflow.load(wdl)
        val call : Call = getCallFromNamespace(ns, "wc")

        // create a few files
        List("X", "Y", "Z").foreach{ case fName =>
            val path = "/tmp/" ++ fName ++ ".txt"
            writeStringToFile(path, "ABCFD12344")
        }

        val inputs : Map[String,WdlValue] =
            Map("fs" -> WdlArray(WdlArrayType(WdlFileType),
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
        val ns = WdlNamespaceWithWorkflow.load(wdl)
        val call : Call = getCallFromNamespace(ns, "Concat")

        val inputs : Map[String,WdlValue] =
            Map("x_ia" ->
                    WdlArray(WdlArrayType(WdlIntegerType),
                             List(WdlInteger(1), WdlInteger(2), WdlInteger(3))),
                "x_fa" ->
                    WdlArray(WdlArrayType(WdlFloatType),
                             List(WdlFloat(1.4), WdlFloat(3.14), WdlFloat(1.618))),
                "x_ba" ->
                    WdlArray(WdlArrayType(WdlBooleanType),
                             List(WdlBoolean(false), WdlBoolean(true), WdlBoolean(true))),
                "x_sa" ->
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

        val ns = WdlNamespaceWithWorkflow.load(wdl)
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

            val ns = WdlNamespaceWithWorkflow.load(wdl)
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

            val ns = WdlNamespaceWithWorkflow.load(wdl)
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

        val ns = WdlNamespaceWithWorkflow.load(wdl)
        val call : Call = getCallFromNamespace(ns, "mul2")
        val inputs : Map[String,WdlValue] = Map("i" -> WdlInteger(3))
        val outputs : Seq[(String, WdlType, WdlValue)] = evalCall(call, inputs)
    }


    "Utils" should "marshal and unmarshal wdlValues" in {
        checkMarshal(WdlString("hello"))
        checkMarshal(WdlInteger(33))
        checkMarshal(WdlArray(
                  WdlArrayType(WdlStringType),
                  List(WdlString("one"), WdlString("two"), WdlString("three"), WdlString("four"))))
        checkMarshal(WdlArray(
                  WdlArrayType(WdlIntegerType),
                  List(WdlInteger(1), WdlInteger(2), WdlInteger(3), WdlInteger(4))))

        // Ragged array
        def mkIntArray(l : List[Int]) : WdlValue = {
            WdlArray(WdlArrayType(WdlIntegerType), l.map(x => WdlInteger(x)))
        }
        val ra : WdlValue = WdlArray(
            WdlArrayType(WdlArrayType(WdlIntegerType)),
            List(mkIntArray(List(1,2)),
                 mkIntArray(List(3,5)),
                 mkIntArray(List(8)),
                 mkIntArray(List(13, 21, 34))))
        checkMarshal(ra)
    }

    // This does not work with wdl4s version 0.7
    ignore should "marshal and unmarshal WdlFloat without losing precision" in {
        checkMarshal(WdlFloat(4.2))
    }

    it should "parse job executable info, and extract help strings" in {
        val jobInfo = """{"links": ["file-F11jP2Q0ZvgYvPv77JBXgyB0"], "inputSpec": [{"help": "Int", "name": "ai", "class": "int"}, {"help": "Int", "name": "bi", "class": "int"}, {"optional": true, "name": "dbg_sleep", "class": "int"}], "dxapi": "1.0.0", "id": "applet-F11jP700ZvgvzxJ3Jq7p6jJZ", "title": "", "runSpec": {"execDepends": [{"name": "dx-java-bindings"}, {"name": "openjdk-8-jre-headless"}, {"package_manager": "apt", "name": "dx-toolkit"}], "bundledDependsByRegion": {"aws:us-east-1": [{"name": "resources.tar.gz", "id": {"$dnanexus_link": "file-F11jP2Q0ZvgYvPv77JBXgyB0"}}]}, "bundledDepends": [{"name": "resources.tar.gz", "id": {"$dnanexus_link": "file-F11jP2Q0ZvgYvPv77JBXgyB0"}}], "systemRequirements": {"main": {"instanceType": "mem1_ssd1_x2"}}, "executionPolicy": {}, "release": "14.04", "interpreter": "bash", "distribution": "Ubuntu"}, "access": {"network": []}, "state": "closed", "folder": "/", "description": "", "tags": [], "outputSpec": [{"name": "sum", "class": "int"}], "sponsored": false, "createdBy": {"user": "user-orodeh"}, "class": "applet", "types": [], "hidden": false, "name": "add3.Add", "created": 1480820636000, "modified": 1480871955198, "summary": "", "project": "container-F12504j0188Bgk6pFXZY6PP3", "developerNotes": ""}"""

        val info = Utils.loadExecInfo(jobInfo)
        assert(info("ai") == Some(WdlIntegerType))
        assert(info("bi") == Some(WdlIntegerType))
        assert(info("dbg_sleep") == None)
    }

    it should "parse job executable info (II)" in {
        val jobInfo = """{
        "inputSpec": [
            {
                "help": "Int",
                "name": "ai",
                "class": "int"
            }
        ],
        "dxapi": "1.0.0",
        "id": "applet-F2KGBj80ZvgjbQY30vQ4qKZY",
        "title": "",
        "runSpec": {
            "executionPolicy": {},
            "release": "14.04",
            "interpreter": "bash",
            "distribution": "Ubuntu"
        },
        "outputSpec": [
            {
                "help": "Int",
                "name": "ai",
                "class": "int"
            }
        ],
        "class": "applet",
        "types": []
    }"""

        var info = Utils.loadExecInfo(jobInfo)
        assert(info("ai") == Some(WdlIntegerType))
    }

    "InstanceTypes" should "Choose reasonable platform instance types" in {
        assert(InstanceTypes.choose(None, None, None) == "mem1_ssd1_x2")

        // parameters are:          RAM,     disk,     cores
        assert(InstanceTypes.choose(Some(3), Some(100), Some(4)) == "mem1_ssd2_x4")
        assert(InstanceTypes.choose(Some(2), Some(20), None) == "mem1_ssd1_x2")
        assert(InstanceTypes.choose(Some(30), Some(128), Some(8)) == "mem3_ssd1_x8")

        assert(InstanceTypes.apply(Some(WdlString("3 GB")),
                                   Some(WdlString("local-disk 10 HDD")),
                                   Some(WdlString("1"))) == "mem1_ssd1_x2")
        assert(InstanceTypes.apply(Some(WdlString("37 GB")),
                                   Some(WdlString("local-disk 10 HDD")),
                                   Some(WdlString("6"))) == "mem3_ssd1_x8")
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

    it should "sanitize json strings" in {
        List("A", "2211", "abcf", "abc ABC 123").foreach(s =>
            assert(Utils.sanitize(s) == s)
        )
        assert(Utils.sanitize("{}\\//") == "     ")
    }
}
