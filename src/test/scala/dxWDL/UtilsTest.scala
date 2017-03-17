package dxWDL

import java.nio.charset.StandardCharsets
import java.nio.file.{Path, Paths, Files}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, OneInstancePerTest}
import spray.json._
import spray.json.DefaultJsonProtocol
//import spray.json.JsString
//import wdl4s.{AstTools, Call, Task, WdlExpression, WdlNamespace, WdlNamespaceWithWorkflow, Workflow}
//import wdl4s.AstTools.EnhancedAstNode
import wdl4s.types._
import wdl4s.values._

class UtilsTest extends FlatSpec with BeforeAndAfterEach with OneInstancePerTest {
    private def checkMarshal(v : WdlValue) : Unit = {
        val m = Utils.marshal(v)
        val u = Utils.unmarshal(m)
        assert(v == u)
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

    it should "sanitize json strings" in {
        List("A", "2211", "abcf", "abc ABC 123").foreach(s =>
            assert(Utils.sanitize(s) == s)
        )
        assert(Utils.sanitize("{}\\//") == "     ")
    }

    it should "print WdlBoolean in a human readable fashion" in {
        val b = WdlBoolean(true)
        //println(s"${b} ${b.toString} ${b.toWdlString}")
        assert(b.toWdlString == "true")
    }

    it should "print call inputs correctly" in {
        println("--------------------------------------")
        val m = Map("A" -> WdlBoolean(true),
                    "B" -> WdlInteger(23),
                    "C" -> WdlFloat(44.3),
                    "D" -> WdlString("ddd"),
                    "E" -> WdlFile("/tmp/xx"))
        System.err.print(s"callInputs=\n${Utils.inputsToString(m)}\n")
        println("--------------------------------------")
    }

}
