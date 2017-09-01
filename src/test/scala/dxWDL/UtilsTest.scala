package dxWDL

import com.typesafe.config._
import net.jcazevedo.moultingyaml._
import org.scalatest.{BeforeAndAfterEach, FlatSpec, OneInstancePerTest}
import scala.collection.JavaConverters._
import spray.json._
import wdl4s.wdl.types._
import wdl4s.wdl.values._

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
        val jobInfo = """{"links": ["file-F11jP2Q0ZvgYvPv77JBXgyB0"], "inputSpec": [{"help": "Int", "name": "ai", "class": "int"}, {"help": "Int", "name": "bi", "class": "int"}, {"optional": true, "name": "dbg_sleep", "class": "int"}], "dxapi": "1.0.0", "id": "applet-F11jP700ZvgvzxJ3Jq7p6jJZ", "title": "", "runSpec": {"execDepends": [{"name": "dx-java-bindings"}, {"name": "openjdk-8-jre-headless"}, {"package_manager": "apt", "name": "dx-toolkit"}], "bundledDependsByRegion": {"aws:us-east-1": [{"name": "resources.tar.gz", "id": {"$dnanexus_link": "file-F11jP2Q0ZvgYvPv77JBXgyB0"}}]}, "bundledDepends": [{"name": "resources.tar.gz", "id": {"$dnanexus_link": "file-F11jP2Q0ZvgYvPv77JBXgyB0"}}], "systemRequirements": {"main": {"instanceType": "mem1_ssd1_x2"}}, "executionPolicy": {}, "release": "14.04", "interpreter": "bash", "distribution": "Ubuntu"}, "access": {"network": []}, "state": "closed", "folder": "/", "description": "", "tags": [], "outputSpec": [{"help": "Int", "name": "sum", "class": "int"}], "sponsored": false, "createdBy": {"user": "user-orodeh"}, "class": "applet", "types": [], "hidden": false, "name": "add3.Add", "created": 1480820636000, "modified": 1480871955198, "summary": "", "project": "container-F12504j0188Bgk6pFXZY6PP3", "developerNotes": ""}"""

        val (infoIn,infoOut) = Utils.loadExecInfo(jobInfo)
        assert(infoIn("ai") == Some(WdlIntegerType))
        assert(infoIn("bi") == Some(WdlIntegerType))
        assert(infoIn("dbg_sleep") == None)
        assert(infoOut("sum") == Some(WdlIntegerType))
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

        var (info,_) = Utils.loadExecInfo(jobInfo)
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
        assert(b.toWdlString == "true")
    }

    it should "pretty print strings in IR with newlines" in {
        val yaml1 = YamlString(
            """\//||\/||
          |// ||  ||__
          |""".stripMargin)

        val buf = """||
                     |  \//||\/||
                     |  // ||  ||__
                     |""".stripMargin
        assert(yaml1.prettyPrint == buf)
        //print(buf)

/*        {

            val yaml2:  = YamlString(
                """hello
                   |world
                   |it is nice outside""".stripMargin)

            assert(yaml2.prettyPrint(Block) ==
                       """||
                          | hello
                          | world
                          | it is nice outside""".stripMargin)
        } */
    }

    "SprayJs" should "marshal optionals" in {
        def marshal(name: String, dxType: String) : JsValue =  {
            s"""{ "name" : "${name}", "class" : "${dxType}" }""".parseJson
        }
        val x: JsValue = marshal("xxx", "array:file")
        //System.err.println(s"json=${x.prettyPrint}")

        val m : Map[String, JsValue] = x.asJsObject.fields
        val m2 = m + ("optional" -> JsBoolean(true))
        val x2 = JsObject(m2)
        //System.err.println(s"json=${x2.prettyPrint}")
    }

    "ConfigFactory" should "understand our reference.conf file" in {
        def confData =
            """|
               |dxWDL {
               |    version = "0.34"
               |    asset_ids = [{
               |        region = "aws:us-east-1"
               |        asset = "record-F5gyyXj0P26p9Jx12q3XY0qV"
               |    }]
               |}""".stripMargin.trim
        val config = ConfigFactory.parseString(confData)
        val rawAssets: List[Config] = config.getConfigList("dxWDL.asset_ids").asScala.toList
        val assets:Map[String, String] = rawAssets.map{ pair =>
            val r = pair.getString("region")
            val assetId = pair.getString("asset")
            assert(assetId.startsWith("record"))
            r -> assetId
        }.toMap
        assert(assets("aws:us-east-1") == "record-F5gyyXj0P26p9Jx12q3XY0qV")
    }
}
