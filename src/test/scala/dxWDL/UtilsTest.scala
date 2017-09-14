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
