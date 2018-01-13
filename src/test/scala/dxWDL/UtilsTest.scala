package dxWDL

import com.typesafe.config._
import net.jcazevedo.moultingyaml._
import org.scalatest.{FlatSpec, Matchers}
import scala.collection.JavaConverters._
import spray.json._
import wdl4s.wdl._
import wdl4s.wdl.types._
import wdl4s.wdl.values._

class UtilsTest extends FlatSpec with Matchers {
    private def checkMarshal(v : WdlValue) : Unit = {
        val m = Utils.marshal(v)
        val u = Utils.unmarshal(m)
        v should equal(u)
    }


    it should "marshal and unmarshal wdlValues" in {
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
            Utils.sanitize(s) should equal(s)
        )
        Utils.sanitize("{}\\//") should equal("     ")
    }

    it should "print WdlBoolean in a human readable fashion" in {
        val b = WdlBoolean(true)
        b.toWdlString should equal("true")
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
        yaml1.prettyPrint should equal(buf)
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

    it should "evaluate constants" in {
        val expr = WdlExpression.fromString("[1, 2, 19]")
        val answer = WdlArray(WdlArrayType(WdlIntegerType),
                              List(WdlInteger(1), WdlInteger(2), WdlInteger(19)))
        Utils.isExpressionConst(expr) should equal(true)
        Utils.evalConst(expr) should equal(answer)
    }

    it should "evaluate expressions with constants only" in {
        val expr = WdlExpression.fromString("3 + 8")
        Utils.isExpressionConst(expr) should equal(true)
        Utils.evalConst(expr) should equal(WdlInteger(11))
    }

    it should "identify non-constants" in {
        val expr = WdlExpression.fromString("i")
        Utils.isExpressionConst(expr) should equal(false)
    }

    ignore should "coerce arrays" in {
        val v = WdlArray(
            WdlArrayType(WdlStringType),
            List(WdlString("one"), WdlString("two"), WdlString("three"), WdlString("four")))
        val wdlType =  WdlNonEmptyArrayType(WdlStringType)
        val v2 = wdlType.coerceRawValue(v).get
        v2.wdlType should equal(wdlType)
    }

    "SprayJs" should "marshal optionals" in {
        def marshal(name: String, dxType: String) : JsValue =  {
            s"""{ "name" : "${name}", "class" : "${dxType}" }""".parseJson
        }
        val x: JsValue = marshal("xxx", "array:file")
        //System.err.println(s"json=${x.prettyPrint}")

        val m : Map[String, JsValue] = x.asJsObject.fields
        val m2 = m + ("optional" -> JsBoolean(true))
        val _ = JsObject(m2)
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
        assets("aws:us-east-1") should equal("record-F5gyyXj0P26p9Jx12q3XY0qV")
    }
}
