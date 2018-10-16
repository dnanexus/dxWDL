package dxWDL

import com.typesafe.config._
import net.jcazevedo.moultingyaml._
import org.scalatest.{FlatSpec, Matchers}
import scala.collection.JavaConverters._
import spray.json._
import wdl.draft2.model._
import wom.types._
import wom.values._

class UtilsTest extends FlatSpec with Matchers {
    private def checkMarshal(v : WomValue) : Unit = {
        val m = Utils.marshal(v)
        val u = Utils.unmarshal(m)
        v should equal(u)
    }


    it should "marshal and unmarshal wdlValues" in {
        checkMarshal(WomString("hello"))
        checkMarshal(WomInteger(33))
        checkMarshal(WomArray(
                  WomArrayType(WomStringType),
                  List(WomString("one"), WomString("two"), WomString("three"), WomString("four"))))
        checkMarshal(WomArray(
                  WomArrayType(WomIntegerType),
                  List(WomInteger(1), WomInteger(2), WomInteger(3), WomInteger(4))))

        // Ragged array
        def mkIntArray(l : List[Int]) : WomValue = {
            WomArray(WomArrayType(WomIntegerType), l.map(x => WomInteger(x)))
        }
        val ra : WomValue = WomArray(
            WomArrayType(WomArrayType(WomIntegerType)),
            List(mkIntArray(List(1,2)),
                 mkIntArray(List(3,5)),
                 mkIntArray(List(8)),
                 mkIntArray(List(13, 21, 34))))
        checkMarshal(ra)
    }

    it should "sanitize json strings" in {
        List("A", "2211", "abcf", "abc ABC 123").foreach(s =>
            Utils.sanitize(s) should equal(s)
        )
        Utils.sanitize("{}\\//") should equal("     ")
    }

    it should "print WomBoolean in a human readable fashion" in {
        val b = WomBoolean(true)
        b.toWomString should equal("true")
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
    }

    it should "evaluate constants" in {
        val expr = WdlExpression.fromString("[1, 2, 19]")
        val answer = WomArray(WomArrayType(WomIntegerType),
                              List(WomInteger(1), WomInteger(2), WomInteger(19)))
        Utils.isExpressionConst(expr) should equal(true)
        Utils.evalConst(expr) should equal(answer)
    }

    it should "evaluate expressions with constants only" in {
        val expr = WdlExpression.fromString("3 + 8")
        Utils.isExpressionConst(expr) should equal(true)
        Utils.evalConst(expr) should equal(WomInteger(11))
    }

    it should "identify non-constants" in {
        val expr = WdlExpression.fromString("i")
        Utils.isExpressionConst(expr) should equal(false)
    }

    it  should "coerce arrays" in {
        val v = WomArray(
            WomArrayType(WomStringType),
            List(WomString("one"), WomString("two"), WomString("three"), WomString("four")))
        val womType =  WomNonEmptyArrayType(WomStringType)
        val v2 = womType.coerceRawValue(v).get
        v2.womType should equal(womType)
    }


    it should "be able to decode unicode strings" in {
        val s = "\u8704\u2200"
        val h = Utils.unicodeToHex(s)
        val s2 = Utils.unicodeFromHex(h)
        s should equal(s2)
    }

    it should "get version number" in {
        val version = Utils.getVersion()
        Utils.ignore(version)
    }

    "SprayJs" should "marshal optionals" in {
        def marshal(name: String, dxType: String) : JsValue =  {
            s"""{ "name" : "${name}", "class" : "${dxType}" }""".parseJson
        }
        val x: JsValue = marshal("xxx", "array:file")

        val m : Map[String, JsValue] = x.asJsObject.fields
        val m2 = m + ("optional" -> JsBoolean(true))
        Utils.ignore(JsObject(m2))
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
