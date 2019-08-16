package dxWDL.base

import org.scalatest.{FlatSpec, Matchers}
import spray.json._

class UtilsTest extends FlatSpec with Matchers {
    val sentence = "I am major major"


    it should "Correctly compress and decompress" in {
        val s2 = Utils.gzipDecompress(Utils.gzipCompress(sentence.getBytes))
        sentence should be (s2)
    }

    it should "Correctly encode and decode base64" in {
        val encodeDecode = Utils.base64DecodeAndGunzip(Utils.gzipAndBase64Encode(sentence))
        sentence should be (encodeDecode)
    }

    it should "make JSON maps deterministic" in {
        val x = JsObject("a" -> JsNumber(1), "b" -> JsNumber(2))
        val y = JsObject("b" -> JsNumber(2), "a" -> JsNumber(1))
        Utils.makeDeterministic(x) should be(Utils.makeDeterministic(y))

        val x2 = JsObject("a" -> JsNumber(10), "b" -> JsNumber(2))
        val y2 = JsObject("b" -> JsNumber(2), "a" -> JsNumber(1))
        assert(Utils.makeDeterministic(x2) != Utils.makeDeterministic(y2))
    }
}
