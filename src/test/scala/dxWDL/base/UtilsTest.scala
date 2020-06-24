package dxWDL.base

import dx.core.util.SysUtils
import dx.util.JsUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import spray.json._

class UtilsTest extends AnyFlatSpec with Matchers {
  val sentence = "I am major major"

  it should "Correctly compress and decompress" in {
    val s2 = SysUtils.gzipDecompress(SysUtils.gzipCompress(sentence.getBytes))
    sentence should be(s2)
  }

  it should "Correctly encode and decode base64" in {
    val encodeDecode = SysUtils.base64DecodeAndGunzip(SysUtils.gzipAndBase64Encode(sentence))
    sentence should be(encodeDecode)
  }

  it should "Make JSON maps deterministic" in {
    val x = JsObject("a" -> JsNumber(1), "b" -> JsNumber(2))
    val y = JsObject("b" -> JsNumber(2), "a" -> JsNumber(1))
    JsUtils.makeDeterministic(x) should be(JsUtils.makeDeterministic(y))

    val x2 = JsObject("a" -> JsNumber(10), "b" -> JsNumber(2))
    val y2 = JsObject("b" -> JsNumber(2), "a" -> JsNumber(1))
    assert(JsUtils.makeDeterministic(x2) != JsUtils.makeDeterministic(y2))
  }
}
