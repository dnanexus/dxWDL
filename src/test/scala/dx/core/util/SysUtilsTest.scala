package dx.core.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SysUtilsTest extends AnyFlatSpec with Matchers {
  val sentence = "I am major major"

  it should "Correctly compress and decompress" in {
    val s2 = SysUtils.gzipDecompress(SysUtils.gzipCompress(sentence.getBytes))
    sentence should be(s2)
  }

  it should "Correctly encode and decode base64" in {
    val encodeDecode = SysUtils.base64DecodeAndGunzip(SysUtils.gzipAndBase64Encode(sentence))
    sentence should be(encodeDecode)
  }
}
