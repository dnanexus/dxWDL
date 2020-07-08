package dx.core.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CompressionUtilsTest extends AnyFlatSpec with Matchers {
  val sentence = "I am major major"

  it should "Correctly compress and decompress" in {
    val s2 = CompressionUtils.gzipDecompress(CompressionUtils.gzipCompress(sentence.getBytes))
    sentence should be(s2)
  }

  it should "Correctly encode and decode base64" in {
    val encodeDecode =
      CompressionUtils.base64DecodeAndGunzip(CompressionUtils.gzipAndBase64Encode(sentence))
    sentence should be(encodeDecode)
  }
}
