package dxWDL.base

import org.scalatest.{FlatSpec, Matchers}

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
}
