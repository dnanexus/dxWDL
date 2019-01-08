// From: https://gist.github.com/owainlewis/1e7d1e68a6818ee4d50e
// By: owainlewis
//
package dxWDL.util

import java.io.{ByteArrayOutputStream, ByteArrayInputStream}
import java.util.zip.{GZIPOutputStream, GZIPInputStream}

import scala.util.Try

object Gzip {

    def compress(input: Array[Byte]): Array[Byte] = {
        val bos = new ByteArrayOutputStream(input.length)
        val gzip = new GZIPOutputStream(bos)
        gzip.write(input)
        gzip.close()
        val compressed = bos.toByteArray
        bos.close()
        compressed
    }

    def decompress(compressed: Array[Byte]): Option[String] =
        Try {
            val inputStream = new GZIPInputStream(new ByteArrayInputStream(compressed))
            scala.io.Source.fromInputStream(inputStream).mkString
        }.toOption
}
