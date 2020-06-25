package dx.core.util

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.Base64
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import scala.jdk.CollectionConverters._

object SysUtils {
  // Create a file from a string
  def writeFileContent(path: Path, str: String): Unit = {
    Files.write(path, str.getBytes(StandardCharsets.UTF_8))
  }

  /**
    * Reads the entire contents of a file as a string. Line endings are not stripped or
    * converted.
    * @param path file path
    * @return file contents as a string
    */
  def readFileContent(path: Path): String = {
    new String(Files.readAllBytes(path), StandardCharsets.UTF_8)
  }

  /**
    * Reads all the lines from a file and returns them as a Vector of strings. Lines have
    * line-ending characters ([\r\n]) stripped off. Notably, there is no way to know if
    * the last line originaly ended with a newline.
    * @param path file path
    * @return Vector of lines as Strings
    */
  def readFileLines(path: Path): Vector[String] = {
    Files.readAllLines(path, StandardCharsets.UTF_8).asScala.toVector
  }

  // recursive directory delete
  //    http://stackoverflow.com/questions/25999255/delete-directory-recursively-in-scala
  def deleteRecursive(file: java.io.File): Unit = {
    if (file.isDirectory) {
      Option(file.listFiles).map(_.toList).getOrElse(Nil).foreach(deleteRecursive)
    }
    file.delete
  }

  def safeMkdir(path: Path): Unit = {
    if (!Files.exists(path)) {
      Files.createDirectories(path)
    } else {
      // Path exists, make sure it is a directory, and not a file
      if (!Files.isDirectory(path))
        throw new Exception(s"Path ${path} exists, but is not a directory")
    }
  }

  // Add a suffix to a filename, before the regular suffix. For example:
  //  xxx.wdl -> xxx.simplified.wdl
  def replaceFileSuffix(src: Path, suffix: String): String = {
    val fName = src.toFile.getName
    val index = fName.lastIndexOf('.')
    if (index == -1) {
      fName + suffix
    } else {
      val prefix = fName.substring(0, index)
      prefix + suffix
    }
  }

  // From: https://gist.github.com/owainlewis/1e7d1e68a6818ee4d50e
  // By: owainlewis
  def gzipCompress(input: Array[Byte]): Array[Byte] = {
    val bos = new ByteArrayOutputStream(input.length)
    val gzip = new GZIPOutputStream(bos)
    gzip.write(input)
    gzip.close()
    val compressed = bos.toByteArray
    bos.close()
    compressed
  }

  def gzipDecompress(compressed: Array[Byte]): String = {
    val inputStream = new GZIPInputStream(new ByteArrayInputStream(compressed))
    scala.io.Source.fromInputStream(inputStream).mkString
  }

  def gzipAndBase64Encode(buf: String): String = {
    val bytes = buf.getBytes
    val gzBytes = gzipCompress(bytes)
    Base64.getEncoder.encodeToString(gzBytes)
  }

  def base64DecodeAndGunzip(buf64: String): String = {
    val ba: Array[Byte] = Base64.getDecoder.decode(buf64.getBytes)
    gzipDecompress(ba)
  }
}
