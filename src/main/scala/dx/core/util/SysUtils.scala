package dx.core.util

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.Base64
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits._
import scala.jdk.CollectionConverters._
import scala.sys.process.{Process, ProcessLogger}

object SysUtils {
  // Create a file from a string
  def writeFileContent(path: Path, str: String): Unit = {
    Files.write(path, str.getBytes(StandardCharsets.UTF_8))
  }

  // This one seems more idiomatic
  def readFileContent(path: Path): String = {
    Files.readAllLines(path, StandardCharsets.UTF_8).asScala.mkString(System.lineSeparator())
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

  // Run a child process and collect stdout and stderr into strings
  def execCommand(cmdLine: String,
                  timeout: Option[Int] = None,
                  quiet: Boolean = false): (String, String) = {
    val cmds = Seq("/bin/sh", "-c", cmdLine)
    val outStream = new StringBuilder()
    val errStream = new StringBuilder()
    val logger = ProcessLogger(
        (o: String) => { outStream.append(o ++ "\n") },
        (e: String) => { errStream.append(e ++ "\n") }
    )

    val p: Process = Process(cmds).run(logger, connectInput = false)
    timeout match {
      case None =>
        // blocks, and returns the exit code. Does NOT connect
        // the standard in of the child job to the parent
        val retcode = p.exitValue()
        if (retcode != 0) {
          if (!quiet) {
            System.err.println(s"STDOUT: ${outStream.toString()}")
            System.err.println(s"STDERR: ${errStream.toString()}")
          }
          throw new Exception(s"Error running command ${cmdLine}")
        }
      case Some(nSec) =>
        val f = Future(blocking(p.exitValue()))
        try {
          Await.result(f, duration.Duration(nSec, "sec"))
        } catch {
          case _: TimeoutException =>
            p.destroy()
            throw new Exception(s"Timeout exceeded (${nSec} seconds)")
        }
    }
    (outStream.toString(), errStream.toString())
  }
}
