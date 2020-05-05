package dxWDL.base

import com.typesafe.config._
import java.io.{ByteArrayOutputStream, ByteArrayInputStream}
import java.nio.charset.{StandardCharsets}
import java.nio.file.{Path, Paths, Files}
import java.util.Base64
import java.util.zip.{GZIPOutputStream, GZIPInputStream}
import scala.collection.JavaConverters._
import scala.collection.immutable.TreeMap
import scala.concurrent._
import spray.json._
import ExecutionContext.Implicits.global
import scala.sys.process._

//import wom.types._
import wdlTools.types.WdlTypes._

class PermissionDeniedException(s: String) extends Exception(s) {}

class InvalidInputException(s: String) extends Exception(s) {}

class IllegalArgumentException(s: String) extends Exception(s) {}

object Utils {
  val APPLET_LOG_MSG_LIMIT = 1000
  val CHECKSUM_PROP = "dxWDL_checksum"
  val DEFAULT_RUNTIME_DEBUG_LEVEL = 1
  val DEFAULT_APPLET_TIMEOUT_IN_DAYS = 2
  val DXFUSE_MAX_MEMORY_CONSUMPTION = 300 * 1024 * 1024 // how much memory dxfuse takes
  val DXAPI_NUM_OBJECTS_LIMIT = 1000 // maximal number of objects in a single API request
  val DX_WDL_ASSET = "dxWDLrt"
  val DX_URL_PREFIX = "dx://"
  val DX_WDL_RUNTIME_CONF_FILE = "dxWDL_runtime.conf"
  val FLAT_FILES_SUFFIX = "___dxfiles"
  val INTERMEDIATE_RESULTS_FOLDER = "intermediate"
  val LAST_STAGE = "last"
  val LINK_INFO_FILENAME = "linking.json"
  val MAX_NUM_RENAME_TRIES = 100
  val MAX_STRING_LEN = 32 * 1024 // Long strings cause problems with bash and the UI
  val MAX_STAGE_NAME_LEN = 60 // maximal length of a workflow stage name
  val MAX_NUM_FILES_MOVE_LIMIT = 1000
  val SCATTER_LIMIT = 500
  val UBUNTU_VERSION = "16.04"
  val VERSION_PROP = "dxWDL_version"
  val REORG_CONFIG = "reorg_conf___"
  val REORG_STATUS = "reorg_status___"
  val REORG_STATUS_COMPLETE = "completed"

  var traceLevel = 0

  // The version lives in application.conf
  def getVersion(): String = {
    val config = ConfigFactory.load("application.conf")
    config.getString("dxWDL.version")
  }

  // the regions live in dxWDL.conf
  def getRegions(): Map[String, String] = {
    val config = ConfigFactory.load(DX_WDL_RUNTIME_CONF_FILE)
    val l: List[Config] = config.getConfigList("dxWDL.region2project").asScala.toList
    val region2project: Map[String, String] = l.map { pair =>
      val r = pair.getString("region")
      val projName = pair.getString("path")
      r -> projName
    }.toMap
    region2project
  }

  // Ignore a value. This is useful for avoiding warnings/errors
  // on unused variables.
  def ignore[A](value: A): Unit = {}

  lazy val appCompileDirPath: Path = {
    val p = Paths.get("/tmp/dxWDL_Compile")
    safeMkdir(p)
    p
  }

  // Convert a fully qualified name to a local name.
  // Examples:
  //   SOURCE         RESULT
  //   lib.concat     concat
  //   lib.sum_list   sum_list
  def getUnqualifiedName(fqn: String): String = {
    if (fqn contains ".")
      fqn.split("\\.").last
    else
      fqn
  }

  // Create a file from a string
  def writeFileContent(path: Path, str: String): Unit = {
    Files.write(path, str.getBytes(StandardCharsets.UTF_8))
  }

  def readFileContent(path: Path): String = {
    // Java 8 Example - Uses UTF-8 character encoding
    val lines = Files.readAllLines(path, StandardCharsets.UTF_8).asScala.toList
    val sb = new StringBuilder(1024)
    lines.foreach { line =>
      sb.append(line)
      sb.append("\n")
    }
    sb.toString
  }

  // This one seems more idiomatic
  def readFileContent2(path: Path): String = {
    Files.readAllLines(path).asScala.mkString(System.lineSeparator())
  }

  def exceptionToString(e: Throwable): String = {
    val sw = new java.io.StringWriter
    e.printStackTrace(new java.io.PrintWriter(sw))
    sw.toString
  }

  // dx does not allow dots in variable names, so we
  // convert them to underscores.
  def transformVarName(varName: String): String = {
    varName.replaceAll("\\.", "___")
  }

  def revTransformVarName(varName: String): String = {
    varName.replaceAll("___", "\\.")
  }

  // Dots are illegal in applet variable names.
  def encodeAppletVarName(varName: String): String = {
    if (varName contains ".")
      throw new Exception(s"Variable ${varName} includes the illegal symbol \\.")
    varName
  }

  // recursive directory delete
  //    http://stackoverflow.com/questions/25999255/delete-directory-recursively-in-scala
  def deleteRecursive(file: java.io.File): Unit = {
    if (file.isDirectory) {
      Option(file.listFiles).map(_.toList).getOrElse(Nil).foreach(deleteRecursive(_))
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
    val fName = src.toFile().getName()
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
//
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

  // Job input, output,  error, and info files are located relative to the home
  // directory
  def jobFilesOfHomeDir(homeDir: Path): (Path, Path, Path, Path) = {
    val jobInputPath = homeDir.resolve("job_input.json")
    val jobOutputPath = homeDir.resolve("job_output.json")
    val jobErrorPath = homeDir.resolve("job_error.json")
    val jobInfoPath = homeDir.resolve("dnanexus-job.json")
    (jobInputPath, jobOutputPath, jobErrorPath, jobInfoPath)
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

    val p: Process = Process(cmds).run(logger, false)
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

  // types
  def isOptional(t: WdlTypes.WT): Boolean = {
    t match {
      case WdlTypes.WT_Optional(_) => true
      case t                  => false
    }
  }

  // We need to deal with types like:
  //     Int??, Array[File]??
  def stripOptional(t: WdlTypes.WT): WomType = {
    t match {
      case WdlTypes.WT_Optional(x) => stripOptional(x)
      case x                  => x
    }
  }

  // Replace all special json characters from with a white space.
  def sanitize(s: String): String = {
    def sanitizeChar(ch: Char): String = ch match {
      case '}'                       => " "
      case '{'                       => " "
      case '$'                       => " "
      case '/'                       => " "
      case '\\'                      => " "
      case '\"'                      => " "
      case '\''                      => " "
      case _ if (ch.isLetterOrDigit) => ch.toString
      case _ if (ch.isControl)       => " "
      case _                         => ch.toString
    }
    if (s != null)
      s.flatMap(sanitizeChar)
    else
      ""
  }

  // Logging output for applets at runtime
  def appletLog(verbose: Boolean, msg: String, limit: Int = APPLET_LOG_MSG_LIMIT): Unit = {
    if (verbose) {
      val shortMsg =
        if (msg.length > limit)
          "Message is too long for logging"
        else
          msg
      System.err.println(shortMsg)
    }
  }

  def genNSpaces(n: Int) = s"${" " * n}"

  def traceLevelSet(i: Int): Unit = {
    traceLevel = i
  }

  def traceLevelInc(): Unit = {
    traceLevel += 1
  }

  def traceLevelDec(): Unit = {
    if (traceLevel > 0)
      traceLevel -= 1
  }

  // Used by the compiler to provide more information to the user
  def trace(verbose: Boolean, msg: String): Unit = {
    if (!verbose)
      return
    val indent = genNSpaces(traceLevel * 2)
    System.err.println(indent + msg)
  }

  // color warnings yellow
  def warning(verbose: Verbose, msg: String): Unit = {
    if (verbose.quiet)
      return;
    System.err.println(Console.YELLOW + msg + Console.RESET)
  }

  def error(msg: String): Unit = {
    System.err.println(Console.RED + msg + Console.RESET)
  }

  // Make a JSON value deterministically sorted.  This is used to
  // ensure that the checksum does not change when maps
  // are ordered in different ways.
  //
  // Note: this does not handle the case where of arrays that
  // may have different equivalent orderings.
  def makeDeterministic(jsValue: JsValue): JsValue = {
    jsValue match {
      case JsObject(m: Map[String, JsValue]) =>
        val m2 = m.map {
          case (k, v) => k -> makeDeterministic(v)
        }.toMap
        val tree = TreeMap(m2.toArray: _*)
        JsObject(tree)
      case other =>
        other
    }
  }

  // Concatenate the elements, until hitting a size limit
  def buildLimitedSizeName(elements: Seq[String], maxLen: Int): String = {
    if (elements.isEmpty)
      return "[]"
    val (_, concat) = elements.tail.foldLeft((false, elements(0))) {
      case ((true, accu), _) =>
        // stopping condition reached, we have reached the size limit
        (true, accu)

      case ((false, accu), _) if accu.size >= maxLen =>
        // move into stopping condition
        (true, accu)

      case ((false, accu), elem) =>
        val tentative = accu + ", " + elem
        if (tentative.size > maxLen) {
          // not enough space
          (true, accu)
        } else {
          // still have space
          (false, tentative)
        }
    }
    "[" + concat + "]"
  }


}
