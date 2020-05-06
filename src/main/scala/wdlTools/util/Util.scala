package wdlTools.util

import java.io.{FileNotFoundException, IOException}

import scala.jdk.CollectionConverters._
import java.net.URL
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, Paths, SimpleFileVisitor}

import com.typesafe.config.ConfigFactory
import Verbosity._

object Util {

  /**
    * The current wdlTools version.
    * @return
    */
  def getVersion: String = {
    val config = ConfigFactory.load("application.conf")
    config.getString("wdlTools.version")
  }

  def getUrl(pathOrUrl: String,
             searchPath: Vector[Path] = Vector.empty,
             mustExist: Boolean = true): URL = {
    if (pathOrUrl.contains("://")) {
      new URL(pathOrUrl)
    } else {
      val path: Path = Paths.get(pathOrUrl)
      val resolved: Option[Path] = if (Files.exists(path)) {
        Some(path)
      } else if (searchPath.nonEmpty) {
        // search in all directories where imports may be found
        searchPath.map(d => d.resolve(pathOrUrl)).collectFirst {
          case fp if Files.exists(fp) => fp
        }
      } else None
      val result = resolved.getOrElse {
        if (mustExist) {
          throw new FileNotFoundException(
              s"Could not resolve path or URL ${pathOrUrl} in search path [${searchPath.mkString(",")}]"
          )
        } else {
          path
        }
      }
      new URL(s"file://${result.toAbsolutePath}")
    }
  }

  def pathToUrl(path: Path): URL = {
    path.toUri.toURL
  }

  /**
    * Determines the local path to a URI's file. The path will be the URI's file name relative to the parent; the
    * current working directory is used as the parent unless `parent` is specified. If the URI indicates a local path
    * and `ovewrite` is `true`, then the absolute local path is returned unless `parent` is specified.
    *
    * @param url a URL, which might be a local path, a file:// uri, or an http(s):// uri)
    * @param parent The directory to which the local file should be made relative
    * @param existsOk Whether it is allowed to return the absolute path of a URI that is a local file that already
    *                  exists, rather than making it relative to the current directory; ignored if `parent` is defined
    * @return The Path to the local file
    */
  def getLocalPath(url: URL, parent: Option[Path] = None, existsOk: Boolean = true): Path = {
    val resolved = url.getProtocol match {
      case null | "" | "file" =>
        val path = Paths.get(url.getPath)
        if (parent.isDefined) {
          parent.get.resolve(path.getFileName)
        } else if (path.isAbsolute) {
          path
        } else {
          Paths.get("").toAbsolutePath.resolve(path)
        }
      case _ =>
        parent.getOrElse(Paths.get("")).resolve(Paths.get(url.getPath).getFileName)
    }
    if (!existsOk && Files.exists(resolved)) {
      throw new Exception(s"File already exists: ${resolved}")
    }
    resolved
  }

  /**
    * Reads the lines from a file and concatenates the lines using the system line separator.
    * @param path the path to the file
    * @return
    */
  def readFromFile(path: Path): String = {
    readLinesFromFile(path).mkString(System.lineSeparator())
  }

  /**
    * Reads the lines from a file
    * @param path the path to the file
    * @return a Seq of the lines from the file
    */
  def readLinesFromFile(path: Path): Seq[String] = {
    val source = io.Source.fromFile(path.toString)
    try {
      source.getLines.toVector
    } finally {
      source.close()
    }
  }

  /**
    * Write a collection of documents, which is a map of URIs to sequences of lines, to
    * disk by converting each URI to a local path.
    * @param docs the documents to write
    * @param outputDir the output directory; if None, the URI is converted to an absolute path if possible
    * @param overwrite whether it is okay to overwrite an existing file
    */
  def writeLinesToFiles(docs: Map[URL, Seq[String]],
                        outputDir: Option[Path],
                        overwrite: Boolean = false): Unit = {
    docs.foreach {
      case (url, lines) =>
        val outputPath = Util.getLocalPath(url, outputDir, overwrite)
        Files.write(outputPath, lines.asJava)
    }
  }

  def rmdir(dir: Path): Unit = {
    Files.walkFileTree(
        dir,
        new SimpleFileVisitor[Path] {
          override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
            Files.delete(file)
            FileVisitResult.CONTINUE
          }
          override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
            Files.delete(dir)
            FileVisitResult.CONTINUE
          }
        }
    )
  }

  /**
    * Write a collection of documents, which is a map of URIs to contents, to disk by converting
    * each URI to a local path.
    * @param docs the documents to write
    * @param outputDir the output directory; if None, the URI is converted to an absolute path if possible
    * @param overwrite whether it is okay to overwrite an existing file
    */
  def writeContentsToFiles(docs: Map[URL, String],
                           outputDir: Option[Path] = None,
                           overwrite: Boolean = false): Unit = {
    docs.foreach {
      case (url, contents) =>
        val outputPath = Util.getLocalPath(url, outputDir, overwrite)
        Files.createDirectories(outputPath.getParent)
        Files.write(outputPath, contents.getBytes())
    }
  }

  /**
    * Pretty formats a Scala value similar to its source represention.
    * Particularly useful for case classes.
    * @see https://gist.github.com/carymrobbins/7b8ed52cd6ea186dbdf8
    * @param a The value to pretty print.
    * @param indentSize Number of spaces for each indent.
    * @param maxElementWidth Largest element size before wrapping.
    * @param depth Initial depth to pretty print indents.
    * @return
    */
  def prettyFormat(a: Any,
                   indentSize: Int = 2,
                   maxElementWidth: Int = 30,
                   depth: Int = 0): String = {
    val indent = " " * depth * indentSize
    val fieldIndent = indent + (" " * indentSize)
    val thisDepth = prettyFormat(_: Any, indentSize, maxElementWidth, depth)
    val nextDepth = prettyFormat(_: Any, indentSize, maxElementWidth, depth + 1)
    a match {
      // Make Strings look similar to their literal form.
      case s: String =>
        val replaceMap = Seq(
            "\n" -> "\\n",
            "\r" -> "\\r",
            "\t" -> "\\t",
            "\"" -> "\\\""
        )
        val buf = replaceMap.foldLeft(s) { case (acc, (c, r)) => acc.replace(c, r) }
        s""""${buf}""""
      // For an empty Seq just use its normal String representation.
      case xs: Seq[_] if xs.isEmpty => xs.toString()
      case xs: Seq[_]               =>
        // If the Seq is not too long, pretty print on one line.
        val resultOneLine = xs.map(nextDepth).toString()
        if (resultOneLine.length <= maxElementWidth) return resultOneLine
        // Otherwise, build it with newlines and proper field indents.
        val result = xs.map(x => s"\n$fieldIndent${nextDepth(x)}").toString()
        result.substring(0, result.length - 1) + "\n" + indent + ")"
      // Product should cover case classes.
      case p: Product =>
        val prefix = p.productPrefix
        // We'll use reflection to get the constructor arg names and values.
        val cls = p.getClass
        val fields = cls.getDeclaredFields.filterNot(_.isSynthetic).map(_.getName)
        val values = p.productIterator.toSeq
        // If we weren't able to match up fields/values, fall back to toString.
        if (fields.length != values.length) return p.toString
        fields.zip(values).toList match {
          // If there are no fields, just use the normal String representation.
          case Nil => p.toString
          // If there is just one field, let's just print it as a wrapper.
          case (_, value) :: Nil => s"$prefix(${thisDepth(value)})"
          // If there is more than one field, build up the field names and values.
          case kvps =>
            val prettyFields = kvps.map { case (k, v) => s"$fieldIndent$k = ${nextDepth(v)}" }
            // If the result is not too long, pretty print on one line.
            val resultOneLine = s"$prefix(${prettyFields.mkString(", ")})"
            if (resultOneLine.length <= maxElementWidth) return resultOneLine
            // Otherwise, build it with newlines and proper field indents.
            s"$prefix(\n${prettyFields.mkString(",\n")}\n$indent)"
        }
      // If we haven't specialized this type, just use its toString.
      case _ => a.toString
    }
  }

  /**
    * Simple bi-directional Map class.
    * @param keys map keys
    * @param values map values - must be unique, i.e. you must be able to map values -> keys without collisions
    * @tparam X keys Type
    * @tparam Y values Type
    */
  case class BiMap[X, Y](keys: Seq[X], values: Seq[Y]) {
    require(keys.size == values.size, "no 1 to 1 relation")
    private lazy val kvMap: Map[X, Y] = keys.zip(values).toMap
    private lazy val vkMap: Map[Y, X] = values.zip(keys).toMap

    def size: Int = keys.size

    def fromKey(x: X): Y = kvMap(x)

    def fromValue(y: Y): X = vkMap(y)

    def filterKeys(p: X => Boolean): BiMap[X, Y] = {
      BiMap.fromPairs(keys.zip(values).filter(item => p(item._1)))
    }
  }

  object BiMap {
    def fromPairs[X, Y](pairs: Seq[(X, Y)]): BiMap[X, Y] = {
      BiMap(pairs.map(_._1), pairs.map(_._2))
    }

    def fromMap[X, Y](map: Map[X, Y]): BiMap[X, Y] = {
      fromPairs(map.toVector)
    }
  }

  def warning(msg: String, verbose: Verbosity): Unit = {
    if (verbose == Quiet) {
      return
    }
    System.err.println(Console.YELLOW + msg + Console.RESET)
  }

  def error(msg: String): Unit = {
    System.err.println(Console.RED + msg + Console.RED)
  }

  // ignore a value without causing a compilation error
  // TODO: log this if antlr4Trace is turned on
  def ignore[A](x: A): Unit = {}

  /**
    * A wrapper around a primitive that enables passing a mutable variable by reference.
    * @param value the flag value
    */
  case class MutableHolder[T](var value: T)
}
