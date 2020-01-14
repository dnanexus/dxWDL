package dxWDL.util

import java.nio.file.{Files, FileSystems, Paths, PathMatcher}
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import wom.expression.{IoFunctionSet, PathFunctionSet}
import wom.expression.IoFunctionSet.IoElement
import wom.values._

import dxWDL.base.{AppInternalException, Utils}
import dxWDL.dx.{DxFileDescribe, DxFile, DxUtils}

case class DxPathFunctions(fileInfoDir: Map[String, (DxFile, DxFileDescribe)],
                           config: DxPathConfig,
                           runtimeDebugLevel: Int)
    extends PathFunctionSet {

  /**
    * Similar to java.nio.Path.resolveSibling with
    * of == a string representation of a java.nio.Path
    */
  override def sibling(of: String, other: String): String = ???

  /**
    * Similar to java.nio.Path.isAbsolute
    */
  override def isAbsolute(path: String): Boolean = {
    Furl.parse(path) match {
      case FurlLocal(localPath) =>
        val p = Paths.get(localPath)
        p.isAbsolute
      case fdx: FurlDx =>
        false
    }
  }

  /**
    * Similar to sibling only if "of" IS an absolute path and "other" IS NOT an absolute path, otherwise return other
    */
  override def absoluteSibling(of: String, other: String): String =
    if (isAbsolute(of) && !isAbsolute(other)) sibling(of, other) else other

  /**
    * If path is relative, prefix it with the _host_ call root.
    */
  override def relativeToHostCallRoot(path: String): String =
    throw new AppInternalException("relativeToHostCallRoot: not implemented in DxIoFunctions")

  /**
    * Similar to java.nio.Path.getFileName
    */
  override def name(path: String): String = {
    Furl.parse(path) match {
      case FurlLocal(localPath) =>
        val p = Paths.get(localPath)
        p.getFileName.toString
      case fdx: FurlDx =>
        fileInfoDir.get(fdx.dxFile.id) match {
          case None =>
            // perform an API call to get the file name
            fdx.dxFile.describe().name
          case Some((_, desc)) =>
            desc.name
        }
    }
  }

  /**
    * Path to stdout
    */
  override def stdout: String = config.stdout.toString

  /**
    * Path to stderr
    */
  override def stderr: String = config.stderr.toString
}

case class DxIoFunctions(fileInfoDir: Map[String, (DxFile, DxFileDescribe)],
                         config: DxPathConfig,
                         runtimeDebugLevel: Int)
    extends IoFunctionSet {
  private val verbose = runtimeDebugLevel >= 1
  override def pathFunctions = new DxPathFunctions(fileInfoDir, config, runtimeDebugLevel)

  // Functions that (possibly) necessitate I/O operation (on local, network, or cloud filesystems)
  /**
    * Read the content of a file
    * @param path path of the file to read from
    * @param maxBytes maximum number of bytes that can be read
    * @param failOnOverflow if true, the Future will fail if the files has more than maxBytes
    * @return the content of the file as a String
    */
  override def readFile(path: String,
                        maxBytes: Option[Int],
                        failOnOverflow: Boolean): Future[String] = {
    val content = Furl.parse(path) match {
      case FurlLocal(localPath) =>
        val p = Paths.get(localPath)
        if (Files.exists(p)) {
          Utils.readFileContent(p)
        } else {
          // stdout and stderr are "defined" as empty, even
          // if they have not been created.
          if (p == config.stdout || p == config.stderr)
            ""
          else
            throw new Exception(s"File ${p} does not exist")
        }
      case fdx: FurlDx =>
        DxUtils.downloadString(fdx.dxFile, verbose)
    }
    Future(content)
  }

  /**
    * Write "content" to the specified "path" location
    */
  override def writeFile(path: String, content: String): Future[WomSingleFile] = {
    Furl.parse(path) match {
      case FurlLocal(localPath) =>
        val p = Paths.get(localPath)
        Utils.writeFileContent(p, content)
        Future(WomSingleFile(localPath))
      case fdx: FurlDx =>
        throw new AppInternalException(
            s"writeFile: not implemented in DxIoFunctions for cloud files (${fdx})"
        )
    }
  }

  /**
    * Creates a temporary directory. This must be in a place accessible to the backend.
    * In a world where then backend is not known at submission time this will not be sufficient.
    */
  override def createTemporaryDirectory(name: Option[String]): Future[String] =
    throw new AppInternalException("createTemporaryDirectory: not implemented in DxIoFunctions")

  /**
    * Copy pathFrom to targetName
    * @return destination as a WomSingleFile
    */
  override def copyFile(source: String, destination: String): Future[WomSingleFile] =
    throw new AppInternalException("copyFile: not implemented in DxIoFunctions")

  /**
    * Glob files and directories using the provided pattern.
    * @return the list of globbed paths
    */
  override def glob(pattern: String): Future[Seq[String]] = {
    Utils.appletLog(config.verbose, s"glob(${pattern})")
    val baseDir = config.homeDir
    val matcher: PathMatcher = FileSystems
      .getDefault()
      .getPathMatcher(s"glob:${baseDir.toString}/${pattern}")
    val retval =
      if (!Files.exists(baseDir)) {
        Seq.empty[String]
      } else {
        val files = Files
          .walk(baseDir)
          .iterator()
          .asScala
          .filter(Files.isRegularFile(_))
          .filter(matcher.matches(_))
          .map(_.toString)
          .toSeq
        files.sorted
      }
    Utils.appletLog(config.verbose, s"""glob results=${retval.mkString("\n")}""")
    Future(retval)
  }

  /**
    * Recursively list all files (and only files, not directories) under "dirPath"
    * dirPath MUST BE a directory
    * @return The list of all files under "dirPath"
    */
  override def listAllFilesUnderDirectory(dirPath: String): Future[Seq[String]] =
    throw new AppInternalException("listAllFilesUnderDirectory: not implemented in DxIoFunctions")

  /**
    * List entries in a directory non recursively. Includes directories
    */
  override def listDirectory(
      path: String
  )(visited: Vector[String] = Vector.empty): Future[Iterator[IoElement]] =
    throw new AppInternalException("listDirectory: not implemented in DxIoFunctions")

  /**
    * Return true if path points to a directory, false otherwise
    */
  override def isDirectory(path: String): Future[Boolean] = {
    Furl.parse(path) match {
      case FurlLocal(localPath) =>
        val p = Paths.get(localPath)
        Future(p.toFile.isDirectory)
      case fdx: FurlDx =>
        throw new AppInternalException(
            s"isDirectory: cannot be applied to non local file (${path})"
        )
    }
  }

  /**
    * Return the size of the file located at "path"
    */
  override def size(path: String): Future[Long] = {
    Furl.parse(path) match {
      case FurlLocal(localPath) =>
        val p = Paths.get(localPath)
        Future(p.toFile.length)
      case fdx: FurlDx =>
        val ssize: Long = pathFunctions.fileInfoDir.get(fdx.dxFile.id) match {
          case None =>
            // perform an API call to get the size
            fdx.dxFile.describe().size
          case Some((_, desc)) =>
            desc.size
        }
        Future(ssize)
    }
  }

  /**
    * To map/flatMap over IO results
    */
  override def ec: ExecutionContext = scala.concurrent.ExecutionContext.global
}
