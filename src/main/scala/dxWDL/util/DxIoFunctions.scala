package dxWDL.util

import java.nio.file.{Paths}
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import wom.expression.{IoFunctionSet, PathFunctionSet}
import wom.expression.IoFunctionSet.IoElement
import wom.values._

case class DxPathFunctions(config: DxPathConfig,
                           runtimeDebugLevel: Int) extends PathFunctionSet {
    /**
      * Similar to java.nio.Path.resolveSibling with
      * of == a string representation of a java.nio.Path
      */
    override def sibling(of: String, other: String): String = ???

    /**
      * Similar to java.nio.Path.isAbsolute
      */
    override def isAbsolute(path: String): Boolean = ???

    /**
      * Similar to sibling only if "of" IS an absolute path and "other" IS NOT an absolute path, otherwise return other
      */
    override def absoluteSibling(of: String, other: String): String =
        if (isAbsolute(of) && !isAbsolute(other)) sibling(of, other) else other

    /**
      * If path is relative, prefix it with the _host_ call root.
      */
    override def relativeToHostCallRoot(path: String): String = ???

    /**
      * Similar to java.nio.Path.getFileName
      */
    override def name(path: String): String = ???

    /**
      * Path to stdout
      */
    override def stdout: String = config.stdout.toString

    /**
      * Path to stderr
      */
    override def stderr: String = config.stderr.toString
}

case class DxIoFunctions(config: DxPathConfig,
                         runtimeDebugLevel: Int) extends IoFunctionSet {

    override def pathFunctions = DxPathFunctions(config, runtimeDebugLevel)

    // Functions that (possibly) necessitate I/O operation (on local, network, or cloud filesystems)
    /**
      * Read the content of a file
      * @param path path of the file to read from
      * @param maxBytes maximum number of bytes that can be read
      * @param failOnOverflow if true, the Future will fail if the files has more than maxBytes
      * @return the content of the file as a String
      */
    override def readFile(path: String, maxBytes: Option[Int], failOnOverflow: Boolean): Future[String] = {
        val content = Furl.parse(path) match {
            case FurlLocal(localPath) =>
                Utils.readFileContent(Paths.get(localPath))
            case fdx : FurlDx =>
                val (_, dxFile) = FurlDx.components(fdx)
                Utils.downloadString(dxFile)
        }
        Future(content)
    }

    /**
      * Write "content" to the specified "path" location
      */
    override def writeFile(path: String, content: String): Future[WomSingleFile] = ???

    /**
      * Creates a temporary directory. This must be in a place accessible to the backend.
      * In a world where then backend is not known at submission time this will not be sufficient.
      */
    override def createTemporaryDirectory(name: Option[String]): Future[String] = ???

    /**
      * Copy pathFrom to targetName
      * @return destination as a WomSingleFile
      */
    override def copyFile(source: String, destination: String): Future[WomSingleFile] = ???

    /**
      * Glob files and directories using the provided pattern.
      * @return the list of globbed paths
      */
    override def glob(pattern: String): Future[Seq[String]] = ???

    /**
      * Recursively list all files (and only files, not directories) under "dirPath"
      * dirPath MUST BE a directory
      * @return The list of all files under "dirPath"
      */
    override def listAllFilesUnderDirectory(dirPath: String): Future[Seq[String]] = ???

    /**
      * List entries in a directory non recursively. Includes directories
      */
    override def listDirectory(path: String)(visited: Vector[String] = Vector.empty)
            : Future[Iterator[IoElement]] = {
        ???
    }

    /**
      * Return true if path points to a directory, false otherwise
      */
    override def isDirectory(path: String): Future[Boolean] = ???

    /**
      * Return the size of the file located at "path"
      */
    override def size(path: String): Future[Long] = ???

    /**
      * To map/flatMap over IO results
      */
    override def ec: ExecutionContext = scala.concurrent.ExecutionContext.global
}
