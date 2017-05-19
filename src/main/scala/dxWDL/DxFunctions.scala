package dxWDL

import com.dnanexus.DXFile
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, FileSystems, Path, Paths, PathMatcher}
import scala.util.{Try, Success, Failure}
import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import wdl4s.expression.{WdlStandardLibraryFunctionsType, WdlStandardLibraryFunctions}
import wdl4s.TsvSerializable
import wdl4s.types._
import wdl4s.values._

object DxFunctions extends WdlStandardLibraryFunctions {
    lazy val dxHomeDir:Path = {
        //val d = System.getProperty("user.home")
        Paths.get(Utils.DX_HOME)
    }

    // Files that have to be downloaded before read.
    // We download them once, and then remove them from the hashtable.
    var remoteFiles = HashMap.empty[String, DXFile]


    def registerRemoteFile(path: String, dxfile: DXFile) = {
        remoteFiles(path) = dxfile
    }

    // Make sure a file is on local disk. Download it if
    // necessary.
    private def handleRemoteFile(path: String) = {
        remoteFiles.get(path) match {
            case Some(dxFile) =>
                // File has not been downloaded yet.
                // Transfer it through the network, and store
                // locally.
                Utils.downloadFile(Paths.get(path), dxFile)
                remoteFiles.remove(path)
            case None => ()
        }
    }

    private def getMetaDir() = {
        val metaDir = Utils.getMetaDirPath()
        Utils.safeMkdir(metaDir)
        metaDir
    }

    private def writeContent(baseName: String, content: String): Try[WdlFile] = {
        val tmpFile = Utils.tmpDirPath.resolve(s"$baseName-${content.md5Sum}.tmp")
        Files.write(tmpFile, content.getBytes(StandardCharsets.UTF_8))
        Success(WdlFile(tmpFile.toString))
    }

    private def writeToTsv(params: Seq[Try[WdlValue]],
                           wdlClass: Class[_ <: WdlValue with TsvSerializable]) = {
        for {
            singleArgument <- extractSingleArgument("writeToTsv", params)
            downcast <- Try(wdlClass.cast(singleArgument))
            tsvSerialized <- downcast.tsvSerialize
            file <- writeContent(wdlClass.getSimpleName.toLowerCase, tsvSerialized)
        } yield file
    }

    override def globPath(pattern: String) : String = {
        pattern
    }

    // Search for the pattern in the home directory. Not clear
    // yet what to do with the [path] argument.
    override def glob(path: String, pattern: String): Seq[String] = {
        val matcher:PathMatcher = FileSystems.getDefault()
            .getPathMatcher(s"glob:${dxHomeDir.toString}/${pattern}")
        dxHomeDir.toFile.listFiles
            .filter(_.isFile)
            .map(_.toPath)
            .filter(matcher.matches(_))
            .map(_.toString)
            .toSeq
    }

    override def readFile(path: String): String = {
        handleRemoteFile(path)

        // The file is on the local disk, we can read it with regular IO
        Utils.readFileContent(Paths.get(path))
    }

    override def writeTempFile(path: String,
                               prefix: String,
                               suffix: String,
                               content: String): String =
        throw new NotImplementedError()

    override def stdout(params: Seq[Try[WdlValue]]): Try[WdlFile] = {
        val stdoutPath = getMetaDir().resolve("stdout")
        if (!Files.exists(stdoutPath))
            Utils.writeFileContent(stdoutPath, "")
        Success(WdlFile(stdoutPath.toString))
    }

    override def stderr(params: Seq[Try[WdlValue]]): Try[WdlFile] = {
        val stderrPath = getMetaDir().resolve("stderr")
        if (!Files.exists(stderrPath))
            Utils.writeFileContent(stderrPath, "")
        Success(WdlFile(stderrPath.toString))
    }

    override def read_json(params: Seq[Try[WdlValue]]): Try[WdlValue] =
        Failure(new NotImplementedError())

    // Write functions: from Cromwell backend.
    // cromwell/backend/src/main/scala/cromwell/backend/wdl/WriteFunctions.scala
    override def write_lines(params: Seq[Try[WdlValue]]): Try[WdlFile] =
        writeToTsv(params, classOf[WdlArray])
    override def write_map(params: Seq[Try[WdlValue]]): Try[WdlFile] =
        writeToTsv(params, classOf[WdlMap])
    override def write_object(params: Seq[Try[WdlValue]]): Try[WdlFile] =
        writeToTsv(params, classOf[WdlObject])
    override def write_objects(params: Seq[Try[WdlValue]]): Try[WdlFile] =
        writeToTsv(params, classOf[WdlArray])
    override def write_tsv(params: Seq[Try[WdlValue]]): Try[WdlFile] =
        writeToTsv(params, classOf[WdlArray])
    override def write_json(params: Seq[Try[WdlValue]]): Try[WdlFile] =
        Failure(new NotImplementedError(s"write_json()"))

    override def size(params: Seq[Try[WdlValue]]): Try[WdlFloat] = {
        // Extract the filename/path argument
        try {
            val fileName:String = params match {
                case _ if params.length == 1 =>
                    params.head.get match {
                        case WdlSingleFile(s) => s
                        case WdlString(s) => s
                        case x => throw new AppException(s"size operator cannot be applied to ${x.toWdlString}")
                    }
                case _ =>
                    throw new IllegalArgumentException(
                        s"""|Invalid number of parameters for engine function size: ${params.length}.
                            |size takes one parameter.""".stripMargin.trim)
            }

            // If this is not an absolute path, we assume the file
            // is located in the DX home directory
            val path:String =
                if (fileName.startsWith("/")) fileName
                else dxHomeDir.resolve(fileName).toString
            val fSize:Long = remoteFiles.get(path) match {
                case Some(dxFile) =>
                    // File has not been downloaded yet.
                    // Query the platform how big it is; do not download it.
                    dxFile.describe().getSize()
                case None =>
                    // File is local
                    val p = Paths.get(fileName)
                    p.toFile.length
            }
            Success(WdlFloat(fSize.toFloat))
        } catch {
            case e : Throwable => Failure(e)
        }
    }


    private def stringOf(x: WdlValue) : String = {
        x match {
            case WdlString(s) => s
            case WdlSingleFile(path) => path
            case _ => throw new Exception(s"Not file/string type ${x.typeName} ${x.toWdlString}")
        }
    }

    private def stringOf(p: Try[WdlValue]) : String = {
        p match {
            case Success(x) => x.toWdlString
            case Failure(e) => s"Failure ${Utils.exceptionToString(e)}"
        }
    }

    override def sub(params: Seq[Try[WdlValue]]): Try[WdlString] =
        params.size match {
            case 3 => (params(0), params(1), params(2)) match {
                case (Success(x), Success(y), Success(z)) =>
                    try {
                        val (str, pattern, replace) = (stringOf(x), stringOf(y), stringOf(z))
                        Success(WdlString(pattern.r.replaceAllIn(str, replace)))
                    } catch {
                        case e : Throwable =>
                            val msg = Utils.exceptionToString(e)
                            Failure(new Exception(s"sub requires three file/string arguments, ${msg}"))
                    }
                case _ =>
                    val (p0,p1,p2) = (stringOf(params(0)), stringOf(params(1)), stringOf(params(2)))
                    Failure(new Exception(s"sub requires three valid arguments, got (${p0}, ${p1}, ${p2})"))
            }
            case n => Failure(
                new IllegalArgumentException(
                    s"""|Invalid number of parameters for engine function sub: $n.
                        |sub takes exactly 3 parameters.""".stripMargin.trim))
        }
}
