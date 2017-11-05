package dxWDL

import com.dnanexus.DXFile
import java.io.PrintStream
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, FileSystems, Path, Paths, PathMatcher}
import scala.util.{Try, Success, Failure}
import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import spray.json._
import Utils.{dxFileOfJsValue, downloadFile, getMetaDirPath, jsValueOfJsonNode,
    DX_FUNCTIONS_FILES, readFileContent, writeFileContent}
import wdl4s.wdl.expression.WdlStandardLibraryFunctions
import wdl4s.wdl.TsvSerializable
import wdl4s.wdl.values._


object DxFunctions extends WdlStandardLibraryFunctions {
    // Stream where to emit debugging information. By default,
    // goes to stderr on the instance. Requires reconfiguration
    // in unit test environments
    var errStream: PrintStream = System.err
    def setErrStream [T <: PrintStream] (err: T) = {
        errStream = err
    }

    lazy val dxHomeDir:Path = {
        //val d = System.getProperty("user.home")
        Paths.get(Utils.DX_HOME)
    }

    // Files that have to be downloaded before read.
    // We download them once, and then remove them from the hashtable.
    private val remoteFiles = HashMap.empty[String, DXFile]

    // Checkpoint the mapping tables
    //
    // This is done when closing the Runner. For example, between
    // the task-runner prolog and epilog.
    //
    // Convert the [localized] table to JSON, and write it
    // out to a checkpoint file.
    def freeze() : Unit = {
        val m:Map[String, JsValue] = remoteFiles.map{ case (path, dxFile) =>
            path.toString -> jsValueOfJsonNode(dxFile.getLinkAsJson)
        }.toMap
        val buf = JsObject(m).prettyPrint
        val path = getMetaDirPath().resolve(DX_FUNCTIONS_FILES)
        writeFileContent(path, buf)
    }

    // Read the checkpoint, and repopulate the table
    def unfreeze() : Unit = {
        val path = getMetaDirPath().resolve(DX_FUNCTIONS_FILES)
        val buf = readFileContent(path)
        val m: Map[String, DXFile] = buf.parseJson match {
            case JsObject(fields) =>
                fields.map{
                    case (k,vJs) => k -> dxFileOfJsValue(vJs)
                }.toMap
            case other =>
                throw new Exception(s"Deserialization error, checkpoint=${other.prettyPrint}")
        }

        // repopulate tables.
        m.foreach{ case(p, dxFile) =>
            remoteFiles(p) = dxFile
        }
    }


    def registerRemoteFile(path: String, dxfile: DXFile) = {
        remoteFiles(path) = dxfile
    }

    def isRemote(dxFile:DXFile) : Boolean = {
        // Note: performs a linear scan
        remoteFiles.exists{ case (_, dxf) => dxf == dxFile }
    }

    def unregisterRemoteFile(path: String) = {
        if (remoteFiles contains path)
            remoteFiles.remove(path)
    }

    // Make sure a file is on local disk. Download it if
    // necessary.
    private def handleRemoteFile(path: String) = {
        remoteFiles.get(path) match {
            case Some(dxFile) =>
                // File has not been downloaded yet.
                // Transfer it through the network, and store
                // locally.
                val p:Path = Paths.get(path)
                if (Files.exists(p)) {
                    errStream.println(s"Lazy download for ${p} invoked")
                    // Remove the empty file that is a stand-in for the
                    // real file.
                    Files.delete(p)
                }
                downloadFile(p, dxFile)
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

    // This creates the [path] argument used in the [glob] method below.
    override def globPath(pattern: String) : String = {
        pattern
    }

    // Search for the pattern in the home directory. The
    // [path] argument is unused, as in Cromwell (see
    // cromwell/backend/src/main/scala/cromwell/backend/io/GlobFunctions.scala).
    //
    override def glob(path: String, pattern: String): Seq[String] = {
        errStream.println(s"DxFunctions.glob(${pattern})")
        val baseDir: Path = dxHomeDir
        val matcher:PathMatcher = FileSystems.getDefault()
            .getPathMatcher(s"glob:${baseDir.toString}/${pattern}")
        val retval =
            if (!Files.exists(baseDir))
                Seq[String]()
            else
                Files.walk(baseDir).iterator().asScala
                    .filter(Files.isRegularFile(_))
                    .filter(matcher.matches(_))
                    .map(_.toString)
                    .toSeq
        errStream.println(s"${retval}")
        retval
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
