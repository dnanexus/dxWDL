package dxWDL.util

import com.dnanexus.DXFile
import dxWDL.util.Utils
import java.io.PrintStream
//import java.nio.charset.StandardCharsets
import java.nio.file.{Files, FileSystems, Path, Paths, PathMatcher}
import scala.util.{Try, Success, Failure}
import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import spray.json._
import wdl4s.parser.MemoryUnit
import wdl.draft2.model.expression.WdlStandardLibraryFunctions
//import wom.TsvSerializable
import wom.values._
import wom.types._


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
            path.toString -> Utils.jsValueOfJsonNode(dxFile.getLinkAsJson)
        }.toMap
        val buf = JsObject(m).prettyPrint
        val path = Utils.getMetaDirPath().resolve(Utils.DX_FUNCTIONS_FILES)
        Utils.writeFileContent(path, buf)
    }

    // Read the checkpoint, and repopulate the table
    def unfreeze() : Unit = {
        val path = Utils.getMetaDirPath().resolve(Utils.DX_FUNCTIONS_FILES)
        val buf = Utils.readFileContent(path)
        val m: Map[String, DXFile] = buf.parseJson match {
            case JsObject(fields) =>
                fields.map{
                    case (k,vJs) => k -> Utils.dxFileFromJsValue(vJs)
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
                Utils.downloadFile(p, dxFile)
                remoteFiles.remove(path)
            case None => ()
        }
    }

    private def getMetaDir() = {
        val metaDir = Utils.getMetaDirPath()
        Utils.safeMkdir(metaDir)
        metaDir
    }


    // Search for the pattern in the home directory.
    // See Cromwell code base:
    // cromwell/backend/src/main/scala/cromwell/backend/io/GlobFunctions.scala.
    //
    override def globHelper(pattern: String): Seq[String] = {
        errStream.println(s"DxFunctions.glob(${pattern})")
        val baseDir: Path = dxHomeDir
        val matcher:PathMatcher = FileSystems.getDefault()
            .getPathMatcher(s"glob:${baseDir.toString}/${pattern}")
        val retval =
            if (!Files.exists(baseDir)) {
                Seq.empty[String]
            } else {
                val files = Files.walk(baseDir).iterator().asScala
                    .filter(Files.isRegularFile(_))
                    .filter(matcher.matches(_))
                    .map(_.toString)
                    .toSeq
                files.sorted
            }
        errStream.println(s"${retval}")
        retval
    }

    protected def fileSizeLimitationConfig: wdl.shared.FileSizeLimitationConfig = {
        wdl.shared.FileSizeLimitationConfig.default
    }

    // We ignore the size limit
    override def readFile(path: String, sizeLimit: Int): String = {
        if (path.startsWith(Utils.DX_URL_PREFIX)) {
            // A non localized file
            val dxFile = DxPath.lookupDxURLFile(path)
            Utils.downloadString(dxFile)
        } else {
            handleRemoteFile(path)

            // The file is on the local disk, we can read it with regular IO
            Utils.readFileContent(Paths.get(path))
        }
    }

    override def writeFile(path: String, content: String): Try[WomFile] = {
        if (path.startsWith(Utils.DX_URL_PREFIX)) {
            return Failure(new Exception("Cannot write non local file"))
        }
        try {
            Utils.writeFileContent(Paths.get(path), content)
            Success(WomSingleFile(path))
        } catch {
            case e: Throwable =>
                Failure(e)
        }
    }

    override def stdout(params: Seq[Try[WomValue]]): Try[WomFile] = {
        val stdoutPath = getMetaDir().resolve("stdout")
        if (!Files.exists(stdoutPath))
            Utils.writeFileContent(stdoutPath, "")
        Success(WomSingleFile(stdoutPath.toString))
    }

    override def stderr(params: Seq[Try[WomValue]]): Try[WomFile] = {
        val stderrPath = getMetaDir().resolve("stderr")
        if (!Files.exists(stderrPath))
            Utils.writeFileContent(stderrPath, "")
        Success(WomSingleFile(stderrPath.toString))
    }


    override def size(params: Seq[Try[WomValue]]): Try[WomFloat] = {
        // Inner function: is this a file type, or an optional containing a file type?
        def isOptionalOfFileType(wdlType: WomType): Boolean = wdlType match {
            case WomSingleFileType => true
            case WomStringType => true
            case WomOptionalType(inner) => isOptionalOfFileType(inner)
            case _ => false
        }

        def getFileSize(fileName: String) : Double = {
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
            fSize.toDouble
        }

        // Inner function: Get the file size, allowing for unpacking of optionals
        def optionalSafeFileSize(value: WomValue): Double = value match {
            case WomString(f) =>
                getFileSize(f)
            case f if f.isInstanceOf[WomFile] =>
                getFileSize(f.valueString)
            case WomOptionalValue(_, Some(o)) => optionalSafeFileSize(o)
            case WomOptionalValue(f, None) if isOptionalOfFileType(f) => 0d
            case _ => throw new Exception(
                s"The 'size' method expects a 'File' or 'File?' argument but instead got ${value.womType}.")
        }

        Try {
            params match {
                case _ if params.length == 1 =>
                    val fileSize = optionalSafeFileSize(params.head.get)
                    WomFloat(fileSize)
                case _ if params.length == 2 =>
                    val fileSize:Double = optionalSafeFileSize(params.head.get)
                    val unit:Double = params.tail.head match {
                        case Success(WomString(suffix)) =>
                            MemoryUnit.fromSuffix(suffix).bytes.toDouble
                        case other => throw new IllegalArgumentException(s"The unit must a string type ${other}")
                    }
                    WomFloat((fileSize/unit).toFloat)
                case _ => throw new UnsupportedOperationException(s"Expected one or two parameters but got ${params.length} instead.")
            }
        }
    }
}
