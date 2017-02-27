package dxWDL

import java.nio.charset.StandardCharsets
import java.nio.file.{Path, Paths, Files}
import scala.util.{Try, Success, Failure}
import wdl4s.expression.{WdlStandardLibraryFunctionsType, WdlStandardLibraryFunctions}
import wdl4s.TsvSerializable
import wdl4s.types._
import wdl4s.values._

case object DxFunctions extends WdlStandardLibraryFunctions {
    def getMetaDir() = {
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

    override def glob(path: String, pattern: String): Seq[String] =
        throw new NotImplementedError()

    override def readFile(path: String): String =
        Utils.readFileContent(Paths.get(path))

    override def writeTempFile(path: String,
                               prefix: String,
                               suffix: String,
                               content: String): String =
        throw new NotImplementedError()

    override def stdout(params: Seq[Try[WdlValue]]): Try[WdlFile] = {
        val stdoutPath = getMetaDir().resolve("stdout")
        try {
            Success(WdlFile(stdoutPath.toString))
        } catch {
            case e : Throwable =>
                Utils.writeFileContent(stdoutPath, "")
                Success(WdlFile(stdoutPath.toString))
        }
    }

    override def stderr(params: Seq[Try[WdlValue]]): Try[WdlFile] = {
        val stderrPath = getMetaDir().resolve("stderr")
        try {
            Success(WdlFile(stderrPath.toString))
        } catch {
            case e : Throwable =>
                Utils.writeFileContent(stderrPath, "")
                Success(WdlFile(stderrPath.toString))
        }
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
        Failure(new NotImplementedError(s"write_json() not implemented yet"))

    override def size(params: Seq[Try[WdlValue]]): Try[WdlFloat] =
        Failure(new NotImplementedError())

    private def stringOf(x: WdlValue) : String = {
        x match {
            case WdlString(s) => s
            case WdlSingleFile(path) => path
            case _ => throw new Exception(s"Not file/string type ${x.typeName} ${x.toWdlString}")
        }
    }

    override def sub(params: Seq[Try[WdlValue]]): Try[WdlString] =
        params.size match {
            case 3 => (params(0), params(1), params(2)) match {
                case (Success(x), Success(y), Success(z)) =>
                    try {
                        val str = stringOf(x)
                        val pattern = stringOf(y)
                        val replace = stringOf(z)
                        Success(WdlString(pattern.r.replaceAllIn(str, replace)))
                    } catch {
                        case e : Throwable =>
                            val msg = Utils.exceptionToString(e)
                            Failure(new Exception(s"sub requires three file/string arguments, ${msg}"))
                    }
                case _ =>
                    Failure(new Exception("sub requires three valid arguments"))
            }
            case n => Failure(
                new IllegalArgumentException(
                    s"""|Invalid number of parameters for engine function sub: $n.
                        |sub takes exactly 3 parameters.""".stripMargin.trim))
        }
}
