package dx.core.languages.wdl

import java.nio.charset.Charset
import java.nio.file.Path

import dx.api.{DxApi, DxFile, DxProject}
import wdlTools.util.{AbstractFileSource, FileAccessProtocol, FileSource, Util}

import scala.io.Source

case class DxFileSource(value: String,
                        dxFile: DxFile,
                        dxProject: Option[DxProject],
                        dxApi: DxApi,
                        override val encoding: Charset)
    extends AbstractFileSource(encoding) {
  override def toString: String = dxFile.asUri

  override lazy val localPath: Path = Util.getPath(dxFile.describe().name)

  override lazy val size: Long = dxFile.describe().size

  override lazy val readString: String = dxApi.downloadString(dxFile)

  override def readLines: Vector[String] = {
    Source.fromString(readString).getLines().toVector
  }

  override def readBytes: Array[Byte] = readString.getBytes(encoding)

  override protected def localizeTo(file: Path): Unit = {
    dxApi.downloadFile(file, dxFile)
  }
}

/**
  * Implementation of FileAccessProtocol for dx:// URIs
  * @param dxApi DxApi instance.
  * @param dxFileCache mapping of file IDs to DxFile objects, for files that have
  *                    already been described.
  * @param encoding character encoding, for resolving binary data.
  */
case class DxFileAccessProtocol(dxApi: DxApi,
                                dxFileCache: Map[String, DxFile] = Map.empty,
                                encoding: Charset = Util.DefaultEncoding)
    extends FileAccessProtocol {
  val prefixes = Vector(DxFileAccessProtocol.DX_URI_PREFIX)
  private var uriToFileSource: Map[String, DxFileSource] = Map.empty

  private def resolveFileUri(uri: String): DxFile = {
    dxApi.resolveDxUrlFile(uri.split("::")(0))
  }

  private def getDxProject(dxFile: DxFile): Option[DxProject] = {
    if (dxFile.project.forall(_.id.startsWith("project-"))) {
      dxFile.project
    } else {
      None
    }
  }

  override def resolve(uri: String): FileSource = {
    // First search in the fileInfoList. This
    // may save us an API call
    uriToFileSource.get(uri) match {
      case Some(src) => src
      case None =>
        val dxFile = resolveFileUri(uri) match {
          case f if dxFileCache.contains(f.id) => dxFileCache(f.id)
          case f                               => f
        }
        val dxProj = getDxProject(dxFile)
        val src = DxFileSource(uri, dxFile, dxProj, dxApi, encoding)
        uriToFileSource += (uri -> src)
        src
    }
  }

  def resolveNoCache(uri: String): FileSource = {
    val dxFile = resolveFileUri(uri)
    val dxProj = getDxProject(dxFile)
    DxFileSource(uri, dxFile, dxProj, dxApi, encoding)
  }

  def fromDxFile(dxFile: DxFile): DxFileSource = {
    val dxProj = getDxProject(dxFile)
    DxFileSource(dxFile.asUri, dxFile, dxProj, dxApi, encoding)
  }
}

object DxFileAccessProtocol {
  val DX_URI_PREFIX = "dx"

  def fromDxFile(dxFile: DxFile, protocols: Vector[FileAccessProtocol]): DxFileSource = {
    protocols
      .collectFirst {
        case dx: DxFileAccessProtocol => dx.fromDxFile(dxFile)
      }
      .getOrElse(
          throw new RuntimeException("No dx protocol")
      )
  }
}
