package dx.core.io

import java.nio.charset.Charset
import java.nio.file.Path

import dx.api.{DxApi, DxFile}
import wdlTools.util.{AbstractFileSource, FileAccessProtocol, FileSource, RealFileSource, Util}

import scala.io.Source

case class DxFileSource(value: String, dxFile: DxFile, dxApi: DxApi, override val encoding: Charset)
    extends AbstractFileSource(encoding)
    with RealFileSource {
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
  * @param encoding character encoding, for resolving binary data.
  */
case class DxFileAccessProtocol(dxApi: DxApi, encoding: Charset = Util.DefaultEncoding)
    extends FileAccessProtocol {
  val prefixes = Vector(DxFileAccessProtocol.DX_URI_PREFIX)
  private var uriToFileSource: Map[String, DxFileSource] = Map.empty

  private def resolveFileUri(uri: String): DxFile = {
    dxApi.resolveDxUrlFile(uri.split("::")(0))
  }

  override def resolve(uri: String): DxFileSource = {
    // First search in the fileInfoList. This may save us an API call.
    uriToFileSource.get(uri) match {
      case Some(src) => src
      case None =>
        val dxFile = resolveFileUri(uri)
        val src = DxFileSource(uri, dxFile, dxApi, encoding)
        uriToFileSource += (uri -> src)
        src
    }
  }

  def resolveNoCache(uri: String): FileSource = {
    DxFileSource(uri, resolveFileUri(uri), dxApi, encoding)
  }

  def fromDxFile(dxFile: DxFile): DxFileSource = {
    DxFileSource(Furl.fromDxFile(dxFile, Map.empty).value, dxFile, dxApi, encoding)
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
