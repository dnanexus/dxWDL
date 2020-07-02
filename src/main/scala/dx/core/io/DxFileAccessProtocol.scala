package dx.core.io

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

case class DxFileCache(files: Vector[DxFile] = Vector.empty) {
  private lazy val fileAndProjectIdToFileCache =
    files.filter(_.project.isDefined).map(f => (f.id, f.project.get.id) -> f).toMap
  private lazy val fileIdToFileCache = files.groupBy(_.id).flatMap {
    case (id, files) if files.size == 1 => Some(id -> files.head)
    case (id, files) if files.size == 2 =>
      val filesWithProj = files.filter(_.project.isDefined)
      if (filesWithProj.size == 1) {
        Some(id -> filesWithProj.head)
      } else {
        None
      }
    case _ => None
  }

  def get(fileId: String, projectId: Option[String] = None): Option[DxFile] = {
    projectId
      .flatMap(projId => fileAndProjectIdToFileCache.get(fileId, projId))
      .orElse(fileIdToFileCache.get(fileId))
  }

  def get(file: DxFile): DxFile = {
    // next check to see if the file is in either cache
    getCached(file).getOrElse(file)
  }

  def getCached(file: DxFile): Option[DxFile] = {
    file.project
      .flatMap(proj => fileAndProjectIdToFileCache.get((file.id, proj.id)))
      .orElse(fileIdToFileCache.get(file.id))
  }
}

/**
  * Implementation of FileAccessProtocol for dx:// URIs
  * @param dxApi DxApi instance.
  * @param dxFileCache Vector of DxFiles that have already been described (matching is done by file+project IDs)
  * @param encoding character encoding, for resolving binary data.
  */
case class DxFileAccessProtocol(dxApi: DxApi,
                                dxFileCache: Vector[DxFile] = Vector.empty,
                                encoding: Charset = Util.DefaultEncoding)
    extends FileAccessProtocol {
  val prefixes = Vector(DxFileAccessProtocol.DX_URI_PREFIX)
  private var uriToFileSource: Map[String, DxFileSource] = Map.empty
  private val fileCache = DxFileCache(dxFileCache)

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

  override def resolve(uri: String): DxFileSource = {
    // First search in the fileInfoList. This may save us an API call
    // ignore the`exists` parameter because it doesn't make sense to have
    // a
    uriToFileSource.get(uri) match {
      case Some(src) => src
      case None =>
        val dxFile = fileCache.get(resolveFileUri(uri))
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
