package dx.core.io

import java.nio.charset.Charset
import java.nio.file.Path

import dx.api.{DxApi, DxFile, DxFileDescribe, DxProject}
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

case class DxFileDescCache(files: Vector[DxFile] = Vector.empty) {
  private lazy val fileAndProjectIdToDescCache: Map[(String, String), DxFileDescribe] =
    files
      .filter(f => f.hasCachedDesc && f.project.isDefined)
      .map(f => (f.id, f.project.get.id) -> f.describe())
      .toMap
  private lazy val fileIdToDescCache: Map[String, DxFileDescribe] =
    files.filter(_.hasCachedDesc).groupBy(_.id).flatMap {
      case (id, files) if files.size == 1 => Some(id -> files.head.describe())
      case (id, files) if files.size > 1 =>
        var filesWithProj = files.filter(_.project.isDefined)
        if (filesWithProj.size > 1) {
          filesWithProj = filesWithProj.filter(_.project.get.id.startsWith("project-"))
        }
        if (filesWithProj.size == 1) {
          Some(id -> filesWithProj.head.describe())
        } else {
          None
        }
      case _ => None
    }

  def get(fileId: String, projectId: Option[String] = None): Option[DxFileDescribe] = {
    projectId
      .flatMap(projId => fileAndProjectIdToDescCache.get(fileId, projId))
      .orElse(fileIdToDescCache.get(fileId))
  }

  def getCached(file: DxFile): Option[DxFileDescribe] = {
    file.project
      .flatMap(proj => fileAndProjectIdToDescCache.get((file.id, proj.id)))
      .orElse(fileIdToDescCache.get(file.id))
  }

  def updateFileFromCache(file: DxFile): DxFile = {
    if (!file.hasCachedDesc) {
      val cachedDesc = getCached(file)
      if (cachedDesc.isDefined) {
        file.cacheDescribe(cachedDesc.get)
        // make sure the DxFile and DxFileDescribe project IDs are in sync
        return file.project match {
          case Some(DxProject(_, id)) if id == cachedDesc.get.project =>
            file
          case _ =>
            DxFile(file.dxApi, file.id, Some(DxProject(file.dxApi, cachedDesc.get.project)))
        }
      }
    }
    file
  }
}

object DxFileDescCache {
  lazy val empty: DxFileDescCache = DxFileDescCache()
}

/**
  * Implementation of FileAccessProtocol for dx:// URIs
  * @param dxApi DxApi instance.
  * @param dxFileCache Vector of DxFiles that have already been described (matching is done by file+project IDs)
  * @param encoding character encoding, for resolving binary data.
  */
case class DxFileAccessProtocol(dxApi: DxApi,
                                dxFileCache: DxFileDescCache = DxFileDescCache.empty,
                                encoding: Charset = Util.DefaultEncoding)
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
        val dxFile = dxFileCache.updateFileFromCache(resolveFileUri(uri))
        val src = DxFileSource(uri, dxFile, dxApi, encoding)
        uriToFileSource += (uri -> src)
        src
    }
  }

  def resolveNoCache(uri: String): FileSource = {
    DxFileSource(uri, resolveFileUri(uri), dxApi, encoding)
  }

  def fromDxFile(dxFile: DxFile): DxFileSource = {
    DxFileSource(dxFile.asUri, dxFile, dxApi, encoding)
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
