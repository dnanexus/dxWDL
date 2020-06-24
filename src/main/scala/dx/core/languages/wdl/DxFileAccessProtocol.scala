package dx.core.languages.wdl

import java.net.URI

import dx.core.io.{DxPathConfig, Furl, FurlDx, FurlLocal}
import dx.api.{DxApi, DxFile, DxFileDescribe}
import wdlTools.eval.FileAccessProtocol

case class DxFileAccessProtocol(dxApi: DxApi,
                                fileInfoDir: Map[String, (DxFile, DxFileDescribe)],
                                config: DxPathConfig)
    extends FileAccessProtocol {
  val prefixes = Vector("dx")

  private def getDxFile(path: String,
                        describe: Boolean = false): (DxFile, Option[DxFileDescribe]) = {
    // First search in the fileInfoList. This
    // may save us an API call
    fileInfoDir.get(path) match {
      case None => ()
      case Some((dxFile, dxDesc)) =>
        return (dxFile, Some(dxDesc))
    }

    val dxFile = Furl.fromUrl(path, dxApi) match {
      case FurlLocal(_) =>
        throw new Exception(
            s"Sanity: ${path} should be a dnanexus file, but it is a local file instead"
        )
      case FurlDx(_, _, dxFile) =>
        dxFile
    }
    if (describe) {
      (dxFile, Some(dxFile.describe()))
    } else {
      (dxFile, None)
    }
  }

  // Get the size of the file in bytes
  def size(uri: URI): Long = {
    val path = uri.toString
    dxApi.logger.appletLog(s"DxIoFunctions size(${path})")
    val (_, dxDesc) = getDxFile(path, describe = true)
    dxDesc.size
  }

  // Read the entire file into a string
  def readFile(uri: URI): String = {
    val path = uri.toString
    dxApi.logger.appletLog(s"DxIoFunctions readFile(${path})")
    val (dxFile, _) = getDxFile(path)
    dxApi.downloadString(dxFile)
  }
}
