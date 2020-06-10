package dxWDL.util

import java.net.URI
import wdlTools.eval.FileAccessProtocol
import dxWDL.base.Utils
import dxWDL.dx.{DxFileDescribe, DxFile, DxPath, DxUtils}

case class DxIoFunctions(fileInfoDir: Map[String, (DxFile, DxFileDescribe)],
                         config: DxPathConfig,
                         runtimeDebugLevel: Int) extends FileAccessProtocol {
  private val verbose = runtimeDebugLevel >= 1

  val prefixes = Vector("dx")

  // Get the size of the file in bytes
  def size(uri : URI) : Long = {
    val path = uri.getPath
    Utils.appletLog(verbose, s"DxIoFunctions size(${path})")

    // First search in the fileInfoList. This
    // may save us an API call
    fileInfoDir.get(path) match {
      case None => ()
      case Some((dxFile, dxDesc)) =>
        return dxDesc.size
    }

    // file isn't in the cache, we need to describe it with an API call.
    //val dxFile = DxPath.resolveDxURLFile(path)
    val furl = Furl.parse(path)
    val dxFile = furl match {
      case FurlLocal(_) =>
        throw new Exception(s"Sanity: ${path} should be a dnanexus file, but it is a local file instead")
      case FurlDx(_, dxProj, dxFile) =>
        dxFile
    }
    val dxDesc = dxFile.describe()
    dxDesc.size
  }

  // Read the entire file into a string
  def readFile(uri : URI) : String = {
    val path = uri.getPath
    Utils.appletLog(verbose, s"DxIoFunctions readFile(${path})")

    val dxFile =
      fileInfoDir.get(path) match {
        case None =>
          DxPath.resolveDxURLFile(path)
        case Some((dxFile, _)) =>
          dxFile
      }
    DxUtils.downloadString(dxFile, false)
  }
}
