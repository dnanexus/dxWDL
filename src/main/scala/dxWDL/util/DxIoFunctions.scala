package dxWDL.util

import wdlTools.eval.FileAccessProtocol
import dxWDL.dx.{DxFileDescribe, DxFile, DxPath}

case class DxIoFunctions(fileInfoDir: Map[String, (DxFile, DxFileDescribe)],
                         config: DxPathConfig,
                         runtimeDebugLevel: Int) extends FileAccessProtocol {
  val prefixes = Vector("dx")

  // Get the size of the file in bytes
  def size(dxFilePath : String) : Long = {
    // First search in the fileInfoList. This
    // may save us an API call
    fileInfoDir.get(dxFilePath) match {
      case None => ()
      case Some((dxFile, dxDesc)) =>
        return dxDesc.size
    }

    // file isn't in the cache, we need to describe it with an API call.
    val dxFile = DxPath.resolveDxURLFile(dxFilePath)
    val dxDesc = dxFile.describe()
    dxDesc.size
  }

  // Read the entire file into a string
  def readFile(path : String) : String = ???
}
