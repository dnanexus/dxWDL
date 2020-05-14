package dxWDL.util

//import dxWDL.base.{AppInternalException, Utils}
import dxWDL.dx.{DxFileDescribe, DxFile}

case class DxIoFunctions(fileInfoDir: Map[String, (DxFile, DxFileDescribe)],
                         config: DxPathConfig,
                         runtimeDebugLevel: Int)
