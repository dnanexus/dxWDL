package dx.executor

import java.nio.file.Path

import dx.api.DxApi
import dx.core.io.DxWorkerPaths
import wdlTools.util.Logger

case class WorkflowMeta(homeDir: Path = DxWorkerPaths.HomeDir,
                        dxApi: DxApi = DxApi.get,
                        logger: Logger = Logger.get)
    extends JobMeta(homeDir, dxApi, logger) {}
