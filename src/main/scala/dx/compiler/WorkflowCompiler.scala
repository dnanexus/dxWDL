package dx.compiler

import dx.api.DxApi
import dx.compiler.ir.DockerRegistry
import dx.core.io.DxPathConfig
import dx.core.ir.Workflow
import dx.core.languages.wdl.ParameterLinkSerde
import spray.json.JsValue
import wdlTools.util.Logger

case class WorkflowCompiler(runtimePathConfig: DxPathConfig,
                            runtimeTraceLevel: Int,
                            dockerRegistry: Option[DockerRegistry],
                            parameterLinkSerializer: ParameterLinkSerde,
                            dxApi: DxApi = DxApi.get,
                            logger: Logger = Logger.get) {
  def apply(
      workflow: Workflow,
      dependencyLinks: Map[String, CompiledExecutable]
  ): (Map[String, JsValue], String) = {}
}
