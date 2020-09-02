package dx.core.languages.wdl

import dx.core.io.DxPathConfig
import wdlTools.eval.{Eval, EvalPaths}
import wdlTools.syntax.WdlVersion
import wdlTools.util.{FileSourceResolver, Logger}

object Evaluator {
  def make(dxPathConfig: DxPathConfig,
           fileResolver: FileSourceResolver,
           wdlVersion: WdlVersion): Eval = {
    val evalCfg = EvalPaths(
        dxPathConfig.homeDir,
        dxPathConfig.tmpDir
    )
    Eval(evalCfg, Some(wdlVersion), fileResolver, Logger.Quiet)
  }
}
