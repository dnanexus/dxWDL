package dx.core.languages.wdl

import dx.core.io.DxPathConfig
import wdlTools.eval.{Eval, EvalPaths}
import wdlTools.syntax.WdlVersion
import wdlTools.util.{FileSourceResolver, Logger}

object Evaluator {
  def make(dxPathConfig: DxPathConfig,
           fileResolver: FileSourceResolver,
           wdlVersion: WdlVersion): Eval = {
    val evalPaths = EvalPaths(
        dxPathConfig.homeDir,
        dxPathConfig.tmpDir
    )
    Eval(evalPaths, wdlVersion, fileResolver, Logger.Quiet)
  }
}
