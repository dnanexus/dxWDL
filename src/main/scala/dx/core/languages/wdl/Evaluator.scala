package dx.core.languages.wdl

import dx.core.io.DxPathConfig
import wdlTools.eval.{Eval, EvalConfig}
import wdlTools.syntax.WdlVersion
import wdlTools.types.{TypeCheckingRegime, TypeOptions}
import wdlTools.util.{FileSourceResolver, Logger}

object Evaluator {
  def make(dxPathConfig: DxPathConfig,
           fileResolver: FileSourceResolver,
           wdlVersion: WdlVersion): Eval = {
    val evalOpts = TypeOptions(fileResolver,
                               typeChecking = TypeCheckingRegime.Strict,
                               antlr4Trace = false,
                               logger = Logger.Quiet)

    val evalCfg = EvalConfig(
        dxPathConfig.homeDir,
        dxPathConfig.tmpDir,
        dxPathConfig.stdout,
        dxPathConfig.stderr,
        fileResolver
    )

    Eval(evalOpts, evalCfg, wdlVersion)
  }
}
