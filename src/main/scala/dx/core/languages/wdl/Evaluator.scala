package dx.core.languages.wdl

import wdlTools.eval.{Eval, EvalConfig}
import wdlTools.syntax.WdlVersion
import wdlTools.types.{TypeCheckingRegime, TypeOptions}
import wdlTools.util.Logger

object Evaluator {
  def make(dxIoFunctions: DxFileAccessProtocol, wdlVersion: WdlVersion): Eval = {
    val evalOpts = TypeOptions(typeChecking = TypeCheckingRegime.Strict,
                               antlr4Trace = false,
                               localDirectories = Vector.empty,
                               logger = Logger.Quiet)

    val evalCfg = EvalConfig.make(
        dxIoFunctions.config.homeDir,
        dxIoFunctions.config.tmpDir,
        dxIoFunctions.config.stdout,
        dxIoFunctions.config.stderr,
        // Add support for the dx cloud file access protocols
        Vector(dxIoFunctions)
    )

    Eval(evalOpts, evalCfg, wdlVersion, None)
  }
}
