package dxWDL.util

import wdlTools.eval.{EvalConfig, Eval}
import wdlTools.syntax.WdlVersion
import wdlTools.types.{TypeCheckingRegime, TypeOptions}

object WdlEvaluator {
  def make(dxIoFunctions: DxIoFunctions, wdlVersion: WdlVersion): Eval = {
    val evalOpts = TypeOptions(typeChecking = TypeCheckingRegime.Strict,
                               antlr4Trace = false,
                               localDirectories = Vector.empty,
                               verbosity = wdlTools.util.Verbosity.Quiet)

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
