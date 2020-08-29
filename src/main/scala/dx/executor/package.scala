package dx

import java.nio.file.Path

import dx.core.io.DxPathConfig
import wdlTools.eval.{Eval, EvalPaths}
import wdlTools.syntax.WdlVersion
import wdlTools.util.{Enum, FileSourceResolver, Logger}

package object executor {
  val DXFUSE_MAX_MEMORY_CONSUMPTION: Int = 300 * 1024 * 1024 // how much memory dxfuse takes
  val INTERMEDIATE_RESULTS_FOLDER = "intermediate"
  val MAX_NUM_FILES_MOVE_LIMIT = 1000
  // TODO: make this configurable - preferably read from a property on the applet
  val SCATTER_LIMIT = 500

  // Different ways of using the mini-workflow runner.
  //   Launch:     there are WDL calls, lanuch the dx:executables.
  //   Collect:    the dx:exucutables are done, collect the results.
  object RunnerWfFragmentMode extends Enum {
    type RunnerWfFragmentMode = Value
    val Launch, Collect = Value
  }

  def createEvaluator(dxPathConfig: DxPathConfig,
                      fileResolver: FileSourceResolver,
                      wdlVersion: WdlVersion): Eval = {
    val evalPaths = EvalPaths(
        dxPathConfig.homeDir,
        dxPathConfig.tmpDir
    )
    Eval(evalPaths, Some(wdlVersion), fileResolver, Logger.Quiet)
  }
}
