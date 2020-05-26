package dxWDL.util

import java.nio.file.{Paths}
import wdlTools.eval.WdlValues
import wdlTools.types.{TypedAbstractSyntax => TAT, WdlTypes}

import dxWDL.base.Utils

object WomValueAnalysis {
  // These are used for evaluating if a WOM expression is constant.
  // Ideally, we should not be using any of the IO functions, since
  // these checks may be part of the compilation process.
  private val tmpDir = Paths.get("/tmp")
  private val dxPathConfig = DxPathConfig(homeDir = tmpDir,
                                               metaDir = tmpDir,
                                               inputFilesDir = tmpDir,
                                               outputFilesDir = tmpDir,
                                               tmpDir = tmpDir,
                                               instanceTypeDB = null,
                                               womSourceCodeEncoded = null,
                                               stdout = null,
                                               stderr = null,
                                               dockerSubmitScript = null,
                                               script = null,
                                               rcPath = null,
                                               dockerCid = null,
                                               setupStreams = null,
                                               dxdaManifest = null,
                                               dxfuseManifest = null,
                                               dxfuseMountpoint = null,
                                               runnerTaskEnv = null,
                                               streamAllFiles = false,
                                               verbose = false)
  private val dxIoFunctions = DxIoFunctions(Map.empty, dxPathConfig, 0)

  private val evaluator : wdlTools.eval.Eval = {
    val evalOpts = wdlTools.util.Options(typeChecking = wdlTools.util.TypeCheckingRegime.Strict,
                                         antlr4Trace = false,
                                         localDirectories = Vector.empty,
                                         verbosity = wdlTools.util.Verbosity.Quiet)
    val evalCfg = wdlTools.util.EvalConfig(dxIoFunctions.config.homeDir,
                                           dxIoFunctions.config.tmpDir,
                                           dxIoFunctions.config.stdout,
                                           dxIoFunctions.config.stderr)
    wdlTools.eval.Eval(evalOpts, evalCfg, doc.version.value, None)
  }

  private def evaluateWomExpression(expr: TAT.Expr,
                                    womType: WdlTypes.T,
                                    env: Map[String, WdlValues.V]): Option[WdlValues.V] = {
    try {
      Some(evaluator.applyExprAndCoerce(expr, womType, env))
    } catch {
      case _ : Throwable => None
    }
  }

  // Examine task foo:
  //
  // task foo {
  //    command {
  //       # create file mut.vcf
  //    }
  //    output {
  //       File mutations = "mut.vcf"
  //    }
  // }
  //
  // File 'mutations' is not a constant output. It is generated,
  // read from disk, and uploaded to the platform.  A file can't
  // have a constant string as an input, this has to be a dnanexus
  // link.
  def requiresEvaluation(womType: WdlTypes.T, value: WdlValues.V): Boolean = {
    def isMutableFile(constantFileStr: String): Boolean = {
      constantFileStr match {
        case path if path.startsWith(Utils.DX_URL_PREFIX) =>
          // platform files are immutable
          false
        case path if path contains "://" =>
          throw new Exception(s"protocol not supported, cannot access ${path}")
        case _ =>
          // anything else might be mutable
          true
      }
    }

    (womType, value) match {
      // Base case: primitive types.
      case (_, WdlValues.V_Null) => false
      case (WdlTypes.T_Boolean, _)                   => false
      case (WdlTypes.T_Int, _)                   => false
      case (WdlTypes.T_Float, _)                     => false
      case (WdlTypes.T_String, _)                    => false
      case (WdlTypes.T_File, WdlValues.V_String(s))     => isMutableFile(s)
      case (WdlTypes.T_File, WdlValues.V_File(s)) => isMutableFile(s)

      // arrays
      case (WdlTypes.T_Array(t), WdlValues.V_Array(_, elems)) =>
        elems.exists(e => requiresEvaluation(t, e))

      // maps
      case (WdlTypes.T_Map(keyType, valueType), WdlValues.V_Map(_, m)) =>
        m.keys.exists(k => requiresEvaluation(keyType, k)) ||
          m.values.exists(v => requiresEvaluation(valueType, v))

      case (WdlTypes.T_Pair(lType, rType), WdlValues.V_Pair(l, r)) =>
        requiresEvaluation(lType, l) ||
          requiresEvaluation(rType, r)

      // Strip optional type
      case (WdlTypes.T_Optional(t), WdlValues.V_Optional(w)) =>
        requiresEvaluation(t, w)
      case (WdlTypes.T_Optional(t), w) =>
        requiresEvaluation(t, w)
      case (t, WdlValues.V_Optional(w)) =>
        requiresEvaluation(t, w)

      // struct -- make sure all of its fields do not require evaluation
      case (WdlTypes.T_Struct(sname1, typeMap: Map[String, WdlTypes.T]),
            WdlValues.T_Struct(sname2, valueMap)) =>
        if (sname1 != sname2) {
          // should we throw an exception here?
          return false
        }
        typeMap.exists {
          case (name, t) =>
            val value: WdlValues.V = valueMap(name)
            requiresEvaluation(t, value)
        }

      case (_, _) =>
        // anything else require evaluation
        true
    }
  }

  // A trivial expression has no operators, it is either a constant WdlValues.V
  // or a single identifier. For example: '5' and 'x' are trivial. 'x + y'
  // is not.
  def isTrivialExpression(expr: TAT.Expr): Boolean = {
    expr match {
      case _ : WdlValues.ValueNull => true
      case _ : WdlValues.ValueNone => true
      case _ : WdlValues.ValueBoolean => true
      case _ : WdlValues.ValueInt => true
      case _ : WdlValues.ValueFloat => true
      case _ : WdlValues.ValueString => true
      case _ : WdlValues.ValueFile => true
      case _ : WdlValues.ValueDirectory => true
      case _ : WdlValues.ExprIdentifier => true
      case _  => false
    }
  }

  // Check if the WDL expression is a constant. If so, calculate and return it.
  // Otherwise, return None.
  //
  def ifConstEval(womType: WdlTypes.T, expr: TAT.Expr): Option[WdlValues.V] = {
    evaluateWomExpression(expr, womType, Map.empty) match {
      case None => None
      case Some(value: WdlValues.V) if requiresEvaluation(womType, value) =>
        // There are WDL constants that require evaluation.
        None
      case Some(value) => Some(value)
    }
  }

  def isExpressionConst(womType: WdlTypes.T, expr: TAT.Expr): Boolean = {
    ifConstEval(womType, expr) match {
      case None    => false
      case Some(_) => true
    }
  }

  def evalConst(womType: WdlTypes.T, expr: TAT.Expr): WdlValues.V = {
    ifConstEval(womType, expr) match {
      case None           => throw new Exception(s"Expression ${expr} is not a WDL constant")
      case Some(wdlValue) => wdlValue
    }
  }

}
