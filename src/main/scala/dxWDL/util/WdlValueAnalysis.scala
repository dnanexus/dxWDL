package dxWDL.util

import java.nio.file.{Paths}
import wdlTools.types.{TypedAbstractSyntax => TAT}

import dxWDL.base.Utils
import dxWDL.base.WdlCompat._

object WdlValueAnalysis {
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
  private def requiresEvaluation(wdlType: WdlType, value: WdlValue): Boolean = {
    def isMutableFile(constantFileStr: String): Boolean = {
      constantFileStr match {
        case path if path.startsWith(Utils.DX_URL_PREFIX) =>
          // platform files are immutable
          false
        case path if path.startsWith("http://") || path.startsWith("https://") =>
          // web files are immutable
          false
        case _ =>
          // anything else might be mutable
          true
      }
    }

    (wdlType, value) match {
      // Base case: primitive types.
      case (WdlBooleanType, _)                   => false
      case (WdlIntegerType, _)                   => false
      case (WdlFloatType, _)                     => false
      case (WdlStringType, _)                    => false
      case (WdlFileType, WdlString(s))     => isMutableFile(s)
      case (WdlFileType, WdlSingleFile(s)) => isMutableFile(s)

      // arrays
      case (WdlArrayType(t), WdlArray(_, elems)) =>
        elems.exists(e => requiresEvaluation(t, e))

      // maps
      case (WdlMapType(keyType, valueType), WdlMap(_, m)) =>
        m.keys.exists(k => requiresEvaluation(keyType, k)) ||
          m.values.exists(v => requiresEvaluation(valueType, v))

      case (WdlPairType(lType, rType), WdlPair(l, r)) =>
        requiresEvaluation(lType, l) ||
          requiresEvaluation(rType, r)

      // Strip optional type
      case (WdlOptionalType(t), WdlOptionalValue(_, Some(w))) =>
        requiresEvaluation(t, w)
      case (WdlOptionalType(t), w) =>
        requiresEvaluation(t, w)

      // missing value
      case (_, WdlOptionalValue(_, None)) => false

      // struct -- make sure all of its fields do not require evaluation
      case (WdlStructType(typeMap: Map[String, WdlType], _), WdlStruct(valueMap, _)) =>
        typeMap.exists {
          case (name, t) =>
            val value: WdlValue = valueMap(name)
            requiresEvaluation(t, value)
        }

      case (_, _) =>
        throw new Exception(
            s"""|Unsupported combination type=(${wdlType})
                |value=(${value})""".stripMargin
              .replaceAll("\n", " ")
        )
    }
  }

  // Check if the WDL expression is a constant. If so, calculate and return it.
  // Otherwise, return None.
  //
  def ifConstEval(wdlType: WdlType, expr: TAT.Expr): Option[WdlValue] = {
    if (requiresEvaluation(wdlType, value)) {
        // There are WDL constants that require evaluation.
        None
    } else {
      Some(value)
    }
  }

  def isExpressionConst(wdlType: WdlType, expr: TAT.Expr): Boolean = {
    ifConstEval(wdlType, expr) match {
      case None    => false
      case Some(_) => true
    }
  }

  def evalConst(wdlType: WdlType, expr: TAT.Expr): WdlValue = {
    ifConstEval(wdlType, expr) match {
      case None           => throw new Exception(s"Expression ${expr} is not a WDL constant")
      case Some(wdlValue) => wdlValue
    }
  }
}
