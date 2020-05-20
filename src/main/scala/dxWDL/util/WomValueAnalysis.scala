package dxWDL.util

import java.nio.file.{Paths}
import wdlTools.types.{TypedAbstractSyntax => TAT}

import dxWDL.base.Utils
import dxWDL.base.WomCompat._

object WomValueAnalysis {
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
  private def requiresEvaluation(wdlType: WomType, value: WomValue): Boolean = {
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
      case (WomBooleanType, _)                   => false
      case (WomIntegerType, _)                   => false
      case (WomFloatType, _)                     => false
      case (WomStringType, _)                    => false
      case (WomFileType, WomString(s))     => isMutableFile(s)
      case (WomFileType, WomSingleFile(s)) => isMutableFile(s)

      // arrays
      case (WomArrayType(t), WomArray(_, elems)) =>
        elems.exists(e => requiresEvaluation(t, e))

      // maps
      case (WomMapType(keyType, valueType), WomMap(_, m)) =>
        m.keys.exists(k => requiresEvaluation(keyType, k)) ||
          m.values.exists(v => requiresEvaluation(valueType, v))

      case (WomPairType(lType, rType), WomPair(l, r)) =>
        requiresEvaluation(lType, l) ||
          requiresEvaluation(rType, r)

      // Strip optional type
      case (WomOptionalType(t), WomOptionalValue(_, Some(w))) =>
        requiresEvaluation(t, w)
      case (WomOptionalType(t), w) =>
        requiresEvaluation(t, w)

      // missing value
      case (_, WomOptionalValue(_, None)) => false

      // struct -- make sure all of its fields do not require evaluation
      case (WomStructType(typeMap: Map[String, WomType], _), WomStruct(valueMap, _)) =>
        typeMap.exists {
          case (name, t) =>
            val value: WomValue = valueMap(name)
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
  def ifConstEval(wdlType: WomType, expr: TAT.Expr): Option[WomValue] = {
    if (requiresEvaluation(wdlType, value)) {
        // There are WDL constants that require evaluation.
        None
    } else {
      Some(value)
    }
  }

  def isExpressionConst(wdlType: WomType, expr: TAT.Expr): Boolean = {
    ifConstEval(wdlType, expr) match {
      case None    => false
      case Some(_) => true
    }
  }

  def evalConst(wdlType: WomType, expr: TAT.Expr): WomValue = {
    ifConstEval(wdlType, expr) match {
      case None           => throw new Exception(s"Expression ${expr} is not a WDL constant")
      case Some(wdlValue) => wdlValue
    }
  }
}
