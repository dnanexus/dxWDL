package dxWDL.util

import cats.data.Validated.{Invalid, Valid}
import common.validation.ErrorOr.ErrorOr
import java.nio.file.{Paths}
import wom.types._
import wom.values._
import wom.expression._

import dxWDL.base.Utils

object WomValueAnalysis {
  // These are used for evaluating if a WOM expression is constant.
  // Ideally, we should not be using any of the IO functions, since
  // these checks may be part of the compilation process.
  private lazy val dxPathConfig = DxPathConfig(Paths.get("/tmp/"), false, false)
  private lazy val dxIoFunctions = DxIoFunctions(Map.empty, dxPathConfig, 0)

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
  private def requiresEvaluation(womType: WomType, value: WomValue): Boolean = {
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
      case (WomBooleanType, _)                   => false
      case (WomIntegerType, _)                   => false
      case (WomFloatType, _)                     => false
      case (WomStringType, _)                    => false
      case (WomSingleFileType, WomString(s))     => isMutableFile(s)
      case (WomSingleFileType, WomSingleFile(s)) => isMutableFile(s)

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
      case (
          WomCompositeType(typeMap: Map[String, WomType], _),
          WomObject(valueMap, _)
          ) =>
        typeMap.exists {
          case (name, t) =>
            val value: WomValue = valueMap(name)
            requiresEvaluation(t, value)
        }

      case (_, _) =>
        throw new Exception(
            s"""|Unsupported combination type=(${womType.stableName},${womType})
                    |value=(${value.toWomString}, ${value})""".stripMargin
              .replaceAll("\n", " ")
        )
    }
  }

  // Check if the WDL expression is a constant. If so, calculate and return it.
  // Otherwise, return None.
  //
  def ifConstEval(womType: WomType, expr: WomExpression): Option[WomValue] = {
    val result: ErrorOr[WomValue] =
      expr.evaluateValue(Map.empty[String, WomValue], dxIoFunctions)
    result match {
      case Invalid(_)                                                   => None
      case Valid(value: WomValue) if requiresEvaluation(womType, value) =>
        // There are WDL constants that require evaluation.
        None
      case Valid(value) => Some(value)
    }
  }

  def isExpressionConst(womType: WomType, expr: WomExpression): Boolean = {
    ifConstEval(womType, expr) match {
      case None    => false
      case Some(_) => true
    }
  }

  def evalConst(womType: WomType, expr: WomExpression): WomValue = {
    ifConstEval(womType, expr) match {
      case None =>
        throw new Exception(s"Expression ${expr} is not a WDL constant")
      case Some(wdlValue) => wdlValue
    }
  }

}
