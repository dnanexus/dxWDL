package dx.core.ir

import dx.core.ir.Type.{TBoolean, _}
import spray.json.{JsBoolean, JsObject, JsString, JsValue}
import wdlTools.util.JsUtils

object TypeSerde {
  def serialize(t: Type): JsValue = {
    t match {
      case TBoolean         => JsString("Boolean")
      case TInt             => JsString("Int")
      case TFloat           => JsString("Float")
      case TString          => JsString("String")
      case TFile            => JsString("File")
      case TDirectory       => JsString("Directory")
      case THash            => JsString("Hash")
      case TSchema(name, _) => JsString(name)
      case TArray(memberType, nonEmpty) =>
        JsObject(
            Map(
                "name" -> JsString("Array"),
                "type" -> serialize(memberType),
                "nonEmpty" -> JsBoolean(nonEmpty)
            )
        )
      case TOptional(inner) =>
        serialize(inner) match {
          case name: JsString =>
            JsObject(Map("name" -> name, "optional" -> JsBoolean(true)))
          case JsObject(fields) =>
            JsObject(fields + ("optional" -> JsBoolean(true)))
          case other =>
            throw new Exception(s"invalid inner type value ${other}")
        }
    }
  }

  def deserialize(jsValue: JsValue, typeAliases: Map[String, Type]): Type = {
    def resolveType(name: String): Type = {
      try {
        simpleFromString(name)
      } catch {
        case _: UnknownTypeException if typeAliases.contains(name) =>
          typeAliases(name)
      }
    }
    def inner(innerValue: JsValue): Type = {
      innerValue match {
        case JsString(name) => resolveType(name)
        case JsObject(fields) =>
          val t = fields("name") match {
            case JsString("Array") =>
              val arrayType = inner(fields("type"))
              val nonEmpty = fields.get("nonEmpty").exists(JsUtils.getBoolean(_))
              TArray(arrayType, nonEmpty)
            case JsString(name) =>
              resolveType(name)
            case _ =>
              throw new Exception(s"invalid type field value ${innerValue}")
          }
          if (fields.get("optional").exists(JsUtils.getBoolean(_))) {
            TOptional(t)
          } else {
            t
          }
        case _ =>
          throw new Exception(s"unexpected type value ${innerValue}")
      }
    }
    inner(jsValue)
  }

  private def toNativePrimitive(t: Type): String = {
    t match {
      case TBoolean => "boolean"
      case TInt     => "int"
      case TFloat   => "float"
      case TString  => "string"
      case TFile    => "file"
      // TODO: case TDirectory =>
      case _ => throw new Exception(s"not a primitive type")
    }
  }

  def toNative(t: Type): (String, Boolean) = {
    val (innerType, optional) = t match {
      case TOptional(innerType) => (innerType, true)
      case _                    => (t, false)
    }
    innerType match {
      case _ if Type.isNativePrimitive(innerType) =>
        (toNativePrimitive(innerType), optional)
      case TArray(memberType, nonEmpty) if Type.isNativePrimitive(memberType) =>
        // arrays of primitives translate to e.g. 'array:file' -
        val nativeInnerType = toNativePrimitive(memberType)
        (s"array:${nativeInnerType}", !nonEmpty || optional)
      case _ =>
        // everything else is a complex type represented as a hash
        ("hash", optional)
    }
  }

  // Get a human readable type name
  // Int ->   "Int"
  // Array[Int] -> "Array[Int]"
  def toString(t: Type): String = {
    t match {
      case TBoolean         => "Boolean"
      case TInt             => "Int"
      case TFloat           => "Float"
      case TString          => "String"
      case TFile            => "File"
      case TDirectory       => "Directory"
      case THash            => "Hash"
      case TSchema(name, _) => name
      case TArray(memberType, _) =>
        s"Array[${toString(memberType)}]"
      case TOptional(TOptional(_)) =>
        throw new Exception(s"nested optional type ${t}")
      case TOptional(inner) =>
        s"${toString(inner)}?"
    }
  }

  case class UnknownTypeException(message: String) extends Exception(message)

  /**
    * Convert a String to a simple (non-compound) type, i.e. TArray and TMap
    * are not supported.
    * @param s type string
    * @return
    */
  def simpleFromString(s: String): Type = {
    s match {
      case "Boolean"   => TBoolean
      case "Int"       => TInt
      case "Float"     => TFloat
      case "String"    => TString
      case "File"      => TFile
      case "Directory" => TDirectory
      case "Hash"      => THash
      case _ if s.endsWith("?") =>
        simpleFromString(s.dropRight(1)) match {
          case TOptional(_) =>
            throw new Exception(s"nested optional type ${s}")
          case inner =>
            TOptional(inner)
        }
      case s if s.contains("[") =>
        throw new Exception(s"type ${s} is not primitive")
      case _ =>
        throw UnknownTypeException(s"Unknown type ${s}")
    }
  }
}
