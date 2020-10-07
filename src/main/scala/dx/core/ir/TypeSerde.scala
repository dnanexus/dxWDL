package dx.core.ir

import dx.core.ir.Type.{TBoolean, _}
import spray.json.{JsBoolean, JsObject, JsString, JsValue}
import wdlTools.util.JsUtils

object TypeSerde {
  private def serializeType(
      t: Type,
      schemas: Map[String, JsValue]
  ): (JsValue, Map[String, JsValue]) = {
    t match {
      case TBoolean   => (JsString("Boolean"), schemas)
      case TInt       => (JsString("Int"), schemas)
      case TFloat     => (JsString("Float"), schemas)
      case TString    => (JsString("String"), schemas)
      case TFile      => (JsString("File"), schemas)
      case TDirectory => (JsString("Directory"), schemas)
      case THash      => (JsString("Hash"), schemas)
      case TArray(memberType, nonEmpty) =>
        val (typeJs, newSchemas) = serializeType(memberType, schemas)
        (JsObject(
             Map(
                 "name" -> JsString("Array"),
                 "type" -> typeJs,
                 "nonEmpty" -> JsBoolean(nonEmpty)
             )
         ),
         newSchemas)
      case TSchema(name, _) if schemas.contains(name) =>
        (JsString(name), schemas)
      case TSchema(name, members) =>
        val (membersJs, newSchemas) =
          members.foldLeft((Map.empty[String, JsValue], Map.empty[String, JsValue])) {
            case ((membersAccu, aliasesAccu), (name, t)) =>
              val (typeJs, newSchemas) = serializeType(t, aliasesAccu)
              (membersAccu + (name -> typeJs), newSchemas)
          }
        val schemaJs = JsObject(
            Map(
                "name" -> JsString(name),
                "members" -> JsObject(membersJs)
            )
        )
        (JsString(name), newSchemas + (name -> schemaJs))
      case TOptional(inner) =>
        serializeType(inner, schemas) match {
          case (name: JsString, newSchemas) =>
            (JsObject(Map("name" -> name, "optional" -> JsBoolean(true))), newSchemas)
          case (JsObject(fields), newSchemas) =>
            (JsObject(fields + ("optional" -> JsBoolean(true))), newSchemas)
          case (other, _) =>
            throw new Exception(s"invalid inner type value ${other}")
        }
    }
  }

  /**
    * Serializes a mapping of variable names to Types.
    * @param types mapping of variable names to Types
    * @return a JsObject with two fields: 'types' and 'schemas'. Any TSchema in the
    *         input map are serialized to a JsString in the 'types' field and a
    *         corresponding entry in the 'schemas' field.
    */
  def serializeMap(
      types: Map[String, Type],
      jsSchema: Map[String, JsValue] = Map.empty,
      encodeDots: Boolean = true
  ): (Map[String, JsValue], Map[String, JsValue]) = {
    types.foldLeft((Map.empty[String, JsValue], jsSchema)) {
      case ((typeAccu, schemaAccu), (name, t)) =>
        val nameEncoded = if (encodeDots) {
          Parameter.encodeDots(name)
        } else {
          name
        }
        val (typeJs, newSchemas) = serializeType(t, schemaAccu)
        (typeAccu + (nameEncoded -> typeJs), newSchemas)
    }
  }

  def serialize(inputs: Map[String, Type], encodeDots: Boolean = true): JsValue = {
    val (typesJs, schemasJs) = serializeMap(inputs, encodeDots = encodeDots)
    JsObject(
        Map(
            "types" -> JsObject(typesJs),
            "schemas" -> JsObject(schemasJs)
        )
    )
  }

  private def deserializeSchema(jsSchema: JsValue,
                                schemas: Map[String, TSchema],
                                jsSchemas: Map[String, JsValue]): Map[String, TSchema] = {
    jsSchema.asJsObject.getFields("name", "members") match {
      case Seq(JsString(name), JsObject(membersJs)) =>
        val (memberTypes, newSchemas) =
          membersJs.foldLeft((Map.empty[String, Type], schemas)) {
            case ((memberAccu, schemaAccu), (name, jsType)) =>
              val (t, newSchemas) = deserializeType(jsType, schemaAccu, jsSchemas)
              (memberAccu + (name -> t), newSchemas)
          }
        newSchemas + (name -> TSchema(name, memberTypes))
      case _ =>
        throw new Exception(s"invalid schema ${jsSchema}")
    }
  }

  private def deserializeType(jsValue: JsValue,
                              schemas: Map[String, TSchema],
                              jsSchemas: Map[String, JsValue]): (Type, Map[String, TSchema]) = {
    jsValue match {
      case JsString(name) if schemas.contains(name) =>
        (schemas(name), schemas)
      case JsString(name) if jsSchemas.contains(name) =>
        val newSchemas = deserializeSchema(jsSchemas(name), schemas, jsSchemas)
        (newSchemas(name), newSchemas)
      case JsString(name) =>
        (simpleFromString(name), schemas)
      case JsObject(fields) =>
        val (t, newSchemas) = fields("name") match {
          case JsString("Array") =>
            val (arrayType, newSchemas) = deserializeType(fields("type"), schemas, jsSchemas)
            val nonEmpty = fields.get("nonEmpty").exists(JsUtils.getBoolean(_))
            (TArray(arrayType, nonEmpty), newSchemas)
          case JsString(name) if schemas.contains(name) =>
            (schemas(name), schemas)
          case JsString(name) if jsSchemas.contains(name) =>
            val newSchemas = deserializeSchema(jsSchemas(name), schemas, jsSchemas)
            (newSchemas(name), newSchemas)
          case JsString(name) =>
            (simpleFromString(name), schemas)
          case _ =>
            throw new Exception(s"invalid type field value ${jsValue}")
        }
        if (fields.get("optional").exists(JsUtils.getBoolean(_))) {
          (TOptional(t), newSchemas)
        } else {
          (t, newSchemas)
        }
      case _ =>
        throw new Exception(s"unexpected type value ${jsValue}")
    }
  }

  def deserializeMap(
      jsTypes: Map[String, JsValue],
      jsSchemas: Map[String, JsValue],
      schemas: Map[String, TSchema] = Map.empty,
      decodeDots: Boolean = true
  ): (Map[String, Type], Map[String, TSchema]) = {
    jsTypes.foldLeft((Map.empty[String, Type], schemas)) {
      case ((typeAccu, schemaAccu), (name, jsType)) =>
        val nameDecoded = if (decodeDots) {
          Parameter.decodeDots(name)
        } else {
          name
        }
        val (t, newSchemas) = deserializeType(jsType, schemaAccu, jsSchemas)
        (typeAccu + (nameDecoded -> t), newSchemas)
    }
  }

  /**
    * Deserializes a JsValue that was serialized using the `serialize` function.
    * @param jsValue the value to deserialize
    * @param schemas initial set of schemas (i.e. type aliases)
    * @return mapping of variable names to deserialized Types
    */
  def deserialize(jsValue: JsValue,
                  schemas: Map[String, TSchema] = Map.empty,
                  decodeDots: Boolean = true): Map[String, Type] = {
    val (jsTypes, jsSchemas) = jsValue match {
      case obj: JsObject if obj.fields.contains("types") =>
        obj.getFields("types", "schemas") match {
          case Seq(JsObject(jsTypes), JsObject(jsAliases)) =>
            (jsTypes, jsAliases)
          case Seq(JsObject(jsTypes)) =>
            (jsTypes, Map.empty[String, JsValue])
          case _ =>
            throw new Exception(s"invalid serialized types value ${jsValue}")
        }
      case JsObject(jsTypes) =>
        (jsTypes, Map.empty[String, JsValue])
      case _ =>
        throw new Exception(s"invalid serialized types value ${jsValue}")
    }
    val (types, _) = deserializeMap(jsTypes, jsSchemas, schemas, decodeDots)
    types
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
