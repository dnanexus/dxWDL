package dx.core.languages.wdl

import spray.json.{
  DeserializationException,
  JsArray,
  JsBoolean,
  JsNull,
  JsNumber,
  JsObject,
  JsString,
  JsValue
}
import wdlTools.eval.WdlValues
import wdlTools.types.WdlTypes

case class WdlValueSerialization(typeAliases: Map[String, WdlTypes.T]) {

  // Serialization of a WDL value to JSON
  private def wdlToJSON(t: WdlTypes.T, w: WdlValues.V): JsValue = {
    (t, w) match {
      // Base case: primitive types.
      // Files are encoded as their full path.
      case (WdlTypes.T_Boolean, WdlValues.V_Boolean(b)) => JsBoolean(b)
      case (WdlTypes.T_Int, WdlValues.V_Int(n))         => JsNumber(n)
      case (WdlTypes.T_Float, WdlValues.V_Float(x))     => JsNumber(x)
      case (WdlTypes.T_String, WdlValues.V_String(s))   => JsString(s)
      case (WdlTypes.T_String, WdlValues.V_File(path))  => JsString(path)
      case (WdlTypes.T_File, WdlValues.V_File(path))    => JsString(path)
      case (WdlTypes.T_File, WdlValues.V_String(path))  => JsString(path)

      // arrays
      // Base case: empty array
      case (_, WdlValues.V_Array(ar)) if ar.isEmpty =>
        JsArray(Vector.empty)

      // Non empty array
      case (WdlTypes.T_Array(t, _), WdlValues.V_Array(elems)) =>
        val jsVals = elems.map(e => wdlToJSON(t, e))
        JsArray(jsVals)

      // Maps. These are projections from a key to value, where
      // the key and value types are statically known.
      //
      // keys are strings, we can use JSON objects
      case (WdlTypes.T_Map(WdlTypes.T_String, valueType), WdlValues.V_Map(m)) =>
        JsObject(m.map {
          case (WdlValues.V_String(k), v) =>
            k -> wdlToJSON(valueType, v)
          case (k, _) =>
            throw new Exception(s"key ${k} should be a WdlTypes.T_String")
        })

      // general case, the keys are not strings.
      case (WdlTypes.T_Map(keyType, valueType), WdlValues.V_Map(m)) =>
        val keys: Vector[JsValue] = m.keys.map(wdlToJSON(keyType, _)).toVector
        val values: Vector[JsValue] = m.values.map(wdlToJSON(valueType, _)).toVector
        JsObject("keys" -> JsArray(keys), "values" -> JsArray(values))

      case (WdlTypes.T_Pair(lType, rType), WdlValues.V_Pair(l, r)) =>
        val lJs = wdlToJSON(lType, l)
        val rJs = wdlToJSON(rType, r)
        JsObject("left" -> lJs, "right" -> rJs)

      // Strip optional type
      case (WdlTypes.T_Optional(t), WdlValues.V_Optional(w)) =>
        wdlToJSON(t, w)

      // missing value
      case (WdlTypes.T_Optional(_), WdlValues.V_Null) => JsNull
      case (_, WdlValues.V_Optional(_))               => JsNull

      // keys are strings, requiring no conversion. We do
      // need to carry the types are runtime.
      case (WdlTypes.T_Struct(_, typeMap), WdlValues.V_Object(m)) =>
        val mJs: Map[String, JsValue] = m.map {
          case (key, v) =>
            val t: WdlTypes.T = typeMap(key)
            key -> wdlToJSON(t, v)
        }
        JsObject(mJs)
      case (WdlTypes.T_Struct(structName, typeMap), WdlValues.V_Struct(structName2, m)) =>
        assert(structName == structName2)
        val mJs: Map[String, JsValue] = m.map {
          case (key, v) =>
            val t: WdlTypes.T = typeMap(key)
            key -> wdlToJSON(t, v)
        }
        JsObject(mJs)

      case (_, _) =>
        throw new Exception(
            s"""|Unsupported combination
                |  type: $t
                |  value: $w
                |""".stripMargin
              .replaceAll("\n", " ")
        )
    }
  }

  private def wdlFromJSON(t: WdlTypes.T, jsv: JsValue): WdlValues.V = {
    (t, jsv) match {
      // base case: primitive types
      case (WdlTypes.T_Boolean, JsBoolean(b)) => WdlValues.V_Boolean(b.booleanValue)
      case (WdlTypes.T_Int, JsNumber(bnm))    => WdlValues.V_Int(bnm.intValue)
      case (WdlTypes.T_Float, JsNumber(bnm))  => WdlValues.V_Float(bnm.doubleValue)
      case (WdlTypes.T_String, JsString(s))   => WdlValues.V_String(s)
      case (WdlTypes.T_File, JsString(path))  => WdlValues.V_File(path)

      // arrays
      case (WdlTypes.T_Array(t, _), JsArray(vec)) =>
        WdlValues.V_Array(vec.map { elem =>
          wdlFromJSON(t, elem)
        })

      // maps with string keys
      case (WdlTypes.T_Map(WdlTypes.T_String, valueType), JsObject(fields)) =>
        val m: Map[WdlValues.V, WdlValues.V] = fields.map {
          case (k, v) =>
            WdlValues.V_String(k) -> wdlFromJSON(valueType, v)
        }.toMap
        WdlValues.V_Map(m)

      // General maps. These are serialized as an object with a keys array and
      // a values array.
      case (WdlTypes.T_Map(keyType, valueType), JsObject(_)) =>
        jsv.asJsObject.getFields("keys", "values") match {
          case Seq(JsArray(kJs), JsArray(vJs)) =>
            val m = (kJs zip vJs).map {
              case (k, v) =>
                val kWdl = wdlFromJSON(keyType, k)
                val vWdl = wdlFromJSON(valueType, v)
                kWdl -> vWdl
            }.toMap
            WdlValues.V_Map(m)
          case _ => throw new Exception(s"Malformed serialized map ${jsv}")
        }

      case (WdlTypes.T_Pair(lType, rType), JsObject(_)) =>
        jsv.asJsObject.getFields("left", "right") match {
          case Seq(lJs, rJs) =>
            val left = wdlFromJSON(lType, lJs)
            val right = wdlFromJSON(rType, rJs)
            WdlValues.V_Pair(left, right)
          case _ => throw new Exception(s"Malformed serialized par ${jsv}")
        }

      case (WdlTypes.T_Optional(_), JsNull) =>
        WdlValues.V_Null
      case (WdlTypes.T_Optional(t), _) =>
        WdlValues.V_Optional(wdlFromJSON(t, jsv))

      // structs
      case (WdlTypes.T_Struct(structName, typeMap), JsObject(fields)) =>
        val m: Map[String, WdlValues.V] = fields.map {
          case (key, elemValue) =>
            val t: WdlTypes.T = typeMap(key)
            val elem: WdlValues.V = wdlFromJSON(t, elemValue)
            key -> elem
        }
        WdlValues.V_Struct(structName, m)

      case (_, _) =>
        throw new Exception(s"Unsupported combination ${t} ${jsv.prettyPrint}")
    }
  }

  // serialization routines
  def toJSON(t: WdlTypes.T, w: WdlValues.V): JsValue = {
    JsObject("wdlType" -> JsString(TypeSerialization(typeAliases).toString(t)),
             "wdlValue" -> wdlToJSON(t, w))
  }

  def fromJSON(jsv: JsValue): WdlValues.V = {
    val obj = jsv.asJsObject
    val typeValue =
      if (obj.fields.contains("wdlType")) {
        obj.getFields("wdlType", "wdlValue")
      } else {
        obj.getFields("womType", "womValue")
      }
    typeValue match {
      case Seq(JsString(typeStr), wValue) =>
        val wdlType = TypeSerialization(typeAliases).fromString(typeStr)
        wdlFromJSON(wdlType, wValue)
      case other => throw DeserializationException(s"WdlValues.V unexpected ${other}")
    }
  }
}
