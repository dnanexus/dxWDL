package dxWDL.base

import spray.json._
import wom.values._
import wom.types._

case class WomValueSerialization(typeAliases: Map[String, WomType]) {

  // Serialization of a WOM value to JSON
  private def womToJSON(t: WomType, w: WomValue): JsValue = {
    (t, w) match {
      // Base case: primitive types.
      // Files are encoded as their full path.
      case (WomBooleanType, WomBoolean(b))          => JsBoolean(b)
      case (WomIntegerType, WomInteger(n))          => JsNumber(n)
      case (WomFloatType, WomFloat(x))              => JsNumber(x)
      case (WomStringType, WomString(s))            => JsString(s)
      case (WomStringType, WomSingleFile(path))     => JsString(path)
      case (WomSingleFileType, WomSingleFile(path)) => JsString(path)
      case (WomSingleFileType, WomString(path))     => JsString(path)

      // arrays
      // Base case: empty array
      case (_, WomArray(_, ar)) if ar.length == 0 =>
        JsArray(Vector.empty)

      // Non empty array
      case (WomArrayType(t), WomArray(_, elems)) =>
        val jsVals = elems.map(e => womToJSON(t, e))
        JsArray(jsVals.toVector)

      // Maps. These are projections from a key to value, where
      // the key and value types are statically known.
      //
      // keys are strings, we can use JSON objects
      case (WomMapType(WomStringType, valueType), WomMap(_, m)) =>
        JsObject(m.map {
          case (WomString(k), v) =>
            k -> womToJSON(valueType, v)
          case (k, _) =>
            throw new Exception(
              s"key ${k.toWomString} should be a WomStringType"
            )
        }.toMap)

      // general case, the keys are not strings.
      case (WomMapType(keyType, valueType), WomMap(_, m)) =>
        val keys: WomValue = WomArray(WomArrayType(keyType), m.keys.toVector)
        val kJs = womToJSON(keys.womType, keys)
        val values: WomValue =
          WomArray(WomArrayType(valueType), m.values.toVector)
        val vJs = womToJSON(values.womType, values)
        JsObject("keys" -> kJs, "values" -> vJs)

      case (WomPairType(lType, rType), WomPair(l, r)) =>
        val lJs = womToJSON(lType, l)
        val rJs = womToJSON(rType, r)
        JsObject("left" -> lJs, "right" -> rJs)

      // Strip optional type
      case (WomOptionalType(t), WomOptionalValue(_, Some(w))) =>
        womToJSON(t, w)

      // missing value
      case (_, WomOptionalValue(_, None)) => JsNull

      // keys are strings, requiring no conversion. We do
      // need to carry the types are runtime.
      case (
          WomCompositeType(typeMap, _),
          WomObject(m: Map[String, WomValue], _)
          ) =>
        val mJs: Map[String, JsValue] = m.map {
          case (key, v) =>
            val t: WomType = typeMap(key)
            key -> womToJSON(t, v)
        }.toMap
        JsObject(mJs)

      case (_, _) =>
        throw new Exception(
          s"""|Unsupported combination type=(${t.stableName},${t})
                    |value=(${w.toWomString}, ${w})""".stripMargin
            .replaceAll("\n", " ")
        )
    }
  }

  private def womFromJSON(t: WomType, jsv: JsValue): WomValue = {
    (t, jsv) match {
      // base case: primitive types
      case (WomBooleanType, JsBoolean(b))      => WomBoolean(b.booleanValue)
      case (WomIntegerType, JsNumber(bnm))     => WomInteger(bnm.intValue)
      case (WomFloatType, JsNumber(bnm))       => WomFloat(bnm.doubleValue)
      case (WomStringType, JsString(s))        => WomString(s)
      case (WomSingleFileType, JsString(path)) => WomSingleFile(path)

      // arrays
      case (WomArrayType(t), JsArray(vec)) =>
        WomArray(WomArrayType(t), vec.map { elem =>
          womFromJSON(t, elem)
        })

      // maps with string keys
      case (WomMapType(WomStringType, valueType), JsObject(fields)) =>
        val m: Map[WomValue, WomValue] = fields.map {
          case (k, v) =>
            WomString(k) -> womFromJSON(valueType, v)
        }.toMap
        WomMap(WomMapType(WomStringType, valueType), m)

      // General maps. These are serialized as an object with a keys array and
      // a values array.
      case (WomMapType(keyType, valueType), JsObject(_)) =>
        jsv.asJsObject.getFields("keys", "values") match {
          case Seq(JsArray(kJs), JsArray(vJs)) =>
            val m = (kJs zip vJs).map {
              case (k, v) =>
                val kWom = womFromJSON(keyType, k)
                val vWom = womFromJSON(valueType, v)
                kWom -> vWom
            }.toMap
            WomMap(WomMapType(keyType, valueType), m)
          case _ => throw new Exception(s"Malformed serialized map ${jsv}")
        }

      case (WomPairType(lType, rType), JsObject(_)) =>
        jsv.asJsObject.getFields("left", "right") match {
          case Seq(lJs, rJs) =>
            val left = womFromJSON(lType, lJs)
            val right = womFromJSON(rType, rJs)
            WomPair(left, right)
          case _ => throw new Exception(s"Malformed serialized par ${jsv}")
        }

      case (WomOptionalType(t), JsNull) =>
        WomOptionalValue(t, None)
      case (WomOptionalType(t), _) =>
        WomOptionalValue(womFromJSON(t, jsv))

      // structs
      case (WomCompositeType(typeMap, Some(structName)), JsObject(fields)) =>
        val m: Map[String, WomValue] = fields.map {
          case (key, elemValue) =>
            val t: WomType = typeMap(key)
            val elem: WomValue = womFromJSON(t, elemValue)
            key -> elem
        }.toMap
        WomObject(m, WomCompositeType(typeMap, Some(structName)))

      case _ =>
        throw new Exception(
          s"Unsupported combination ${t.stableName} ${jsv.prettyPrint}"
        )
    }
  }

  // serialization routines
  def toJSON(w: WomValue): JsValue = {
    JsObject(
      "womType" -> JsString(
        WomTypeSerialization(typeAliases).toString(w.womType)
      ),
      "womValue" -> womToJSON(w.womType, w)
    )
  }

  def fromJSON(jsv: JsValue): WomValue = {
    jsv.asJsObject.getFields("womType", "womValue") match {
      case Seq(JsString(typeStr), wValue) =>
        val womType = WomTypeSerialization(typeAliases).fromString(typeStr)
        womFromJSON(womType, wValue)
      case other =>
        throw new DeserializationException(s"WomValue unexpected ${other}")
    }
  }
}
