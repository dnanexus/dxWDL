/**
  Bridge between WDL values and DNAx values. Since the WDL value has
already been calculated, we avoid the lazy evaluation issues that the
WdlVarLinks module has to contend with.
  */

package dxWDL

import spray.json._
import wdl4s.wdl.types._
import wdl4s.wdl.values._

case class BValue(wvl: WdlVarLinks, wdlValue: WdlValue)

object BValue {
    // Serialization of a WDL value to JSON
    private def wdlToJSON(t:WdlType, w:WdlValue) : JsValue = {
        (t, w)  match {
            // Base case: primitive types.
            // Files are encoded as their full path.
            case (WdlBooleanType, WdlBoolean(b)) => JsBoolean(b)
            case (WdlIntegerType, WdlInteger(n)) => JsNumber(n)
            case (WdlFloatType, WdlFloat(x)) => JsNumber(x)
            case (WdlStringType, WdlString(s)) => JsString(s)
            case (WdlStringType, WdlSingleFile(path)) => JsString(path)
            case (WdlFileType, WdlSingleFile(path)) => JsString(path)
            case (WdlFileType, WdlString(path)) => JsString(path)

            // arrays
            // Base case: empty array
            case (_, WdlArray(_, ar)) if ar.length == 0 =>
                JsArray(Vector.empty)

            // Non empty array
            case (WdlArrayType(t), WdlArray(_, elems)) =>
                val jsVals = elems.map(e => wdlToJSON(t, e))
                JsArray(jsVals.toVector)

            // Maps. These are projections from a key to value, where
            // the key and value types are statically known.
            //
            // keys are strings, we can use JSON objects
            case (WdlMapType(WdlStringType, valueType), WdlMap(_, m)) =>
                JsObject(m.map{
                         case (WdlString(k), v) =>
                             k -> wdlToJSON(valueType, v)
                         case (k,_) =>
                             throw new Exception(s"key ${k.toWdlString} should be a WdlStringType")
                     }.toMap)

            // general case, the keys are not strings.
            case (WdlMapType(keyType, valueType), WdlMap(_, m)) =>
                val keys:WdlValue = WdlArray(WdlArrayType(keyType), m.keys.toVector)
                val kJs = wdlToJSON(keys.wdlType, keys)
                val values:WdlValue = WdlArray(WdlArrayType(valueType), m.values.toVector)
                val vJs = wdlToJSON(values.wdlType, values)
                JsObject("keys" -> kJs, "values" -> vJs)

            // keys are strings, requiring no conversion. We do
            // need to carry the types are runtime.
            case (WdlObjectType, WdlObject(m: Map[String, WdlValue])) =>
                JsObject(m.map{ case (k, v) =>
                             k -> JsObject(
                                 "type" -> JsString(w.wdlType.toWdlString),
                                 "value" -> wdlToJSON(v.wdlType, v))
                         }.toMap)

            case (WdlPairType(lType, rType), WdlPair(l,r)) =>
                val lJs = wdlToJSON(lType, l)
                val rJs = wdlToJSON(rType, r)
                JsObject("left" -> lJs, "right" -> rJs)

            // Strip optional type
            case (WdlOptionalType(t), WdlOptionalValue(_,Some(w))) =>
                wdlToJSON(t, w)
            case (WdlOptionalType(t), w) =>
                wdlToJSON(t, w)
            case (t, WdlOptionalValue(_,Some(w))) =>
                wdlToJSON(t, w)

            // If the value is none then, it is a missing value
            // What if the value is null?

            case (_,_) => throw new Exception(
                s"""|Unsupported combination type=(${t.toWdlString},${t})
                    |value=(${w.toWdlString}, ${w})"""
                    .stripMargin.replaceAll("\n", " "))
        }
    }

    private def wdlFromJSON(t:WdlType, jsv:JsValue) : WdlValue = {
        (t, jsv)  match {
            // base case: primitive types
            case (WdlBooleanType, JsBoolean(b)) => WdlBoolean(b.booleanValue)
            case (WdlIntegerType, JsNumber(bnm)) => WdlInteger(bnm.intValue)
            case (WdlFloatType, JsNumber(bnm)) => WdlFloat(bnm.doubleValue)
            case (WdlStringType, JsString(s)) => WdlString(s)
            case (WdlFileType, JsString(path)) => WdlSingleFile(path)

            // arrays
            case (WdlArrayType(t), JsArray(vec)) =>
                WdlArray(WdlArrayType(t),
                         vec.map{ elem => wdlFromJSON(t, elem) })


            // maps with string keys
            case (WdlMapType(WdlStringType, valueType), JsObject(fields)) =>
                val m: Map[WdlValue, WdlValue] = fields.map {
                    case (k,v) =>
                        WdlString(k) -> wdlFromJSON(valueType, v)
                }.toMap
                WdlMap(WdlMapType(WdlStringType, valueType), m)

            // General maps. These are serialized as an object with a keys array and
            // a values array.
            case (WdlMapType(keyType, valueType), JsObject(_)) =>
                jsv.asJsObject.getFields("keys", "values") match {
                    case Seq(JsArray(kJs), JsArray(vJs)) =>
                        val m = (kJs zip vJs).map{ case (k, v) =>
                            val kWdl = wdlFromJSON(keyType, k)
                            val vWdl = wdlFromJSON(valueType, v)
                            kWdl -> vWdl
                        }.toMap
                        WdlMap(WdlMapType(keyType, valueType), m)
                    case _ => throw new Exception(s"Malformed serialized map ${jsv}")
                }

            case (WdlObjectType, JsObject(fields)) =>
                val m: Map[String, WdlValue] = fields.map{ case (k,v) =>
                    val elem:WdlValue =
                        v.asJsObject.getFields("type", "value") match {
                            case Seq(JsString(elemTypeStr), elemValue) =>
                                val elemType:WdlType = WdlType.fromWdlString(elemTypeStr)
                                wdlFromJSON(elemType, elemValue)
                        }
                    k -> elem
                }.toMap
                WdlObject(m)

            case (WdlPairType(lType, rType), JsObject(_)) =>
                jsv.asJsObject.getFields("left", "right") match {
                    case Seq(lJs, rJs) =>
                        val left = wdlFromJSON(lType, lJs)
                        val right = wdlFromJSON(rType, rJs)
                        WdlPair(left, right)
                    case _ => throw new Exception(s"Malformed serialized par ${jsv}")
                }

            case (WdlOptionalType(t), _) =>
                wdlFromJSON(t, jsv)

            case _ =>
                throw new AppInternalException(
                    s"Unsupported combination ${t.toWdlString} ${jsv.prettyPrint}"
                )
        }
    }

    // serialization routines
    def toJSON(bv: BValue) : JsValue = {
        val dxLinkJs = bv.wvl.dxlink match {
            case DxlValue(jsv) => jsv
            case other => throw new Exception(s"cannot serialize a ${other} DxLink case class")
        }
        JsObject("wdlType" -> JsString(bv.wvl.wdlType.toWdlString),
                 "attrs" -> JsObject(bv.wvl.attrs.m),
                 "dxlink" -> dxLinkJs,
                 "wdlValue" -> wdlToJSON(bv.wvl.wdlType, bv.wdlValue))
    }

    def fromJSON(jsv:JsValue) : BValue = {
        jsv.asJsObject.getFields("wdlType", "attrs", "dxlink", "wdlValue") match {
            case Seq(JsString(typeStr), JsObject(attrs), dxlink, wValue) =>
                val wdlType = WdlType.fromWdlString(typeStr)
                val wvl = WdlVarLinks(wdlType, DeclAttrs(attrs), DxlValue(dxlink))
                val wdlValue = wdlFromJSON(wdlType, wValue)
                BValue(wvl, wdlValue)
            case other => throw new DeserializationException(s"BValue unexpected ${other}")
        }
    }

}
