package dx.core.ir

import dx.core.ir.Value.{VBoolean, VFloat, VInt, VString}
import spray.json.{JsBoolean, JsNumber, JsString, JsValue}

object ValueSerde {
  def deserializePrimitive(jsValue: JsValue): Value = {
    jsValue match {
      case JsBoolean(b)                         => VBoolean(b.booleanValue)
      case JsNumber(value) if value.isValidLong => VInt(value.toLongExact)
      case JsNumber(value)                      => VFloat(value.toDouble)
      case JsString(s)                          => VString(s)
      case other                                => throw new Exception(s"Unsupported JSON value ${other}")
    }
  }

  def serializePrimitive(value: Value): JsValue = {
    value match {
      case VBoolean(b) => JsBoolean(b)
      case VInt(i)     => JsNumber(i)
      case VFloat(f)   => JsNumber(f)
      case VString(s)  => JsString(s)
      case other       => throw new Exception(s"Unsupported IR value ${other}")
    }
  }
}
