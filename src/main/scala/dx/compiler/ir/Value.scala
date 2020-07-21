package dx.compiler.ir

import spray.json.{JsBoolean, JsNumber, JsString, JsValue}

sealed trait Value

// Represents the empty value for an optional field.
case object VNull extends Value

// Primitive values.
case class VInt(value: Long) extends Value
case class VFloat(value: Double) extends Value
case class VString(value: String) extends Value
case class VBoolean(value: Boolean) extends Value

// Represents a DNAnexus file object.
case class VFile(value: String) extends Value

// Represents a DNAnexus folder or a file-based representation of a directory
// (e.g. a zip or tar archive file).
case class VDirectory(value: String) extends Value

// A JSON object.
case class VHash(value: Map[String, Value]) extends Value

// An array of values
case class VArray(value: Vector[Value]) extends Value

object Value {
  def fromJsPrimitive(jsValue: JsValue): Value = {
    jsValue match {
      case JsBoolean(b)                         => VBoolean(b.booleanValue)
      case JsNumber(value) if value.isValidLong => VInt(value.toLongExact)
      case JsNumber(value)                      => VFloat(value.toDouble)
      case JsString(s)                          => VString(s)
      case other                                => throw new Exception(s"Unsupported JSON value ${other}")
    }
  }

  def toJsPrimitive(value: Value): JsValue = {
    value match {
      case VBoolean(b) => JsBoolean(b)
      case VInt(i)     => JsNumber(i)
      case VFloat(f)   => JsNumber(f)
      case VString(s)  => JsString(s)
      case other       => throw new Exception(s"Unsupported IR value ${other}")
    }
  }
}
