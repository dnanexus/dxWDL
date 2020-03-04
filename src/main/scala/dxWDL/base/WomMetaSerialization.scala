package dxWDL.base

import wom.callable.MetaValueElement
import wom.callable.MetaValueElement._

// Convert WOM MetaValueElement to/from JSON, etc
object WomMetaSerialization {
  def unwrapAny(element: MetaValueElement): Any = {
    element match {
      case MetaValueElementString(text)   => text
      case MetaValueElementInteger(i)     => i
      case MetaValueElementFloat(f)       => f
      case MetaValueElementBoolean(b)     => b
      case MetaValueElementArray(array)   => array.map(unwrapAny)
      case MetaValueElementObject(fields) => fields.mapValues(unwrapAny)
      case MetaValueElementNull           => None
      case _                              => throw new Exception(s"Expected MetaValueElement, got ${element}")
    }
  }
}
