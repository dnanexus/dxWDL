package dx.core.ir

import dx.core.ir.Type.{TDirectory, _}
import dx.core.ir.Value._
import spray.json._
import wdlTools.util.JsUtils

object ValueSerde extends DefaultJsonProtocol {

  /**
    * Serializes a Value to JSON.
    * @param value the Value to serialize
    * @param handler an optional function to perform special handling of certain values
    * @return
    */
  def serialize(value: Value, handler: Option[Value => Option[JsValue]] = None): JsValue = {
    def inner(innerValue: Value): JsValue = {
      val v = handler.flatMap(_(innerValue))
      if (v.isDefined) {
        return v.get
      }
      innerValue match {
        case VNull            => JsNull
        case VBoolean(b)      => JsBoolean(b)
        case VInt(i)          => JsNumber(i)
        case VFloat(f)        => JsNumber(f)
        case VString(s)       => JsString(s)
        case VFile(path)      => JsString(path)
        case VDirectory(path) => JsString(path)
        case VArray(array)    => JsArray(array.map(inner))
        case VHash(members)   => JsObject(members.view.mapValues(inner).toMap)
      }
    }
    inner(value)
  }

  def serializeMap(values: Map[String, Value]): Map[String, JsValue] = {
    values.map {
      case (name, value) => name -> serialize(value)
    }
  }

  /**
    * Determines if a JsValue looks like a Map object - a JsObject with "keys" and
    * "values" keys whose values are arrays of the same length.
    * @param jsValue the JsValue
    * @return
    */
  def isMapObject(jsValue: JsValue): Boolean = {
    jsValue match {
      case JsObject(members) if members.keySet == Set("keys", "values") =>
        try {
          val keys = JsUtils.getValues(members("keys"))
          val values = JsUtils.getValues(members("values"))
          keys.size == values.size
        } catch {
          case _: Throwable => false
        }
      case _ => false
    }
  }

  /**
    * Deserializes a JsValue to a Value, in the absence of type information.
    * @param jsValue the JsValue
    * @return
    */
  def deserialize(jsValue: JsValue, handler: Option[JsValue => Option[Value]] = None): Value = {
    def inner(innerValue: JsValue): Value = {
      val v = handler.flatMap(_(innerValue))
      if (v.isDefined) {
        return v.get
      }
      innerValue match {
        case JsNull                               => VNull
        case JsBoolean(b)                         => VBoolean(b.booleanValue)
        case JsNumber(value) if value.isValidLong => VInt(value.toLongExact)
        case JsNumber(value)                      => VFloat(value.toDouble)
        case JsString(s)                          => VString(s)
        case JsArray(array)                       => VArray(array.map(x => inner(x)))
        case JsObject(members)                    => VHash(members.view.mapValues(inner).toMap)
      }
    }
    inner(jsValue)
  }

  /**
    * Deserializes a JsValue to a Value of the specified type.
    * @param jsValue the JsValue
    * @param t the Type
    * @param translator an optional function for special handling of certain values
    * @return
    */
  def deserializeWithType(jsValue: JsValue,
                          t: Type,
                          translator: Option[(JsValue, Type) => JsValue] = None): Value = {
    def inner(innerValue: JsValue, innerType: Type): Value = {
      val updatedValue = translator.map(_(innerValue, innerType)).getOrElse(innerValue)
      (innerType, updatedValue) match {
        case (TOptional(_), JsNull)                       => VNull
        case (TOptional(t), _)                            => inner(updatedValue, t)
        case (TBoolean, JsBoolean(b))                     => VBoolean(b.booleanValue)
        case (TInt, JsNumber(value)) if value.isValidLong => VInt(value.toLongExact)
        case (TFloat, JsNumber(value))                    => VFloat(value.toDouble)
        case (TString, JsString(s))                       => VString(s)
        case (TFile, JsString(path))                      => VFile(path)
        case (TDirectory, JsString(path))                 => VDirectory(path)
        case (TArray(_, true), JsArray(array)) if array.isEmpty =>
          throw new Exception(s"Cannot convert empty array to non-empty type ${innerType}")
        case (TArray(t, _), JsArray(array)) =>
          VArray(array.map(x => inner(x, t)))
        case (TSchema(name, memberTypes), JsObject(members)) =>
          // ensure 1) members keys are a subset of memberTypes keys, 2) members
          // values are convertable to the corresponding types, and 3) any keys
          // in memberTypes that do not appear in members are optional
          val keys1 = members.keySet
          val keys2 = memberTypes.keySet
          val extra = keys2.diff(keys1)
          if (extra.nonEmpty) {
            throw new Exception(
                s"struct ${name} value has members that do not appear in the struct definition: ${extra}"
            )
          }
          val missingNonOptional = keys1.diff(keys2).map(key => key -> memberTypes(key)).filterNot {
            case (_, TOptional(_)) => false
            case _                 => true
          }
          if (missingNonOptional.nonEmpty) {
            throw new Exception(
                s"struct ${name} value is missing non-optional members ${missingNonOptional}"
            )
          }
          VHash(members.map {
            case (key, value) => key -> inner(value, memberTypes(key))
          })
        case (THash, JsObject(members)) =>
          VHash(members.map {
            case (key, value) => key -> deserialize(value)
          })
      }
    }
    inner(jsValue, t)
  }

  def deserializeMap(m: Map[String, JsValue]): Map[String, Value] = {
    m.map {
      case (k, v) => k -> deserialize(v)
    }
  }

  // support automatic conversion to/from JsValue
  implicit val valueFormat: RootJsonFormat[Value] = new RootJsonFormat[Value] {
    override def read(jsv: JsValue): Value = deserialize(jsv)
    override def write(value: Value): JsValue = serialize(value)
  }
}
