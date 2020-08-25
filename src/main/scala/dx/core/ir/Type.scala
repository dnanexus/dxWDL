package dx.core.ir

sealed trait Type

object Type {
  // Primitive types that are supported natively.
  case object TInt extends Type
  case object TFloat extends Type
  case object TString extends Type
  case object TBoolean extends Type
  case object TFile extends Type

  /**
    * A directory maps to a DNAnexus folder, or to some other representation of
    * a hierarchy of files, e.g. a tar or zip archive.
    */
  case object TDirectory extends Type

  /**
    * Wrapper that indicates a type is optional.
    * @param t wrapped type
    */
  case class TOptional(t: Type) extends Type

  /**
    * Array of primitive or file values - all items in an array must be of the
    * same type. Some languages (e.g. WDL) have a quantifier to specify that
    * the array must be non-empty - this does not change how the array is
    * represented, but an error may be thrown if the array value is empty.
    * @param t inner type
    * @param nonEmpty whether the array must not be empty
    */
  case class TArray(t: Type, nonEmpty: Boolean = false) extends Type

  /**
    * A JSON object.
    */
  case object THash extends Type

  /**
    * A special type of hash that encodes a map with arbitrary key type as an
    * object with two keys ("keys" and "values").
    * @param key key type
    * @param value value type
    */
  case class TMap(key: Type, value: Type) extends Type

  /**
    * Represents a user-defined type. Values of this type are represented
    * as VHash.
    * @param name type name
    */
  case class TSchema(name: String, members: Map[String, Type]) extends Type

  def isOptional(t: Type): Boolean = {
    t match {
      case _: TOptional => true
      case _            => false
    }
  }

  /**
    * Makes a type optional.
    * @param t the type
    * @param force if true, then `t` will be made optional even if it is already optional.
    * @return
    */
  def ensureOptional(t: Type, force: Boolean = false): TOptional = {
    t match {
      case t if force   => TOptional(t)
      case t: TOptional => t
      case _            => TOptional(t)
    }
  }

  def unwrapOptional(t: Type, mustBeOptional: Boolean = false): Type = {
    t match {
      case TOptional(wrapped) => wrapped
      case _ if mustBeOptional =>
        throw new Exception(s"Type ${t} is not T_Optional")
      case _ => t
    }
  }
}
