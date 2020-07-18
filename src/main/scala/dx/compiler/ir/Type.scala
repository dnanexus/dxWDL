package dx.compiler.ir

sealed trait Type

// Null represents that no value is specified for an optional field.
case object TNull extends Type

// Primitive types that are supported natively.
case object TInt extends Type
case object TFloat extends Type
case object TString extends Type
case object TBoolean extends Type

// Data object types - only file is supported.
case object TFile extends Type

// A directory maps to a DNAnexus folder, or to some other representation of
// a hierarchy of files, e.g. a tar or zip archive.
case object TDirectory extends Type

// A JSON object
case object THash extends Type

// Array of primitive or file values - all items in an array must be of the
// same type. An array must be non-empty - use TNull for empty arrays.
case class TArray(t: Type) extends Type
