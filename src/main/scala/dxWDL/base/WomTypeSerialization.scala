package dxWDL.base

import wom.types._

// Write a wom type as a string, and be able to convert back.
case class WomTypeSerialization(typeAliases: Map[String, WomType]) {

  def toString(t: WomType): String = {
    t match {
      // Base case: primitive types.
      case WomNothingType    => "Nothing"
      case WomBooleanType    => "Boolean"
      case WomIntegerType    => "Integer"
      case WomLongType       => "Long"
      case WomFloatType      => "Float"
      case WomStringType     => "String"
      case WomSingleFileType => "SingleFile"

      // compound types
      case WomMaybeEmptyArrayType(memberType) =>
        val inner = toString(memberType)
        s"MaybeEmptyArray[${inner}]"
      case WomMapType(keyType, valueType) =>
        val k = toString(keyType)
        val v = toString(valueType)
        s"Map[$k, $v]"
      case WomNonEmptyArrayType(memberType) =>
        val inner = toString(memberType)
        s"NonEmptyArray[${inner}]"
      case WomOptionalType(memberType) =>
        val inner = toString(memberType)
        s"Option[$inner]"
      case WomPairType(lType, rType) =>
        val ls = toString(lType)
        val rs = toString(rType)
        s"Pair[$ls, $rs]"

      // structs
      case WomCompositeType(_, Some(structName)) =>
        structName

      // catch-all for other types not currently supported
      case _ =>
        throw new Exception(s"Unsupported WOM type ${t}, ${t.stableName}")
    }
  }

  // Split a string like "KK, VV" into the pair ("KK", "VV").
  // These types appear in maps, for example:
  //   1. Map[Int, String]
  // More complex examples are:
  //   2. Map[File, Map[String, File]]
  //   3. Map[Map[String, File], Int]
  //
  // The difficultly is that we can't just search for the first comma, we
  // need to find the "central comma". The one that splits the string
  // into to complete types.
  //
  // The inputs handed to this method, in the above examples are:
  //   1. Int, String
  //   2. File, Map[String, File]
  //   3. Map[String, File], Int
  private def splitInTwo(s: String): (String, String) = {
    // find the central comma, it is in the one location where
    // the square brackets even out.
    val letters = s.toArray
    def find(crntPos: Int, numOpenBrackets: Int): Int = {
      if (crntPos >= letters.length)
        throw new Exception("out of bounds")
      if (numOpenBrackets < 0)
        throw new Exception("number of open brackets cannot go below zero")
      letters(crntPos) match {
        case '[' => find(crntPos + 1, numOpenBrackets + 1)
        case ']' => find(crntPos + 1, numOpenBrackets - 1)
        case ',' if (numOpenBrackets == 0) =>
          crntPos
        case _ => find(crntPos + 1, numOpenBrackets)
      }
    }
    val centralCommaPos = find(0, 0)

    val firstType  = s.substring(0, centralCommaPos)
    val secondType = s.substring(centralCommaPos + 1)
    (firstType.trim, secondType.trim)
  }

  def fromString(str: String): WomType = {
    str match {
      case "Nothing"    => WomNothingType
      case "Boolean"    => WomBooleanType
      case "Integer"    => WomIntegerType
      case "Long"       => WomLongType
      case "Float"      => WomFloatType
      case "String"     => WomStringType
      case "SingleFile" => WomSingleFileType
      case _ if str.contains("[") && str.contains("]") =>
        val openParen  = str.indexOf("[")
        val closeParen = str.lastIndexOf("]")
        val outer      = str.substring(0, openParen)
        val inner      = str.substring(openParen + 1, closeParen)
        outer match {
          case "MaybeEmptyArray" => WomMaybeEmptyArrayType(fromString(inner))
          case "Map"             =>
            // split a string like "KK, VV" into (KK, VV)
            val (ks, vs) = splitInTwo(inner)
            val kt       = fromString(ks)
            val vt       = fromString(vs)
            WomMapType(kt, vt)
          case "NonEmptyArray" => WomNonEmptyArrayType(fromString(inner))
          case "Option"        => WomOptionalType(fromString(inner))
          case "Pair" =>
            val (ls, rs) = splitInTwo(inner)
            val lt       = fromString(ls)
            val rt       = fromString(rs)
            WomPairType(lt, rt)
        }
      case name if typeAliases contains name =>
        // This must be a user defined type, look in the type-aliases.
        typeAliases(name)

      case _ =>
        throw new Exception(s"Cannot convert ${str} to a wom type")
    }
  }
}

object WomTypeSerialization {
  // Get a human readable type name
  // Int ->   "Int"
  // Array[Int] -> "Array[Int]"
  def typeName(t: WomType): String = {
    t match {
      // Base case: primitive types.
      case WomNothingType    => "Nothing"
      case WomBooleanType    => "Boolean"
      case WomIntegerType    => "Int"
      case WomLongType       => "Long"
      case WomFloatType      => "Float"
      case WomStringType     => "String"
      case WomSingleFileType => "File"

      // compound types
      case WomMaybeEmptyArrayType(memberType) =>
        val inner = typeName(memberType)
        s"Array[${inner}]"
      case WomMapType(keyType, valueType) =>
        val k = typeName(keyType)
        val v = typeName(valueType)
        s"Map[$k, $v]"
      case WomNonEmptyArrayType(memberType) =>
        val inner = typeName(memberType)
        s"Array[${inner}]+"
      case WomOptionalType(memberType) =>
        val inner = typeName(memberType)
        s"$inner?"
      case WomPairType(lType, rType) =>
        val ls = typeName(lType)
        val rs = typeName(rType)
        s"Pair[$ls, $rs]"

      // structs
      case WomCompositeType(_, Some(structName)) =>
        structName

      // catch-all for other types not currently supported
      case _ =>
        throw new Exception(s"Unsupported WOM type ${t}, ${t.stableName}")
    }
  }
}
