package dxWDL.base

import wdlTools.types.WdlTypes._

// Write a wom type as a string, and be able to convert back.
case class WdlTypeSerialization(typeAliases: Map[String, WdlTypes.T]) {
  type WdlType = WdlTypes.T

  def toString(t: WdlType): String = {
    t match {
      // Base case: primitive types.
      case T_Any        => "Any"
      case T_Boolean    => "Boolean"
      case T_Int    => "Integer"
      case T_Float      => "Float"
      case T_String     => "String"
      case T_File => "File"
      case T_Directory => "Directory"

      // compound types
      case T_Array(memberType, false) =>
        val inner = toString(memberType)
        s"MaybeEmptyArray[${inner}]"
      case T_Array(memberType, true) =>
        val inner = toString(memberType)
        s"NonEmptyArray[${inner}]"
      case T_Map(keyType, valueType) =>
        val k = toString(keyType)
        val v = toString(valueType)
        s"Map[$k, $v]"
      case T_Optional(memberType) =>
        val inner = toString(memberType)
        s"Option[$inner]"
      case T_Pair(lType, rType) =>
        val ls = toString(lType)
        val rs = toString(rType)
        s"Pair[$ls, $rs]"

      // structs
      case T_Struct(_, Some(structName)) =>
        structName

      // catch-all for other types not currently supported
      case _ =>
        throw new Exception(s"Unsupported WDL type ${t}, ${t.stableName}")
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

    val firstType = s.substring(0, centralCommaPos)
    val secondType = s.substring(centralCommaPos + 1)
    (firstType.trim, secondType.trim)
  }

  def fromString(str: String): WdlType = {
    str match {
      case "Any" => T_Any
      case "Boolean" => T_Boolean
      case "Integer" => T_Int
      case "Float" => T_Float
      case "String" => T_String
      case "File" => T_File
      case "Directory" => T_Directory

      case _ if str.contains("[") && str.contains("]") =>
        val openParen = str.indexOf("[")
        val closeParen = str.lastIndexOf("]")
        val outer = str.substring(0, openParen)
        val inner = str.substring(openParen + 1, closeParen)
        outer match {
          case "MaybeEmptyArray" => T_Array(fromString(inner), false)
          case "NonEmptyArray" => T_Array(fromString(inner), true)
          case "Map"             =>
            // split a string like "KK, VV" into (KK, VV)
            val (ks, vs) = splitInTwo(inner)
            val kt = fromString(ks)
            val vt = fromString(vs)
            T_Map(kt, vt)
          case "Option"        => T_Optional(fromString(inner))
          case "Pair" =>
            val (ls, rs) = splitInTwo(inner)
            val lt = fromString(ls)
            val rt = fromString(rs)
            T_Pair(lt, rt)
        }
      case name if typeAliases contains name =>
        // This must be a user defined type, look in the type-aliases.
        typeAliases(name)

      case _ =>
        throw new Exception(s"Cannot convert ${str} to a wom type")
    }
  }
}

object WdlTypeSerialization {
  // Get a human readable type name
  // Int ->   "Int"
  // Array[Int] -> "Array[Int]"
  def typeName(t: WdlType): String = {
    t match {
      // Base case: primitive types.
      case T_Any        => "Any"
      case T_Boolean    => "Boolean"
      case T_Int    => "Integer"
      case T_Float      => "Float"
      case T_String     => "String"
      case T_File => "File"
      case T_Directory => "Directory"

      // compound types
      case T_Array(memberType, _) =>
        val inner = typeName(memberType)
        s"Array[${inner}]"
      case T_Map(keyType, valueType) =>
        val k = typeName(keyType)
        val v = typeName(valueType)
        s"Map[$k, $v]"
      case T_Optional(memberType) =>
        val inner = typeName(memberType)
        s"$inner?"
      case T_Pair(lType, rType) =>
        val ls = typeName(lType)
        val rs = typeName(rType)
        s"Pair[$ls, $rs]"

      // structs
      case T_Struct(_, Some(structName)) =>
        structName

      // catch-all for other types not currently supported
      case _ =>
        throw new Exception(s"Unsupported WDL type ${t}")
    }
  }
}
