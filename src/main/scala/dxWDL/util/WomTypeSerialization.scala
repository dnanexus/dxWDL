package dxWDL.util

import wom.types._

// Write a wom type as a string, and be able to convert back.
object WomTypeSerialization {
    def toString(t:WomType) : String = {
        t match {
            // Base case: primitive types.
            case WomNothingType => "Nothing"
            case WomBooleanType => "Boolean"
            case WomIntegerType => "Integer"
            case WomLongType => "Long"
            case WomFloatType => "Float"
            case WomStringType => "String"
            case WomSingleFileType => "SingleFile"

            // compound types
            case WomMaybeEmptyArrayType(memberType) =>
                val inner = toString(memberType)
                s"MaybeEmptyArray[${inner}]"
            case WomNonEmptyArrayType(memberType) =>
                val inner = toString(memberType)
                s"NonEmptyArray[${inner}]"
            case WomOptionalType(memberType) =>
                val inner = toString(memberType)
                s"Option[$inner]"
            case WomMapType(keyType, valueType) =>
                val k = toString(keyType)
                val v = toString(valueType)
                s"Map[$k, $v]"

            // catch-all for other types not currently supported
            case _ =>
                throw new Exception(s"Unsupported WOM type ${t}, ${t.toDisplayString}")
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
    private def splitInTwo(s: String) : (String, String) = {
        // find the central comma, it is in the one location where
        // the square brackets even out.
        val letters = s.toArray
        def find(crntPos : Int,
                 numOpenBrackets : Int) : Int = {
            if (crntPos >= letters.length)
                throw new Exception("out of bounds")
            if (numOpenBrackets < 0)
                throw new Exception("number of open brackets cannot go below zero")
            letters(crntPos) match {
                case '[' => find(crntPos+1, numOpenBrackets+1)
                case ']' => find(crntPos+1, numOpenBrackets-1)
                case ',' if (numOpenBrackets == 0) =>
                    crntPos
                case _ => find(crntPos+1, numOpenBrackets)
            }
        }
        val centralCommaPos = find(0, 0)

        val firstType = s.substring(0, centralCommaPos)
        val secondType = s.substring(centralCommaPos + 1)
        (firstType.trim, secondType.trim)
    }

    def fromString(str: String) : WomType = {
        str match {
            case "Nothing" => WomNothingType
            case "Boolean" => WomBooleanType
            case "Integer" => WomIntegerType
            case "Long" => WomLongType
            case "Float" => WomFloatType
            case "String" => WomStringType
            case "SingleFile" => WomSingleFileType
            case _ if str.contains("[") && str.contains("]") =>
                val openParen = str.indexOf("[")
                val closeParen = str.lastIndexOf("]")
                val outer = str.substring(0, openParen)
                val inner = str.substring(openParen+1, closeParen)
                outer match {
                    case "MaybeEmptyArray" => WomMaybeEmptyArrayType(fromString(inner))
                    case "NonEmptyArray" => WomNonEmptyArrayType(fromString(inner))
                    case "Option" => WomOptionalType(fromString(inner))
                    case "Map" =>
                        // split a string like "KK, VV" into (KK, VV)
                        val (ks, vs) = splitInTwo(inner)
                        val kt = fromString(ks)
                        val vt = fromString(vs)
                        WomMapType(kt, vt)
                }
            case _ =>
                throw new Exception(s"Cannot convert ${str} to a wom type")
        }
    }
}
