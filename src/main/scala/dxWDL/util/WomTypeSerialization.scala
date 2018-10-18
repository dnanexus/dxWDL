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

            // catch all for other types not currently supported
            case _ =>
                throw new Exception(s"Unsupported WOM type ${t}, ${t.toDisplayString}")
        }
    }


    private def splitInTwo(s: String) : (String, String) = ???

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
                    case "Option" => WomNonEmptyArrayType(fromString(inner))
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
