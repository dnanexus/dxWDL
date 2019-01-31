package dxWDL.util

import org.scalatest.{FlatSpec, Matchers}
import wom.types._

class WomTypeSerializationTest extends FlatSpec with Matchers {

    it should "work on optional type" in {
        val t : WomType = WomOptionalType(WomIntegerType)
        WomTypeSerialization.fromString(WomTypeSerialization.toString(t)) should be (t)
    }

    it should "handle map types" in {
        val t : WomType = WomMapType(WomStringType, WomIntegerType)
        WomTypeSerialization.fromString(WomTypeSerialization.toString(t)) should be (t)
    }

    it should "handle map types II" in {
        val t : WomType = WomMapType(WomStringType, WomMapType(WomFloatType, WomIntegerType))
        WomTypeSerialization.fromString(WomTypeSerialization.toString(t)) should be (t)
    }

    it should "handle map types III" in {
        val t : WomType = WomMapType(WomMapType(WomFloatType, WomIntegerType),
                                     WomArrayType(WomLongType))
        WomTypeSerialization.fromString(WomTypeSerialization.toString(t)) should be (t)
    }
}
