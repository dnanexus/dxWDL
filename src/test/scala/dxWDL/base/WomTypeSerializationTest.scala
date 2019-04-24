package dxWDL.base

import org.scalatest.{FlatSpec, Matchers}
import wom.types._

class WomTypeSerializationTest extends FlatSpec with Matchers {
    private val typeSerialize = WomTypeSerialization(Map.empty)

    it should "work on optional type" in {
        val t : WomType = WomOptionalType(WomIntegerType)
        typeSerialize.fromString(typeSerialize.toString(t)) should be (t)
    }

    it should "handle map types" in {
        val t : WomType = WomMapType(WomStringType, WomIntegerType)
        typeSerialize.fromString(typeSerialize.toString(t)) should be (t)
    }

    it should "handle map types II" in {
        val t : WomType = WomMapType(WomStringType, WomMapType(WomFloatType, WomIntegerType))
        typeSerialize.fromString(typeSerialize.toString(t)) should be (t)
    }

    it should "handle map types III" in {
        val t : WomType = WomMapType(WomMapType(WomFloatType, WomIntegerType),
                                     WomArrayType(WomLongType))
        typeSerialize.fromString(typeSerialize.toString(t)) should be (t)
    }

    it should "handle pairs" in {
        val t : WomType = WomPairType(WomIntegerType, WomStringType)
        typeSerialize.fromString(typeSerialize.toString(t)) should be (t)
    }
}
