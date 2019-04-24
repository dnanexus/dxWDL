package dxWDL.base

import org.scalatest.{FlatSpec, Matchers}
import wom.values._
import wom.types._

class WomValueSerializationTest extends FlatSpec with Matchers {

    it should "work on optional values" in {
        val valueSerialize = WomValueSerialization(Map.empty)

        val v : WomValue = WomOptionalValue(WomIntegerType,Some(WomInteger(13)))
        valueSerialize.fromJSON(valueSerialize.toJSON(v)) should be(v)
    }

    it should "support structs" in {
        val personType = WomCompositeType(Map("name" -> WomStringType,
                                              "age" -> WomIntegerType),
                                          Some("Person"))
        val typeAliases: Map[String, WomType] = Map("Person" -> personType)

        val bradly : WomValue = WomObject(Map("name" -> WomString("Bradly"),
                                              "age" -> WomInteger(42)),
                                          personType)

        val valueSerialize = WomValueSerialization(typeAliases)

        valueSerialize.fromJSON(valueSerialize.toJSON(bradly)) should be(bradly)
    }
}
