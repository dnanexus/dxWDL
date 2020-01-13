package dxWDL.base

import org.scalatest.{FlatSpec, Matchers}
import wom.types._

class WomTypeSerializationTest extends FlatSpec with Matchers {

  val testCases: List[WomType] = List(
      // Primitive types
      WomNothingType,
      WomBooleanType,
      WomIntegerType,
      WomLongType,
      WomFloatType,
      WomStringType,
      WomSingleFileType,
      // array
      WomMaybeEmptyArrayType(WomStringType),
      WomNonEmptyArrayType(WomSingleFileType),
      // maps
      WomMapType(WomStringType, WomSingleFileType),
      WomMapType(WomStringType, WomMapType(WomFloatType, WomIntegerType)),
      // optionals
      WomOptionalType(WomLongType),
      WomOptionalType(WomMaybeEmptyArrayType(WomBooleanType)),
      WomPairType(WomIntegerType, WomStringType)
  )

  it should "work for various WDL types" in {
    val typeSerialize = WomTypeSerialization(Map.empty)

    for (t <- testCases) {
      typeSerialize.fromString(typeSerialize.toString(t)) should be(t)
    }
  }

  val personType =
    WomCompositeType(Map("name" -> WomStringType, "age" -> WomIntegerType), Some("Person"))
  val houseType = WomCompositeType(
      Map("street" -> WomStringType, "zip code" -> WomIntegerType, "owner" -> personType),
      Some("House")
  )

  val structTestCases: List[WomType] = List(
      personType,
      WomPairType(personType, houseType),
      WomOptionalType(houseType)
  )

  it should "work for structs" in {
    val typeAliases: Map[String, WomType] = Map("Person" -> personType, "House" -> houseType)
    val typeSerialize = WomTypeSerialization(typeAliases)

    for (t <- structTestCases) {
      typeSerialize.fromString(typeSerialize.toString(t)) should be(t)
    }
  }

  val badTypeNames: List[String] = List(
      "A bad type",
      "dummy",
      "Map[Int, UnrealFile]",
      "Pair[Int, Map[Int, X__String]]"
  )

  it should "detect bad type descriptions" in {
    val typeAliases: Map[String, WomType] = Map("Person" -> personType, "House" -> houseType)
    val typeSerialize = WomTypeSerialization(typeAliases)

    for (typeDesc <- badTypeNames) {
      assertThrows[Exception] {
        typeSerialize.fromString(typeDesc)
      }
    }
  }

  it should "detect objects that aren't structs" in {
    val typeSerialize = WomTypeSerialization(Map.empty)
    val objectWithoutName =
      WomCompositeType(Map("name" -> WomStringType, "age" -> WomIntegerType), None)
    assertThrows[Exception] {
      typeSerialize.toString(objectWithoutName)
    }
  }
}
