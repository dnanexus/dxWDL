package dxWDL.base

import org.scalatest.{FlatSpec, Matchers}
import spray.json._
import wom.values._
import wom.types._

class WomValueSerializationTest extends FlatSpec with Matchers {
  val personType =
    WomCompositeType(Map("name" -> WomStringType, "age" -> WomIntegerType), Some("Person"))
  val houseType = WomCompositeType(
    Map("street" -> WomStringType, "zip code" -> WomIntegerType, "owner" -> personType),
    Some("House")
  )
  val typeAliases: Map[String, WomType] = Map("Person" -> personType, "House" -> houseType)
  val valueSerializer                   = WomValueSerialization(typeAliases)

  val valueTestCases: List[WomValue] = List(
    // primitive types
    WomBoolean(false),
    WomInteger(12),
    WomFloat(1.4),
    WomString("charming"),
    WomSingleFile("/tmp/foo.txg"),
    // arrays
    WomArray(WomArrayType(WomIntegerType), Vector(WomInteger(4), WomInteger(5))),
    // compounds
    WomOptionalValue(WomIntegerType, Some(WomInteger(13))),
    WomPair(WomString("A"), WomArray(WomArrayType(WomStringType), Vector.empty)),
    // map with string keys
    WomMap(
      WomMapType(WomStringType, WomIntegerType),
      Map(
        WomString("A") -> WomInteger(1),
        WomString("C") -> WomInteger(4),
        WomString("G") -> WomInteger(5),
        WomString("T") -> WomInteger(5)
      )
    ),
    // map with non-string keys
    WomMap(
      WomMapType(WomIntegerType, WomSingleFileType),
      Map(
        WomInteger(1) -> WomSingleFile("/tmp/A.txt"),
        WomInteger(3) -> WomSingleFile("/tmp/B.txt")
      )
    ),
    // structs
    WomObject(Map("name" -> WomString("Bradly"), "age" -> WomInteger(42)), personType)
  )

  it should "work on a variety of values" in {
    for (v <- valueTestCases) {
      valueSerializer.fromJSON(valueSerializer.toJSON(v)) should be(v)
    }
  }

  it should "detect bad JSON" in {
    val badJson = JsObject("a" -> JsNumber(1), "b" -> JsString("hello"))

    assertThrows[Exception] {
      valueSerializer.fromJSON(badJson)
    }
  }

  it should "detect bad objects" in {
    val invalidObject = WomObject(Map("name" -> WomString("Bradly"), "age" -> WomInteger(42)))
    assertThrows[Exception] {
      valueSerializer.toJSON(invalidObject)
    }
  }
}
