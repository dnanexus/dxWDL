package dx.core.languages.wdl

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import wdlTools.types.WdlTypes

class TypeSerializationTest extends AnyFlatSpec with Matchers {

  val testCases: Vector[WdlTypes.T] = Vector(
      // Primitive types
      WdlTypes.T_Boolean,
      WdlTypes.T_Int,
      WdlTypes.T_Float,
      WdlTypes.T_String,
      WdlTypes.T_File,
      // arrays
      WdlTypes.T_Array(WdlTypes.T_String, nonEmpty = false),
      WdlTypes.T_Array(WdlTypes.T_File, nonEmpty = true),
      // maps
      WdlTypes.T_Map(WdlTypes.T_String, WdlTypes.T_File),
      WdlTypes.T_Map(WdlTypes.T_String, WdlTypes.T_Map(WdlTypes.T_Float, WdlTypes.T_Int)),
      // optionals
      WdlTypes.T_Optional(WdlTypes.T_Int),
      WdlTypes.T_Optional(WdlTypes.T_Array(WdlTypes.T_Boolean, nonEmpty = false)),
      WdlTypes.T_Pair(WdlTypes.T_Int, WdlTypes.T_String)
  )

  it should "work for various WDL types" in {
    val typeSerialize = TypeSerialization(Map.empty)

    for (t <- testCases) {
      typeSerialize.fromString(typeSerialize.toString(t)) should be(t)
    }
  }

  private val personType =
    WdlTypes.T_Struct("Person", Map("name" -> WdlTypes.T_String, "age" -> WdlTypes.T_Int))
  private val houseType = WdlTypes.T_Struct(
      "House",
      Map("street" -> WdlTypes.T_String, "zip code" -> WdlTypes.T_Int, "owner" -> personType)
  )

  val structTestCases: Vector[WdlTypes.T] = Vector(
      personType,
      WdlTypes.T_Pair(personType, houseType),
      WdlTypes.T_Optional(houseType)
  )

  it should "work for structs" in {
    val typeAliases: Map[String, WdlTypes.T] = Map("Person" -> personType, "House" -> houseType)
    val typeSerialize = TypeSerialization(typeAliases)

    for (t <- structTestCases) {
      typeSerialize.fromString(typeSerialize.toString(t)) should be(t)
    }
  }

  val badTypeNames: Vector[String] = Vector(
      "A bad type",
      "placeholder",
      "Map[Int, UnrealFile]",
      "Pair[Int, Map[Int, X__String]]"
  )

  it should "detect bad type descriptions" in {
    val typeAliases: Map[String, WdlTypes.T] = Map("Person" -> personType, "House" -> houseType)
    val typeSerialize = TypeSerialization(typeAliases)

    for (typeDesc <- badTypeNames) {
      assertThrows[Exception] {
        typeSerialize.fromString(typeDesc)
      }
    }
  }
}
