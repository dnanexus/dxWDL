package dx.core.ir

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import spray.json.{JsObject, JsString, JsValue}

class TypeSerializationTest extends AnyFlatSpec with Matchers {
  private val testCases: Vector[Type] = Vector(
      // Primitive types
      Type.TBoolean,
      Type.TInt,
      Type.TFloat,
      Type.TString,
      Type.TFile,
      // arrays
      Type.TArray(Type.TString),
      Type.TArray(Type.TFile, nonEmpty = true),
      // optionals
      Type.TOptional(Type.TInt),
      Type.TOptional(Type.TArray(Type.TBoolean))
  )

  it should "work for various WDL types" in {
    testCases.foreach { t =>
      val typeMap = Map("key" -> t)
      val jsv = TypeSerde.serialize(typeMap)
      TypeSerde.deserialize(jsv, Map.empty) shouldBe typeMap
    }
  }

  private val personType = Type.TSchema("Person", Map("name" -> Type.TString, "age" -> Type.TInt))
  private val houseType = Type.TSchema(
      "House",
      Map("street" -> Type.TString, "zip code" -> Type.TInt, "owner" -> personType)
  )
  private val structTestCases: Vector[Type] = Vector(
      personType,
      Type.TArray(houseType),
      Type.TOptional(houseType)
  )

  it should "work for structs" in {
    val typeAliases: Map[String, Type.TSchema] = Map("Person" -> personType, "House" -> houseType)
    structTestCases.foreach { t =>
      val typeMap = Map("key" -> t)
      val jsv = TypeSerde.serialize(typeMap)
      TypeSerde.deserialize(jsv, typeAliases) shouldBe typeMap
    }
  }

  val badTypes: Vector[JsValue] = Vector(
      JsString("A bad type"),
      JsString("placeholder"),
      JsObject("name" -> JsString("Map"),
               "keyType" -> JsString("Int"),
               "valueType" -> JsString("UnrealFile"))
  )

  it should "detect bad type descriptions" in {
    val typeAliases: Map[String, Type.TSchema] = Map("Person" -> personType, "House" -> houseType)
    badTypes.foreach { jsv =>
      assertThrows[Exception] {
        val typeMap = JsObject("key" -> jsv)
        TypeSerde.deserialize(typeMap, typeAliases)
      }
    }
  }
}
