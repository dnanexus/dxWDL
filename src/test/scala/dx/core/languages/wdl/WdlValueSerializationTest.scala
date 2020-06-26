package dx.core.languages.wdl

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import spray.json._
import wdlTools.eval.WdlValues._
import wdlTools.types.WdlTypes._

class WdlValueSerializationTest extends AnyFlatSpec with Matchers {
  private val personType =
    T_Struct("Person", Map("name" -> T_String, "age" -> T_Int))
  private val houseType =
    T_Struct("House", Map("street" -> T_String, "zip code" -> T_Int, "owner" -> personType))
  private val typeAliases: Map[String, T] = Map("Person" -> personType, "House" -> houseType)
  private val valueSerializer = WdlValueSerialization(typeAliases)

  val valueTestCases: List[(T, V)] = List(
      // primitive types
      (T_Boolean, V_Boolean(false)),
      (T_Int, V_Int(12)),
      (T_Float, V_Float(1.4)),
      (T_String, V_String("charming")),
      (T_File, V_File("/tmp/foo.txg")),
      // arrays
      (T_Array(T_Int, nonEmpty = false), V_Array(Vector(V_Int(4), V_Int(5)))),
      // compounds
      (T_Optional(T_Int), V_Optional(V_Int(13))),
      (T_Pair(T_String, T_Array(T_Int)), V_Pair(V_String("A"), V_Array(Vector.empty))),
      // map with string keys
      (T_Map(T_String, T_Int),
       V_Map(
           Map(V_String("A") -> V_Int(1),
               V_String("C") -> V_Int(4),
               V_String("G") -> V_Int(5),
               V_String("T") -> V_Int(5))
       )),
      // map with non-string keys
      (T_Map(T_Int, T_File),
       V_Map(Map(V_Int(1) -> V_File("/tmp/A.txt"), V_Int(3) -> V_File("/tmp/B.txt")))),
      // structs
      (T_Struct("Person", Map("name" -> T_String, "age" -> T_Int)),
       V_Struct("Person", Map("name" -> V_String("Bradly"), "age" -> V_Int(42))))
  )

  it should "work on a variety of values" in {
    for ((t, v) <- valueTestCases) {
      valueSerializer.fromJSON(valueSerializer.toJSON(t, v)) should be(v)
    }
  }

  it should "detect bad JSON" in {
    val badJson = JsObject("a" -> JsNumber(1), "b" -> JsString("hello"))

    assertThrows[Exception] {
      valueSerializer.fromJSON(badJson)
    }
  }
}
