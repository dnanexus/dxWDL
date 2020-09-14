package dx.core.ir

import dx.core.ir.Type._
import dx.core.ir.Value._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import spray.json._

class ValueSerializationTest extends AnyFlatSpec with Matchers {
  val valueTestCases: Vector[(Type, Value)] = Vector(
      // primitive types
      (TBoolean, VBoolean(false)),
      (TInt, VInt(12)),
      (TFloat, VFloat(1.4)),
      (TString, VString("charming")),
      (TFile, VFile("/tmp/foo.txg")),
      // arrays
      (TArray(TInt), VArray(Vector(VInt(4), VInt(5)))),
      // compounds
      (TOptional(TInt), VInt(13)),
      // map with string keys
      (TMap(TString, TInt),
       VMap(
           Map(VString("A") -> VInt(1),
               VString("C") -> VInt(4),
               VString("G") -> VInt(5),
               VString("T") -> VInt(5))
       )),
      // map with non-string keys
      (TMap(TInt, TFile),
       VMap(Map(VInt(1) -> VFile("/tmp/A.txt"), VInt(3) -> VFile("/tmp/B.txt")))),
      // structs
      (TSchema("Person", Map("name" -> TString, "age" -> TInt)),
       VHash(Map("name" -> VString("Bradly"), "age" -> VInt(42))))
  )

  it should "work on a variety of values" in {
    valueTestCases.foreach {
      case (t, v) =>
        val jsv = ValueSerde.serialize(v)
        val irValue = ValueSerde.deserializeWithType(jsv, t)
        irValue shouldBe v
    }
  }

  it should "detect bad JSON" in {
    val badJson = JsObject("a" -> JsNumber(1), "b" -> JsString("hello"))
    assertThrows[Exception] {
      ValueSerde.deserialize(badJson)
    }
  }
}
