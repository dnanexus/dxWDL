package dx.core.ir

import dx.core.ir.Type._
import dx.core.ir.Value._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

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
      // hash
      (THash, VHash(Map("A" -> VInt(1), "C" -> VInt(4), "G" -> VInt(5), "T" -> VInt(5)))),
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
}
