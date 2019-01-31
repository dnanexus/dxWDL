package dxWDL.util

import org.scalatest.{FlatSpec, Matchers}
import wom.values._
import wom.types._

class WomValueSerializationTest extends FlatSpec with Matchers {

    it should "work on optional values" in {
        val v : WomValue = WomOptionalValue(WomIntegerType,Some(WomInteger(13)))

        WomValueSerialization.fromJSON(WomValueSerialization.toJSON(v)) should be(v)
    }
}
