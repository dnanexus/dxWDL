package dx.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import spray.json.{JsNumber, JsObject}

class JsUtilsTest extends AnyFlatSpec with Matchers {
  it should "make JSON maps deterministic" in {
    val x = JsObject("a" -> JsNumber(1), "b" -> JsNumber(2))
    val y = JsObject("b" -> JsNumber(2), "a" -> JsNumber(1))
    JsUtils.makeDeterministic(x) should be(JsUtils.makeDeterministic(y))

    val x2 = JsObject("a" -> JsNumber(10), "b" -> JsNumber(2))
    val y2 = JsObject("b" -> JsNumber(2), "a" -> JsNumber(1))
    assert(JsUtils.makeDeterministic(x2) != JsUtils.makeDeterministic(y2))
  }
}
