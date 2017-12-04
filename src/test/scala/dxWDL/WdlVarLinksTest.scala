package dxWDL

import com.dnanexus.{IOClass}
import org.scalatest.{FlatSpec, Matchers}
import spray.json._
import wdl4s.wdl.types._

class WdlVarLinksTest extends FlatSpec with Matchers {

    it should "import JSON values" in {
        val wvl = WdlVarLinks.importFromDxExec(IOClass.BOOLEAN, DeclAttrs.empty, JsBoolean(true))
        wvl.wdlType should equal(WdlBooleanType)

        val wvl2 = WdlVarLinks.importFromDxExec(IOClass.ARRAY_OF_FLOATS, DeclAttrs.empty,
                                                JsArray(Vector(JsNumber(1), JsNumber(2.3))))
        wvl2.wdlType should equal(WdlArrayType(WdlFloatType))

        val wvl3 = WdlVarLinks.importFromDxExec(IOClass.ARRAY_OF_STRINGS,
                                                DeclAttrs.empty,
                                                JsArray(Vector(JsString("hello"),
                                                               JsString("sunshine"),
                                                               JsString("ride"))))
        wvl3.wdlType should equal(WdlArrayType(WdlStringType))
    }
}
