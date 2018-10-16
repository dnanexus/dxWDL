package dxWDL

import com.dnanexus.{IOClass}
import org.scalatest.{FlatSpec, Matchers}
import spray.json._
import wom.types._

class WdlVarLinksTest extends FlatSpec with Matchers {

    it should "import JSON values" in {
        val wvl = WdlVarLinks.importFromDxExec(DXIOParam(IOClass.BOOLEAN, false),
                                               DeclAttrs.empty, JsBoolean(true))
        wvl.womType should equal(WomBooleanType)

        val wvl2 = WdlVarLinks.importFromDxExec(DXIOParam(IOClass.ARRAY_OF_FLOATS, true),
                                                DeclAttrs.empty,
                                                JsArray(Vector(JsNumber(1), JsNumber(2.3))))
        wvl2.womType should equal(WomMaybeEmptyArrayType(WomFloatType))

        val wvl3 = WdlVarLinks.importFromDxExec(DXIOParam(IOClass.ARRAY_OF_STRINGS, false),
                                                DeclAttrs.empty,
                                                JsArray(Vector(JsString("hello"),
                                                               JsString("sunshine"),
                                                               JsString("ride"))))
        wvl3.womType should equal(WomNonEmptyArrayType(WomStringType))
    }
}
