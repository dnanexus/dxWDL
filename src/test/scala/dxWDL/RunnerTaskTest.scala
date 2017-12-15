package dxWDL

import org.scalatest.{FlatSpec, Matchers}
import spray.json._
import wdl4s.wdl.types._
import wdl4s.wdl.values._

class RunnerTaskTest extends FlatSpec with Matchers {
    it should "serialize correctly" in {

        val wdlValues = List(
            WdlBoolean(true),
            WdlInteger(3),
            WdlFloat(4.5),
            WdlString("flute"),
            WdlFile("invalid-file"),

            WdlArray(WdlArrayType(WdlIntegerType),
                     List(WdlInteger(3), WdlInteger(15))),
            WdlArray(WdlArrayType(WdlStringType),
                     List(WdlString("r"), WdlString("l"))),

            WdlOptionalValue(WdlStringType, Some(WdlString("french horm"))),
            WdlOptionalValue(WdlStringType, None),

            WdlArray(WdlArrayType(WdlOptionalType(WdlIntegerType)),
                     List(WdlOptionalValue(WdlIntegerType, Some(WdlInteger(1))),
                          WdlOptionalValue(WdlIntegerType, None),
                          WdlOptionalValue(WdlIntegerType, Some(WdlInteger(2)))))
        )

        wdlValues.foreach{ w =>
            val jsv:JsValue = RunnerTaskSerialization.toJSON(w)
            val w2:WdlValue = RunnerTaskSerialization.fromJSON(jsv)
            w2 should equal(w)
        }
    }
}
