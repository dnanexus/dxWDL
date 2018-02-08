package dxWDL

import org.scalatest.{FlatSpec, Matchers}
import spray.json._
import wom.types._
import wom.values._

class RunnerTaskTest extends FlatSpec with Matchers {
    it should "serialize correctly" in {

        val wdlValues = List(
            WomBoolean(true),
            WomInteger(3),
            WomFloat(4.5),
            WomString("flute"),
            WomFile("invalid-file"),

            WomArray(WomArrayType(WomIntegerType),
                     List(WomInteger(3), WomInteger(15))),
            WomArray(WomArrayType(WomStringType),
                     List(WomString("r"), WomString("l"))),

            WomOptionalValue(WomStringType, Some(WomString("french horm"))),
            WomOptionalValue(WomStringType, None),

            WomArray(WomArrayType(WomOptionalType(WomIntegerType)),
                     List(WomOptionalValue(WomIntegerType, Some(WomInteger(1))),
                          WomOptionalValue(WomIntegerType, None),
                          WomOptionalValue(WomIntegerType, Some(WomInteger(2)))))
        )

        wdlValues.foreach{ w =>
            val jsv:JsValue = runner.TaskSerialization.toJSON(w)
            val w2:WomValue = runner.TaskSerialization.fromJSON(jsv)
            w2 should equal(w)
        }
    }
}
