package dxWDL.runner

//import cats.data.Validated.{Invalid, Valid}
//import common.validation.ErrorOr.ErrorOr
import spray.json._
import wom.callable.WorkflowDefinition
//import wom.expression.WomExpression
//import wom.types._
import wom.values._

import dxWDL.util._

case class WfFragInput(subBlockNr: Int,
                       env: Map[String, WomValue])

case class WfFragInputOutput(dxIoFunctions : DxIoFunctions,
                             wf: WorkflowDefinition,
                             runtimeDebugLevel: Int) {
    val verbose = runtimeDebugLevel >= 1

    // 1. Convert the inputs to WOM values
    // 2. Setup an environment to evaluate the sub-block. This should
    //    look to the WOM code as if all previous code had been evaluated.
    def loadInputs(inputs : JsValue) : WfFragInput = {
        // extra information used for running workflow fragments
        /*
        val hardCodedFragInfo = JsObject(
            "calls" -> JsArray(calls.map{x => JsString(x) }),
            "subBlockNum" -> JsNumber(subBlockNum),
            "fqnDict" -> JsObject(fqnDict.map{ case (k, v) => k -> JsString(v) }.toMap)
        )
        Some(JsObject("name" -> JsString(Utils.EXTRA_WORKFLOW_INFO),
                      "class" -> JsString("hash"),
                      "default" -> hardCodedFragInfo))
         */

        //WfFragInput(subBlockNr, env)
        throw new NotImplementedError("TODO")
    }
}
