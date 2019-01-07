package dxWDL.runner

//import cats.data.Validated.{Invalid, Valid}
//import common.validation.ErrorOr.ErrorOr
import spray.json._
import wom.callable.WorkflowDefinition
//import wom.expression.WomExpression
//import wom.types._
import wom.values._

import dxWDL.util._
import dxWDL.util.Utils.META_WORKFLOW_INFO

case class WfFragInput(subBlockNr: Int,
                       env: Map[String, WomValue],
                       calls : Vector[String])

case class WfFragInputOutput(dxIoFunctions : DxIoFunctions,
                             wf: WorkflowDefinition,
                             runtimeDebugLevel: Int) {
    val verbose = runtimeDebugLevel >= 1

    private def getMetaInfo(metaInfo : JsValue) : (Vector[String], Int, Map[String, String]) = {
        // meta information used for running workflow fragments

        val calls: Vector[String] = metaInfo("calls").map{
            case JsString(x) => x
            case other => throw new Exception(s"Bad value ${other}")
        }.toVector
        val subBlockNum: Int = metaInfo("subBlockNum") match {
            case JsNumber(i) => i.toInt
            case other => throw new Exception(s"Bad value ${other}")
        }
        val fqnDict : Map[String, String]  = metaInfo("fqnDict").map {
            case (key, JsString(value)) => key -> value
        }.toMap

        (calls, subBlockNum, fqnDict)
    }

    // 1. Convert the inputs to WOM values
    // 2. Setup an environment to evaluate the sub-block. This should
    //    look to the WOM code as if all previous code had been evaluated.
    def loadInputs(inputs : JsValue) : WfFragInput = {
        val fields = inputs.asJsObject.fields
        val metaInfo: Map[String, JsValue] =
            fields.get(META_WORKFLOW_INFO) match {
                case None =>
                    throw new Exception(
                        s"""|JSON object does not contain the field
                            |${META_WORKFLOW_INFO}""".stripMargin.replaceAll("\n", " "))
                case Some(x) => x.fields
            }
        val (calls, subBlockNr, fqnDict) = getMetaInfo(metaInfo)

        val regularFields = fields - META_WORKFLOW_INFO


        WfFragInput(subBlockNum,
                    env,
                    calls)
    }
}
