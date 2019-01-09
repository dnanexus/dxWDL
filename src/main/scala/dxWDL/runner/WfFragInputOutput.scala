package dxWDL.runner

import com.dnanexus.DXProject
import spray.json._
import wom.types._
import wom.values._

import dxWDL.util._
import dxWDL.util.Utils.META_WORKFLOW_INFO

case class WfFragInput(wfSource: String,
                       instanceTypeDB: InstanceTypeDB,
                       subBlockNr: Int,
                       env: Map[String, WomValue],
                       execLinkInfo : Map[String, ExecLinkInfo])

case class WfFragInputOutput(dxIoFunctions : DxIoFunctions,
                             dxProject: DXProject,
                             runtimeDebugLevel: Int) {
    val verbose = runtimeDebugLevel >= 1
    val jobInputOutput = JobInputOutput(dxIoFunctions, runtimeDebugLevel)

    private def getMetaInfo(metaInfo : Map[String, JsValue]) :
            (String, InstanceTypeDB, Map[String, ExecLinkInfo], Int, Map[String, String], Map[String, WomType]) = {
        // meta information used for running workflow fragments

        val workflowSource : String = metaInfo.get("workflowSource") match {
            case Some(JsString(src)) => Utils.base64Decode(src)
            case other => throw new Exception(s"Bad value ${other}")
        }
        val instanceTypeDB : InstanceTypeDB = metaInfo.get("instanceTypeDB") match {
            case Some(JsString(src)) =>
                val dbRaw = Utils.base64Decode(src)
                dbRaw.parseJson.convertTo[InstanceTypeDB]
            case other => throw new Exception(s"Bad value ${other}")
        }
        val execLinkInfo: Map[String, ExecLinkInfo] = metaInfo.get("execLinkInfo") match {
            case Some(JsObject(fields)) =>
                fields.map{
                    case (key, ali) =>
                        key -> ExecLinkInfo.readJson(ali, dxProject)
                }.toMap
            case other => throw new Exception(s"Bad value ${other}")
        }
        val subBlockNum: Int = metaInfo("subBlockNum") match {
            case JsNumber(i) => i.toInt
            case other => throw new Exception(s"Bad value ${other}")
        }
        val fqnDict : Map[String, String]  = metaInfo.get("fqnDict") match {
            case Some(JsObject(fields)) =>
                fields.map{
                    case (key, JsString(value)) => key -> value
                    case other => throw new Exception(s"Bad value ${other}")
                }.toMap
            case other => throw new Exception(s"Bad value ${other}")
        }
        val fqnDictTypes : Map[String, WomType]  = metaInfo.get("fqnDictTypes") match {
            case Some(JsObject(fields)) =>
                fields.map{
                    case (key, JsString(value)) => key -> WomTypeSerialization.fromString(value)
                    case other => throw new Exception(s"Bad value ${other}")
                }.toMap
            case other => throw new Exception(s"Bad value ${other}")
        }

        (workflowSource, instanceTypeDB, execLinkInfo, subBlockNum, fqnDict, fqnDictTypes)
    }

    // 1. Convert the inputs to WOM values
    // 2. Setup an environment to evaluate the sub-block. This should
    //    look to the WOM code as if all previous code had been evaluated.
    def loadInputs(inputs : JsValue) : WfFragInput = {
        val fields : Map[String, JsValue] = inputs
            .asJsObject.fields
            .filter{ case (fieldName,_) => !fieldName.endsWith(Utils.FLAT_FILES_SUFFIX) }

        // Extract the meta information needed to setup the closure
        // for the subblock
        val metaInfo: Map[String, JsValue] =
            fields.get(META_WORKFLOW_INFO) match {
                case Some(JsObject(fields)) => fields
                case other =>
                    throw new Exception(
                        s"JSON object has bad value ${other} for field ${META_WORKFLOW_INFO}")
            }
        val (wfSource, instanceTypeDB, execLinkInfo,
             subBlockNr, fqnDict, fqnDictTypes) = getMetaInfo(metaInfo)

        // What remains are inputs from other stages. Convert from JSON
        // to wom values
        val regularFields = fields - META_WORKFLOW_INFO
        val env : Map[String, WomValue] = regularFields.map{
            case (name, jsValue) =>
                val fqnName = fqnDict.get(name) match {
                    case None => throw new Exception(s"Did not find variable ${name} in the block environment")
                    case Some(x) => x
                }
                val womType = fqnDictTypes.get(name) match {
                    case None => throw new Exception(s"Did not find variable ${name} in the block environment")
                    case Some(x) => x
                }
                fqnName -> jobInputOutput.unpackJobInput(womType, jsValue)
        }.toMap

        WfFragInput(wfSource,
                    instanceTypeDB,
                    subBlockNr,
                    env,
                    execLinkInfo)
    }
}
