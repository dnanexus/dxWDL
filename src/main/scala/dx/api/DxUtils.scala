package dx.api

import spray.json._

object DxUtils {
  def isDxId(objName: String): Boolean = {
    objName match {
      case _ if objName.startsWith("applet-")   => true
      case _ if objName.startsWith("file-")     => true
      case _ if objName.startsWith("record-")   => true
      case _ if objName.startsWith("workflow-") => true
      case _                                    => false
    }
  }

  def dxDataObjectToURL(dxObj: DxDataObject): String = {
    dxObj match {
      case DxFile(_, _, Some(container)) =>
        s"${DxPath.DX_URL_PREFIX}${container.id}:${dxObj.id}"
      case DxRecord(_, _, Some(container)) =>
        s"${DxPath.DX_URL_PREFIX}${container.id}:${dxObj.id}"
      case _ =>
        s"${DxPath.DX_URL_PREFIX}${dxObj.id}"
    }
  }

  // Create a dx link to a field in an execution. The execution could
  // be a job or an analysis.
  def makeEBOR(dxExec: DxExecution, fieldName: String): JsValue = {
    dxExec match {
      case _: DxJob =>
        JsObject(
            "$dnanexus_link" -> JsObject("field" -> JsString(fieldName),
                                         "job" -> JsString(dxExec.id))
        )
      case _: DxAnalysis =>
        JsObject(
            "$dnanexus_link" -> JsObject("field" -> JsString(fieldName),
                                         "analysis" -> JsString(dxExec.id))
        )
      case _ =>
        throw new Exception(s"makeEBOR can't work with ${dxExec.id}")
    }
  }
}
