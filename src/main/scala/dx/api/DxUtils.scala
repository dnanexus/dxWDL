package dx.api

import spray.json._

object DxUtils {
  private val dxObjectIdRegexp = "^(applet|database|dbcluster|file|record|workflow)-(\\w{24})$".r
  // Other entity ID regexps if/when needed:
  //  private val dxContainerIdRegexp = "^(container|project)-(\\w{24})$".r
  //  private val dxExecutableIdRegexp = "^(applet|app|globalworkflow|workflow)-(\\w{24})$".r
  //  private val dxExecutionIdRegexp = "^(analysis|job)-(\\w{24})$".r

  def parseDxObjectId(dxId: String): (String, String) = {
    dxId match {
      case dxObjectIdRegexp(idType, idHash) => (idType, idHash)
      case _                                => throw new RuntimeException(s"${dxId} is not a valid object ID")
    }
  }

  def isDataObjectId(objName: String): Boolean = {
    try {
      parseDxObjectId(objName)
      true
    } catch {
      case _: Throwable => false
    }
  }

  def dxDataObjectToUri(dxObj: DxDataObject): String = {
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
  def dxExecutionToEbor(dxExec: DxExecution, fieldName: String): JsValue = {
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
