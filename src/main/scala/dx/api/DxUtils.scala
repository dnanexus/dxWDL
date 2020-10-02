package dx.api

import spray.json._

object DxUtils {
  val DxLinkKey = "$dnanexus_link"
  val RecordClass = "record"
  val RecordPrefix = s"${RecordClass}-"
  private val dataObjectClasses =
    Set("applet", "database", "dbcluster", "file", RecordClass, "workflow")
  private val containerClasses = Set("container", "project")
  private val executableClasses = Set("applet", "app", "globalworkflow", "workflow")
  private val executionClasses = Set("analysis", "job")
  private val allClasses = dataObjectClasses | containerClasses | executableClasses | executionClasses
  private val objectIdRegexp =
    s"^(${allClasses.mkString("|")})-([A-Za-z0-9]{24})$$".r
  private val dataObjectIdRegexp =
    s"^(${dataObjectClasses.mkString("|")})-([A-Za-z0-9]{24})$$".r
  private val executableIdRegexp =
    s"^(${executableClasses.mkString("|")})-([A-Za-z0-9]{24})$$".r
  // Other entity ID regexps if/when needed:
  //  private val containerIdRegexp = s"^(${containerClasses.mkString("|")})-(\\w{24})$$".r
  //  private val executableIdRegexp = s"^(${executableClasses.mkString("|")})-(\\w{24})$$".r
  //  private val executionIdRegexp = s"^(${executionClasses.mkString("|")})-(\\w{24})$$".r

  def parseObjectId(dxId: String): (String, String) = {
    dxId match {
      case objectIdRegexp(idType, idHash) =>
        (idType, idHash)
      case _ =>
        throw new IllegalArgumentException(s"${dxId} is not a valid object ID")
    }
  }

  def isObjectId(objName: String): Boolean = {
    try {
      parseObjectId(objName)
      true
    } catch {
      case _: IllegalArgumentException => false
    }
  }

  def parseDataObjectId(dxId: String): (String, String) = {
    dxId match {
      case dataObjectIdRegexp(idType, idHash) =>
        (idType, idHash)
      case _ =>
        throw new IllegalArgumentException(s"${dxId} is not a valid data object ID")
    }
  }

  def isDataObjectId(value: String): Boolean = {
    try {
      parseDataObjectId(value)
      true
    } catch {
      case _: IllegalArgumentException => false
    }
  }

  def isRecordId(objId: String): Boolean = {
    isDataObjectId(objId) && objId.startsWith(RecordPrefix)
  }

  def parseExecutableId(dxId: String): (String, String) = {
    dxId match {
      case executableIdRegexp(idType, idHash) =>
        (idType, idHash)
      case _ =>
        throw new IllegalArgumentException(s"${dxId} is not a valid data object ID")
    }
  }

  def dxDataObjectToUri(dxObj: DxDataObject): String = {
    dxObj match {
      case DxFile(_, _, Some(container)) =>
        s"${DxPath.DxUriPrefix}${container.id}:${dxObj.id}"
      case DxRecord(_, _, Some(container)) =>
        s"${DxPath.DxUriPrefix}${container.id}:${dxObj.id}"
      case _ =>
        s"${DxPath.DxUriPrefix}${dxObj.id}"
    }
  }

  // Create a dx link to a field in an execution. The execution could
  // be a job or an analysis.
  def dxExecutionToEbor(dxExec: DxExecution, fieldName: String): JsValue = {
    dxExec match {
      case _: DxJob =>
        JsObject(
            DxLinkKey -> JsObject("field" -> JsString(fieldName), "job" -> JsString(dxExec.id))
        )
      case _: DxAnalysis =>
        JsObject(
            DxLinkKey -> JsObject("field" -> JsString(fieldName), "analysis" -> JsString(dxExec.id))
        )
      case _ =>
        throw new Exception(s"Cannot create EBOR for execution ${dxExec.id}")
    }
  }
}
