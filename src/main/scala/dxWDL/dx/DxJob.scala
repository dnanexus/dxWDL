package dxWDL.dx

import com.dnanexus.DXAPI
import com.fasterxml.jackson.databind.JsonNode
import spray.json._

case class DxJobDescribe(project : String,
                         id  : String,
                         name : String,
                         created : Long,
                         modified : Long,
                         properties: Option[Map[String, String]],
                         details : Option[JsValue],
                         applet : DxApplet,
                         parentJob : Option[DxJob],
                         analysis : Option[DxAnalysis]) extends DxObjectDescribe

case class DxJob(id : String,
                 project : Option[DxProject] = None) extends DxObject with DxExecution {
    def describe(fields : Set[Field.Value] = Set.empty) : DxJobDescribe = {
        val projSpec = DxObject.maybeSpecifyProject(project)
        val baseFields = DxObject.requestFields(fields)
        val allFields = baseFields ++ Map("applet" -> JsTrue,
                                          "parentJob"  -> JsTrue,
                                          "analysis" -> JsTrue)
        val request = JsObject(projSpec + ("fields" -> JsObject(allFields)))
        val response = DXAPI.analysisDescribe(id,
                                              DxUtils.jsonNodeOfJsValue(request),
                                              classOf[JsonNode],
                                              DxUtils.dxEnv)
        val descJs:JsValue = DxUtils.jsValueOfJsonNode(response)
        val desc = descJs.asJsObject.getFields("project", "id", "name", "folder",
                                               "created", "modified",
                                               "applet") match {
            case Seq(JsString(project), JsString(id), JsString(name),
                     JsNumber(created), JsNumber(modified), JsString(applet)) =>
                DxJobDescribe(project,
                              id,
                              name,
                              created.toLong,
                              modified.toLong,
                              None,
                              None,
                              DxApplet.getInstance(applet),
                              None,
                              None)
            case _ =>
                throw new Exception(s"Malformed JSON ${descJs}")
        }

        val details = descJs.asJsObject.fields.get("details")
        val props = descJs.asJsObject.fields.get("properties").map(DxObject.parseJsonProperties)
        val parentJob = descJs.asJsObject.fields.get("parentJob").map{
            case JsString(x) => DxJob.getInstance(x)
            case other => throw new Exception(s"should be a job ${other}")
        }
        val analysis = descJs.asJsObject.fields.get("analysis").map{
            case JsString(x) => DxAnalysis.getInstance(x)
            case other => throw new Exception(s"should be an analysis ${other}")
        }
        desc.copy(details = details,
                  properties = props,
                  parentJob = parentJob,
                  analysis = analysis)
    }
}

object DxJob {
    def getInstance(id : String) : DxJob = {
        if (id.startsWith("job-"))
            return DxJob(id, None)
        throw new IllegalArgumentException(s"${id} isn't a job")
    }
}
