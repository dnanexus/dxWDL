package dxWDL.dx

import com.dnanexus.DXAPI
import com.fasterxml.jackson.databind.JsonNode
import dxWDL.base._
import spray.json._

case class DxAppletDescribe(project: String,
                            id: String,
                            name: String,
                            folder: String,
                            created: Long,
                            modified: Long,
                            properties: Option[Map[String, String]],
                            details: Option[JsValue],
                            inputSpec: Option[Vector[IOParameter]],
                            outputSpec: Option[Vector[IOParameter]],
                            description: Option[String] = None,
                            developerNotes: Option[String] = None,
                            summary: Option[String] = None,
                            title: Option[String] = None,
                            types: Option[Vector[String]] = None,
                            tags: Option[Vector[String]] = None,
                            runSpec: Option[JsValue] = None,
                            access: Option[JsValue] = None,
                            ignoreReuse: Option[Boolean] = None)
    extends DxObjectDescribe

case class DxApplet(id: String, project: Option[DxProject]) extends DxExecutable {
  def describe(fields: Set[Field.Value] = Set.empty): DxAppletDescribe = {
    val projSpec = DxObject.maybeSpecifyProject(project)
    val defaultFields = Set(Field.Project,
                            Field.Id,
                            Field.Name,
                            Field.Folder,
                            Field.Created,
                            Field.Modified,
                            Field.InputSpec,
                            Field.OutputSpec)
    val allFields = fields ++ defaultFields
    val request = JsObject(projSpec + ("fields" -> DxObject.requestFields(allFields)))
    val response =
      DXAPI.appletDescribe(id, DxUtils.jsonNodeOfJsValue(request), classOf[JsonNode], DxUtils.dxEnv)
    val descJs: JsValue = DxUtils.jsValueOfJsonNode(response)
    val desc = descJs.asJsObject.getFields("project",
                                           "id",
                                           "name",
                                           "folder",
                                           "created",
                                           "modified",
                                           "inputSpec",
                                           "outputSpec") match {
      case Seq(JsString(project),
               JsString(id),
               JsString(name),
               JsString(folder),
               JsNumber(created),
               JsNumber(modified),
               JsArray(inputSpec),
               JsArray(outputSpec)) =>
        DxAppletDescribe(
            project,
            id,
            name,
            folder,
            created.toLong,
            modified.toLong,
            None,
            None,
            Some(DxObject.parseIOSpec(inputSpec)),
            Some(DxObject.parseIOSpec(outputSpec))
        )
      case _ =>
        throw new Exception(s"Malformed JSON ${descJs}")
    }

    val descFields: Map[String, JsValue] = descJs.asJsObject.fields
    val details = descFields.get("details")
    val props = descFields.get("properties").map(DxObject.parseJsonProperties)
    val description = descFields.get("description").flatMap(unwrapString)
    val developerNotes = descFields.get("developerNotes").flatMap(unwrapString)
    val summary = descFields.get("summary").flatMap(unwrapString)
    val title = descFields.get("title").flatMap(unwrapString)
    val types = descFields.get("types").flatMap(unwrapStringArray)
    val tags = descFields.get("tags").flatMap(unwrapStringArray)
    val runSpec = descFields.get("runSpec")
    val access = descFields.get("access")
    val ignoreReuse = descFields.get("ignoreReuse").flatMap(unwrapBoolean)
    desc.copy(
        details = details,
        properties = props,
        description = description,
        developerNotes = developerNotes,
        summary = summary,
        title = title,
        types = types,
        tags = tags,
        runSpec = runSpec,
        access = access,
        ignoreReuse = ignoreReuse
    )
  }

  def unwrapString(jsValue: JsValue): Option[String] = {
    jsValue match {
      case JsString(value) => Some(value)
      case _               => None
    }
  }

  def unwrapStringArray(jsValue: JsValue): Option[Vector[String]] = {
    jsValue match {
      case JsArray(array) => Some(array.flatMap(unwrapString))
      case _              => None
    }
  }

  def unwrapBoolean(jsValue: JsValue): Option[Boolean] = {
    jsValue match {
      case JsBoolean(value) => Some(value)
      case _                => None
    }
  }

  def newRun(name: String,
             input: JsValue,
             instanceType: Option[String] = None,
             properties: Map[String, JsValue] = Map.empty,
             delayWorkspaceDestruction: Option[Boolean] = None): DxJob = {
    val fields = Map(
        "name" -> JsString(name),
        "input" -> input
    )
    // If this is a task that specifies the instance type
    // at runtime, launch it in the requested instance.
    val instanceFields = instanceType match {
      case None => Map.empty
      case Some(iType) =>
        Map(
            "systemRequirements" -> JsObject(
                "main" -> JsObject("instanceType" -> JsString(iType))
            )
        )
    }

    val props =
      if (properties.isEmpty)
        Map.empty
      else
        Map("properties" -> JsObject(properties))

    val dwd = delayWorkspaceDestruction match {
      case Some(true) => Map("delayWorkspaceDestruction" -> JsTrue)
      case _          => Map.empty
    }
    val req = JsObject(fields ++ instanceFields ++ props ++ dwd)
    val retval: JsonNode =
      DXAPI.appletRun(this.id, DxUtils.jsonNodeOfJsValue(req), classOf[JsonNode])
    val info: JsValue = DxUtils.jsValueOfJsonNode(retval)
    val id: String = info.asJsObject.fields.get("id") match {
      case Some(JsString(x)) => x
      case _ =>
        throw new AppInternalException(s"Bad format returned from jobNew ${info.prettyPrint}")
    }
    DxJob.getInstance(id)
  }
}

object DxApplet {
  def getInstance(id: String): DxApplet = {
    DxObject.getInstance(id) match {
      case a: DxApplet => a
      case _ =>
        throw new IllegalArgumentException(s"${id} isn't an applet")
    }
  }

  def getInstance(id: String, project: DxProject): DxApplet = {
    DxObject.getInstance(id, Some(project)) match {
      case a: DxApplet => a
      case _ =>
        throw new IllegalArgumentException(s"${id} isn't an applet")
    }
  }
}
