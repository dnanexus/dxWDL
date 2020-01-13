package dxWDL.dx

import com.dnanexus.DXAPI
import com.fasterxml.jackson.databind.JsonNode
import spray.json._

import dxWDL.base.Utils

case class DxProjectDescribe(
    id: String,
    name: String,
    created: Long,
    modified: Long,
    properties: Option[Map[String, String]],
    details: Option[JsValue]
) extends DxObjectDescribe

case class FolderContents(
    dataObjects: Vector[DxDataObject],
    subFolders: Vector[String]
)

// A project is a subtype of a container
case class DxProject(id: String) extends DxDataObject {

  def describe(fields: Set[Field.Value] = Set.empty): DxProjectDescribe = {
    val defaultFields = Set(Field.Id, Field.Name, Field.Created, Field.Modified)
    val request = JsObject("fields" -> DxObject.requestFields(defaultFields))
    val response =
      if (id.startsWith("project-"))
        DXAPI.projectDescribe(
            id,
            DxUtils.jsonNodeOfJsValue(request),
            classOf[JsonNode],
            DxUtils.dxEnv
        )
      else
        DXAPI.containerDescribe(
            id,
            DxUtils.jsonNodeOfJsValue(request),
            classOf[JsonNode],
            DxUtils.dxEnv
        )
    val descJs: JsValue = DxUtils.jsValueOfJsonNode(response)
    val desc = descJs.asJsObject.getFields("id", "name", "created", "modified") match {
      case Seq(JsString(id), JsString(name), JsNumber(created), JsNumber(modified)) =>
        DxProjectDescribe(id, name, created.toLong, modified.toLong, None, None)
      case _ => throw new Exception(s"malformed JSON ${descJs}")
    }
    val details = descJs.asJsObject.fields.get("details")
    val props = descJs.asJsObject.fields
      .get("properties")
      .map(DxObject.parseJsonProperties)
    desc.copy(details = details, properties = props)
  }

  def listFolder(path: String): FolderContents = {
    val request = JsObject("folder" -> JsString(path))
    val response = id match {
      case _ if (id.startsWith("container-")) =>
        DXAPI.containerListFolder(
            id,
            DxUtils.jsonNodeOfJsValue(request),
            classOf[JsonNode],
            DxUtils.dxEnv
        )
      case _ if (id.startsWith("project-")) =>
        DXAPI.projectListFolder(
            id,
            DxUtils.jsonNodeOfJsValue(request),
            classOf[JsonNode],
            DxUtils.dxEnv
        )
      case _ =>
        throw new Exception(s"invalid project id ${id}")
    }
    val repJs: JsValue = DxUtils.jsValueOfJsonNode(response)

    // extract object ids
    val objsJs = repJs.asJsObject.fields("objects") match {
      case JsArray(a) => a
      case _          => throw new Exception("not an array")
    }
    val objs = objsJs.map {
      case JsObject(fields) =>
        fields.get("id") match {
          case Some(JsString(id)) =>
            val o = DxObject.getInstance(id, Some(this))
            o.asInstanceOf[DxDataObject]
          case other =>
            throw new Exception(s"malformed json reply ${other}")
        }
      case other =>
        throw new Exception(s"malformed json reply ${other}")
    }.toVector

    // extract sub folders
    val subdirsJs = repJs.asJsObject.fields("folders") match {
      case JsArray(a) => a
      case _          => throw new Exception("not an array")
    }
    val subdirs = subdirsJs.map {
      case JsString(x) => x
      case other =>
        throw new Exception(s"malformed json reply ${other}")
    }.toVector

    FolderContents(objs, subdirs)
  }

  def newFolder(folderPath: String, parents: Boolean): Unit = {
    val request = JsObject(
        "project" -> JsString(id),
        "folder" -> JsString(folderPath),
        "parents" -> (if (parents) JsTrue else JsFalse)
    )
    val response = id match {
      case _ if (id.startsWith("container-")) =>
        DXAPI.containerNewFolder(
            id,
            DxUtils.jsonNodeOfJsValue(request),
            classOf[JsonNode],
            DxUtils.dxEnv
        )
      case _ if (id.startsWith("project-")) =>
        DXAPI.projectNewFolder(
            id,
            DxUtils.jsonNodeOfJsValue(request),
            classOf[JsonNode],
            DxUtils.dxEnv
        )
      case _ =>
        throw new Exception(s"invalid project id ${id}")
    }
    val repJs: JsValue = DxUtils.jsValueOfJsonNode(response)
    Utils.ignore(repJs)
  }

  def moveObjects(
      objs: Vector[DxDataObject],
      destinationFolder: String
  ): Unit = {
    val request = JsObject(
        "objects" -> JsArray(objs.map { x =>
          JsString(x.id)
        }.toVector),
        "folders" -> JsArray(Vector.empty[JsString]),
        "destination" -> JsString(destinationFolder)
    )
    val response = id match {
      case _ if (id.startsWith("container-")) =>
        DXAPI.containerMove(
            id,
            DxUtils.jsonNodeOfJsValue(request),
            classOf[JsonNode],
            DxUtils.dxEnv
        )
      case _ if (id.startsWith("project-")) =>
        DXAPI.projectMove(
            id,
            DxUtils.jsonNodeOfJsValue(request),
            classOf[JsonNode],
            DxUtils.dxEnv
        )
      case _ =>
        throw new Exception(s"invalid project id ${id}")
    }
    val repJs: JsValue = DxUtils.jsValueOfJsonNode(response)
    Utils.ignore(repJs)
  }

  def removeObjects(objs: Vector[DxDataObject]): Unit = {
    val request = JsObject("objects" -> JsArray(objs.map { x =>
      JsString(x.id)
    }.toVector), "force" -> JsFalse)
    val response = id match {
      case _ if (id.startsWith("container-")) =>
        DXAPI.containerRemoveObjects(
            id,
            DxUtils.jsonNodeOfJsValue(request),
            classOf[JsonNode],
            DxUtils.dxEnv
        )
      case _ if (id.startsWith("project-")) =>
        DXAPI.projectRemoveObjects(
            id,
            DxUtils.jsonNodeOfJsValue(request),
            classOf[JsonNode],
            DxUtils.dxEnv
        )
      case _ =>
        throw new Exception(s"invalid project id ${id}")
    }
    val repJs: JsValue = DxUtils.jsValueOfJsonNode(response)
    Utils.ignore(repJs)
  }
}

object DxProject {

  def getInstance(id: String): DxProject = {
    DxObject.getInstance(id) match {
      case p: DxProject => p
      case _ =>
        throw new IllegalArgumentException(s"${id} isn't a project")
    }
  }
}
