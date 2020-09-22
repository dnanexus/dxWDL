package dx.api

import spray.json._

case class DxProjectDescribe(id: String,
                             name: String,
                             created: Long,
                             modified: Long,
                             properties: Option[Map[String, String]],
                             details: Option[JsValue],
                             billTo: Option[String],
                             region: Option[String],
                             availableInstanceTypes: Option[JsValue])
    extends DxObjectDescribe

case class FolderContents(dataObjects: Vector[DxDataObject], subFolders: Vector[String])

// A project is a subtype of a container
case class DxProject(dxApi: DxApi, id: String) extends DxDataObject {
  def describe(fields: Set[Field.Value] = Set.empty): DxProjectDescribe = {
    val defaultFields =
      Set(Field.Project, Field.Id, Field.Name, Field.Folder, Field.Created, Field.Modified)
    val request = Map("fields" -> DxObject.requestFields(fields ++ defaultFields))
    val descJs = id match {
      case _ if id.startsWith("project-") =>
        dxApi.projectDescribe(id, request)
      case _ if id.startsWith("container-") =>
        dxApi.containerDescribe(id, request)
      case _ =>
        throw new Exception(s"invalid project id ${id}")
    }
    val desc = descJs.getFields("id", "name", "created", "modified") match {
      case Seq(JsString(id), JsString(name), JsNumber(created), JsNumber(modified)) =>
        DxProjectDescribe(id, name, created.toLong, modified.toLong, None, None, None, None, None)
      case _ =>
        throw new Exception(s"malformed JSON ${descJs}")
    }
    val details = descJs.fields.get("details")
    val props = descJs.fields.get("properties").map(DxObject.parseJsonProperties)
    val billTo = descJs.fields.get("billTo") match {
      case Some(JsString(x)) => Some(x)
      case _                 => None
    }
    val region = descJs.fields.get("region") match {
      case Some(JsString(x)) => Some(x)
      case _                 => None
    }
    val availableInstanceTypes = descJs.fields.get("availableInstanceTypes")
    desc.copy(details = details,
              properties = props,
              billTo = billTo,
              region = region,
              availableInstanceTypes = availableInstanceTypes)
  }

  def listFolder(path: String): FolderContents = {
    val request = Map("folder" -> JsString(path))
    val repJs = id match {
      case _ if id.startsWith("project-") =>
        dxApi.projectListFolder(id, request)
      case _ if id.startsWith("container-") =>
        dxApi.containerListFolder(id, request)
      case _ =>
        throw new Exception(s"invalid project id ${id}")
    }

    // extract object ids
    val objsJs = repJs.fields("objects") match {
      case JsArray(a) => a
      case _          => throw new Exception("not an array")
    }
    val objs = objsJs.map {
      case JsObject(fields) =>
        fields.get("id") match {
          case Some(JsString(id)) =>
            dxApi.dataObject(id, Some(this))
          case other =>
            throw new Exception(s"malformed json reply ${other}")
        }
      case other =>
        throw new Exception(s"malformed json reply ${other}")
    }

    // extract sub folders
    val subdirsJs = repJs.fields("folders") match {
      case JsArray(a) => a
      case _          => throw new Exception("not an array")
    }
    val subdirs = subdirsJs.map {
      case JsString(x) => x
      case other =>
        throw new Exception(s"malformed json reply ${other}")
    }

    FolderContents(objs, subdirs)
  }

  def newFolder(folderPath: String, parents: Boolean): Unit = {
    val request = Map("project" -> JsString(id),
                      "folder" -> JsString(folderPath),
                      "parents" -> JsBoolean(parents))
    id match {
      case _ if id.startsWith("project-") =>
        dxApi.projectNewFolder(id, request)
      case _ if id.startsWith("container-") =>
        dxApi.containerNewFolder(id, request)
      case _ =>
        throw new Exception(s"invalid project id ${id}")
    }
  }

  def removeFolder(folderPath: String,
                   recurse: Boolean,
                   partial: Boolean = false,
                   force: Boolean = true): Boolean = {
    val request = Map("project" -> JsString(id),
                      "folder" -> JsString(folderPath),
                      "recurse" -> JsBoolean(recurse),
                      "partial" -> JsBoolean(partial),
                      "force" -> JsBoolean(force))
    id match {
      case _ if id.startsWith("project-") =>
        dxApi.projectRemoveFolder(id, request)
      case _ if id.startsWith("container-") =>
        dxApi.containerRemoveFolder(id, request)
      case _ =>
        throw new Exception(s"invalid project id ${id}")
    }
  }

  def moveObjects(objs: Vector[DxDataObject], destinationFolder: String): Unit = {
    val request = Map(
        "objects" -> JsArray(objs.map(x => JsString(x.id))),
        "folders" -> JsArray(Vector.empty[JsString]),
        "destination" -> JsString(destinationFolder)
    )
    id match {
      case _ if id.startsWith("project-") =>
        dxApi.projectMove(id, request)
      case _ if id.startsWith("container-") =>
        dxApi.containerMove(id, request)
      case _ =>
        throw new Exception(s"invalid project id ${id}")
    }
  }

  def removeObjects(objs: Vector[DxDataObject]): Unit = {
    val request = Map(
        "objects" -> JsArray(objs.map(x => JsString(x.id))),
        "force" -> JsFalse
    )
    id match {
      case _ if id.startsWith("project-") =>
        dxApi.projectRemoveObjects(id, request)
      case _ if id.startsWith("container-") =>
        dxApi.containerRemoveObjects(id, request)
      case _ =>
        throw new Exception(s"invalid project id ${id}")
    }
  }
}
