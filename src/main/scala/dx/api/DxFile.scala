package dx.api

import dx.AppInternalException
import spray.json._

case class DxFilePart(state: String, size: Long, md5: String)

object DxArchivalState extends Enumeration {
  type DxArchivalState = Value
  val LIVE, ARCHIVAL, ARCHIVED, UNARCHIVING = Value

  def fromString(s: String): DxArchivalState.Value = {
    s match {
      case "live"        => LIVE
      case "archival"    => ARCHIVAL
      case "archived"    => ARCHIVED
      case "unarchiving" => UNARCHIVING
    }
  }

  def fromString(jsv: JsValue): DxArchivalState.Value = {
    jsv match {
      case JsString(s) => fromString(s)
      case other       => throw new Exception(s"Archival state is not a string type ${other}")
    }
  }
}

case class DxFileDescribe(project: String,
                          id: String,
                          name: String,
                          folder: String,
                          created: Long,
                          modified: Long,
                          size: Long,
                          archivalState: DxArchivalState.Value,
                          properties: Option[Map[String, String]],
                          details: Option[JsValue],
                          parts: Option[Map[Int, DxFilePart]])
    extends DxObjectDescribe

case class DxFile(dxApi: DxApi, id: String, project: Option[DxProject]) extends DxDataObject {
  def describe(fields: Set[Field.Value] = Set.empty): DxFileDescribe = {
    val projSpec = DxObject.maybeSpecifyProject(project)
    val defaultFields = Set(Field.Project,
                            Field.Id,
                            Field.Name,
                            Field.Folder,
                            Field.Created,
                            Field.Modified,
                            Field.Size,
                            Field.ArchivalState)
    val allFields = fields ++ defaultFields
    val descJs = dxApi.fileDescribe(id, projSpec + ("fields" -> DxObject.requestFields(allFields)))
    val desc = DxFile.parseJsonFileDesribe(descJs)

    // populate optional fields
    val details = descJs.fields.get("details")
    val props = descJs.fields.get("properties").map(DxObject.parseJsonProperties)
    val parts = descJs.fields.get("parts").map(DxFile.parseFileParts)
    desc.copy(details = details, properties = props, parts = parts)
  }

  def getLinkAsJson: JsValue = {
    project match {
      case None =>
        JsObject("$dnanexus_link" -> JsString(id))
      case Some(p) =>
        JsObject(
            "$dnanexus_link" -> JsObject(
                "project" -> JsString(p.id),
                "id" -> JsString(id)
            )
        )
    }
  }
}

object DxFile {
  // Parse a JSON description of a file received from the platform
  def parseJsonFileDesribe(descJs: JsObject): DxFileDescribe = {
    val desc = descJs
      .getFields("project", "id", "name", "folder", "created", "modified", "archivalState") match {
      case Seq(JsString(project),
               JsString(id),
               JsString(name),
               JsString(folder),
               JsNumber(created),
               JsNumber(modified),
               JsString(archivalState)) =>
        DxFileDescribe(project,
                       id,
                       name,
                       folder,
                       created.toLong,
                       modified.toLong,
                       0,
                       DxArchivalState.fromString(archivalState),
                       None,
                       None,
                       None)
      case _ =>
        throw new Exception(s"Malformed JSON ${descJs}")
    }

    // populate the size field. It is missing from files that are in the open or closing
    // states.
    val sizeRaw = descJs.fields.getOrElse("size", JsNumber(0))
    val size = sizeRaw match {
      case JsNumber(x) => x.toLong
      case other       => throw new Exception(s"size ${other} is not a number")
    }

    desc.copy(size = size)
  }

  // Parse the parts from a description of a file
  // The format is something like this:
  // {
  //  "1": {
  //    "md5": "71565d7f4dc0760457eb252a31d45964",
  //    "size": 42,
  //    "state": "complete"
  //  }
  //}
  //
  def parseFileParts(jsv: JsValue): Map[Int, DxFilePart] = {
    jsv.asJsObject.fields.map {
      case (partNumber, partDesc) =>
        val dxPart = partDesc.asJsObject.getFields("md5", "size", "state") match {
          case Seq(JsString(md5), JsNumber(size), JsString(state)) =>
            DxFilePart(state, size.toLong, md5)
          case _ => throw new Exception(s"malformed part description ${partDesc.prettyPrint}")
        }
        partNumber.toInt -> dxPart
    }
  }

  // Parse a dnanexus file descriptor. Examples:
  //
  // "$dnanexus_link": {
  //    "project": "project-BKJfY1j0b06Z4y8PX8bQ094f",
  //    "id": "file-BKQGkgQ0b06xG5560GGQ001B"
  //   }
  //
  //  {"$dnanexus_link": "file-F0J6JbQ0ZvgVz1J9q5qKfkqP"}
  //
  def fromJsValue(dxApi: DxApi, jsValue: JsValue): DxFile = {
    val innerObj = jsValue match {
      case JsObject(fields) =>
        fields.get("$dnanexus_link") match {
          case None    => throw new AppInternalException(s"Non-dxfile json $jsValue")
          case Some(x) => x
        }
      case _ =>
        throw new AppInternalException(s"Non-dxfile json $jsValue")
    }

    val (fid, projId): (String, Option[String]) = innerObj match {
      case JsString(fid) =>
        // We just have a file-id
        (fid, None)
      case JsObject(linkFields) =>
        // file-id and project-id
        val fid =
          linkFields.get("id") match {
            case Some(JsString(s)) => s
            case _                 => throw new AppInternalException(s"No file ID found in $jsValue")
          }
        linkFields.get("project") match {
          case Some(JsString(pid: String)) => (fid, Some(pid))
          case _                           => (fid, None)
        }
      case _ =>
        throw new AppInternalException(s"Could not parse a dxlink from $innerObj")
    }

    projId match {
      case None      => DxFile(dxApi, fid, None)
      case Some(pid) => DxFile(dxApi, fid, Some(DxProject(dxApi, pid)))
    }
  }

  def toJsValue(dxFile: DxFile): JsValue = {
    dxFile.getLinkAsJson
  }

  def isDxFile(jsValue: JsValue): Boolean = {
    jsValue match {
      case JsObject(fields) =>
        fields.get("$dnanexus_link") match {
          case Some(JsString(s)) if s.startsWith("file-") => true
          case Some(JsObject(linkFields)) =>
            linkFields.get("id") match {
              case Some(JsString(s)) if s.startsWith("file-") => true
              case _                                          => false
            }
          case _ => false
        }
      case _ => false
    }
  }
}
