package dx.api

import dx.AppInternalException
import spray.json._
import wdlTools.util.Enum

case class DxFilePart(state: String, size: Long, md5: String)

object DxArchivalState extends Enum {
  type DxArchivalState = Value
  val Live, Archival, Archived, Unarchiving = Value

  def fromString(jsv: JsValue): DxArchivalState.Value = {
    jsv match {
      case JsString(s) => withNameIgnoreCase(s)
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

case class DxFile(dxApi: DxApi, id: String, project: Option[DxProject])
    extends CachingDxObject[DxFileDescribe]
    with DxDataObject {
  def describeNoCache(fields: Set[Field.Value] = Set.empty): DxFileDescribe = {
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
    DxFile.parseDescribeJson(descJs)
  }

  def asJson: JsValue = {
    project match {
      case None =>
        JsObject(DxUtils.DxLinkKey -> JsString(id))
      case Some(p) =>
        JsObject(
            DxUtils.DxLinkKey -> JsObject(
                "project" -> JsString(p.id),
                "id" -> JsString(id)
            )
        )
    }
  }

  // Convert a dx-file to a string with the format:
  //   dx://proj-xxxx:file-yyyy::/A/B/C.txt
  //
  // This is needed for operations like:
  //     File filename
  //     String  = sub(filename, ".txt", "") + ".md"
  // The standard library functions requires the file name to
  // end with a posix-like name. It can't just be:
  // "dx://file-xxxx", or "dx://proj-xxxx:file-yyyy". It needs
  // to be something like:  dx://xxxx:yyyy:genome.txt, so that
  // we can change the suffix.
  //
  // We need to change the standard so that the conversion from file to
  // string is well defined, and requires an explicit conversion function.
  //
  def asUri: String = {
    val desc = describe()
    val logicalName = s"${desc.folder}/${desc.name}"
    project match {
      case None =>
        s"${DxPath.DxUriPrefix}${id}::${logicalName}"
      case Some(proj) =>
        val projId = proj.id
        s"${DxPath.DxUriPrefix}${projId}:${id}::${logicalName}"
    }
  }
}

object DxFile {
  // Parse a JSON description of a file received from the platform
  def parseDescribeJson(descJs: JsObject): DxFileDescribe = {
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
                       DxArchivalState.withNameIgnoreCase(archivalState),
                       None,
                       None,
                       None)
      case _ =>
        throw new Exception(s"Malformed JSON ${descJs}")
    }

    // populate the size field. It is missing from files that are in the open or closing states.
    val sizeRaw = descJs.fields.getOrElse("size", JsNumber(0))
    val size = sizeRaw match {
      case JsNumber(x) => x.toLong
      case other       => throw new Exception(s"size ${other} is not a number")
    }

    // optional fields
    val details = descJs.fields.get("details")
    val props = descJs.fields.get("properties").map(DxObject.parseJsonProperties)
    val parts = descJs.fields.get("parts").map(DxFile.parseFileParts)

    desc.copy(size = size, details = details, properties = props, parts = parts)
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

  def isLinkJson(jsv: JsValue): Boolean = {
    jsv match {
      case JsObject(fields) if fields.keySet == Set(DxUtils.DxLinkKey) => true
      case _                                                           => false
    }
  }

  // Parse a dnanexus file descriptor. Examples:
  //
  // {
  //   DxUtils.DxLinkKey: {
  //     "project": "project-BKJfY1j0b06Z4y8PX8bQ094f",
  //     "id": "file-BKQGkgQ0b06xG5560GGQ001B"
  //   },
  //   DxUtils.DxLinkKey: "file-F0J6JbQ0ZvgVz1J9q5qKfkqP"
  // }
  def fromJson(dxApi: DxApi, jsValue: JsValue): DxFile = {
    val innerObj = jsValue match {
      case JsObject(fields) if fields.contains(DxUtils.DxLinkKey) =>
        fields(DxUtils.DxLinkKey)
      case _ =>
        throw new AppInternalException(
            s"An object with key '$$dnanexus_link' is expected, not $jsValue"
        )
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

  def isDxFile(jsValue: JsValue): Boolean = {
    jsValue match {
      case JsObject(fields) =>
        fields.get(DxUtils.DxLinkKey) match {
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

  // Search through a JSON value for all the dx:file links inside it. Returns
  // those as a vector.
  def findFiles(dxApi: DxApi, jsValue: JsValue): Vector[DxFile] = {
    jsValue match {
      case JsBoolean(_) | JsNumber(_) | JsString(_) | JsNull =>
        Vector.empty[DxFile]
      case JsObject(_) if DxFile.isDxFile(jsValue) =>
        Vector(DxFile.fromJson(dxApi, jsValue))
      case JsObject(fields) =>
        fields.map { case (_, v) => findFiles(dxApi, v) }.toVector.flatten
      case JsArray(elems) =>
        elems.flatMap(e => findFiles(dxApi, e))
    }
  }
}
