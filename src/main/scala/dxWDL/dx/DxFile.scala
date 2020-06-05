package dxWDL.dx

import com.dnanexus.DXAPI
import com.fasterxml.jackson.databind.JsonNode
import dxWDL.base.Verbose
import spray.json._

// maximal number of objects in a single API request
import dxWDL.base.Utils.DXAPI_NUM_OBJECTS_LIMIT

case class DxFilePart(state: String, size: Long, md5: String)

object DxArchivalState extends Enumeration {
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

case class DxFile(id: String, project: Option[DxProject]) extends DxDataObject {

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
    val request = JsObject(projSpec + ("fields" -> DxObject.requestFields(allFields)))
    val response =
      DXAPI.fileDescribe(id, DxUtils.jsonNodeOfJsValue(request), classOf[JsonNode], DxUtils.dxEnv)
    val descJs: JsValue = DxUtils.jsValueOfJsonNode(response)
    val desc = DxFile.parseJsonFileDesribe(descJs)

    // populate optional fields
    val details = descJs.asJsObject.fields.get("details")
    val props = descJs.asJsObject.fields.get("properties").map(DxObject.parseJsonProperties)
    val parts = descJs.asJsObject.fields.get("parts").map(DxFile.parseFileParts)
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
  def getInstance(id: String): DxFile = {
    DxObject.getInstance(id) match {
      case f: DxFile => f
      case _ =>
        throw new IllegalArgumentException(s"${id} isn't a file")
    }
  }

  def getInstance(id: String, project: DxProject): DxFile = {
    DxObject.getInstance(id, Some(project)) match {
      case f: DxFile => f
      case _ =>
        throw new IllegalArgumentException(s"${id} isn't a file")
    }
  }

  // Parse a JSON description of a file received from the platform
  def parseJsonFileDesribe(descJs: JsValue): DxFileDescribe = {
    val desc = descJs.asJsObject
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
    val sizeRaw = descJs.asJsObject.fields.get("size").getOrElse(JsNumber(0))
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
    //System.out.println(jsv.prettyPrint)
    jsv.asJsObject.fields.map {
      case (partNumber, partDesc) =>
        val dxPart = partDesc.asJsObject.getFields("md5", "size", "state") match {
          case Seq(JsString(md5), JsNumber(size), JsString(state)) =>
            DxFilePart(state, size.toLong, md5)
          case _ => throw new Exception(s"malformed part description ${partDesc.prettyPrint}")
        }
        partNumber.toInt -> dxPart
    }.toMap
  }

  // Describe a large number of platform objects in bulk.
  private def submitRequest(objs: Vector[DxFile],
                            extraFields: Vector[String],
                            project: Option[DxProject]): Map[DxFile, DxFileDescribe] = {
    val ids = objs.map(file => file.getId)
    val dxFindDataObjects = DxFindDataObjects(None, Verbose(on = false, quiet = true, keywords = Set.empty))

    dxFindDataObjects.apply(
      dxProject = project,
      folder = None,
      recurse = true,
      klassRestriction = Some("file"),
      withProperties = Vector.empty,
      nameConstraints = Vector.empty,
      withInputOutputSpec = true,
      idConstraints = ids,
      extrafields = extraFields
    ).asInstanceOf[Map[DxFile, DxFileDescribe]]
  }

  // Describe the names of all the files in one batch. This is much more efficient
  // than submitting file describes one-by-one.
  def bulkDescribe(objs: Vector[DxFile],
                   extraFields: Set[Field.Value] = Set.empty): Map[DxFile, DxFileDescribe] = {
    if (objs.isEmpty) {
      // avoid an unnessary API call; this is important for unit tests
      // that do not have a network connection.
      return Map.empty
    }
    var descriptions: Map[DxFile, DxFileDescribe] = Map.empty
    val objsByProj = objs.groupBy(file => file.project)
    for ((proj, files) <- objsByProj) {

      // Limit on number of objects in one API request
      val slices = files.grouped(DXAPI_NUM_OBJECTS_LIMIT).toList

      val extraFieldsStr = extraFields
        .map {
          case Field.Details => "details"
          case Field.Parts   => "parts"
        }
        .toSet
        .toVector

      // iterate on the ranges
      descriptions ++= slices.foldLeft(Map.empty[DxFile, DxFileDescribe]) {
        case (accu, objRange) =>
          accu ++ submitRequest(objRange.toVector, extraFieldsStr, proj)
      }
    }
    descriptions
  }
}
