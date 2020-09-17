package dx.api

import spray.json._

case class DxFindDataObjects(dxApi: DxApi = DxApi.get, limit: Option[Int] = None) {
  private def parseDescribe(jsv: JsValue,
                            dxobj: DxDataObject,
                            dxProject: DxProject): DxObjectDescribe = {
    val size = jsv.asJsObject.fields.get("size") match {
      case None                 => None
      case Some(JsNumber(size)) => Some(size.toLong)
      case Some(other)          => throw new Exception(s"malformed size field ${other}")
    }
    val name: String = jsv.asJsObject.fields.get("name") match {
      case None                 => throw new Exception("name field missing")
      case Some(JsString(name)) => name
      case other                => throw new Exception(s"malformed name field ${other}")
    }
    val folder = jsv.asJsObject.fields.get("folder") match {
      case None                   => throw new Exception("folder field missing")
      case Some(JsString(folder)) => folder
      case Some(other)            => throw new Exception(s"malformed folder field ${other}")
    }
    val properties: Map[String, String] = jsv.asJsObject.fields.get("properties") match {
      case None        => Map.empty
      case Some(props) => DxObject.parseJsonProperties(props)
    }
    val inputSpec: Option[Vector[IOParameter]] = jsv.asJsObject.fields.get("inputSpec") match {
      case None         => None
      case Some(JsNull) => None
      case Some(JsArray(iSpecVec)) =>
        Some(iSpecVec.map(iSpec => IOParameter.parseIoParam(dxApi, iSpec)))
      case Some(other) =>
        throw new Exception(s"malformed inputSpec field ${other}")
    }
    val outputSpec: Option[Vector[IOParameter]] = jsv.asJsObject.fields.get("outputSpec") match {
      case None         => None
      case Some(JsNull) => None
      case Some(JsArray(oSpecVec)) =>
        Some(oSpecVec.map(oSpec => IOParameter.parseIoParam(dxApi, oSpec)))
      case Some(other) =>
        throw new Exception(s"malformed output field ${other}")
    }
    val created: Long = jsv.asJsObject.fields.get("created") match {
      case None                 => throw new Exception("'created' field is missing")
      case Some(JsNumber(date)) => date.toLong
      case Some(other)          => throw new Exception(s"malformed created field ${other}")
    }
    val modified: Long = jsv.asJsObject.fields.get("modified") match {
      case None                 => throw new Exception("'modified' field is missing")
      case Some(JsNumber(date)) => date.toLong
      case Some(other)          => throw new Exception(s"malformed created field ${other}")
    }
    val details: Option[JsValue] = jsv.asJsObject.fields.get("details")

    dxobj match {
      case _: DxApp =>
        DxAppDescribe(dxobj.id,
                      name,
                      created,
                      modified,
                      Some(properties),
                      details,
                      inputSpec,
                      outputSpec)
      case _: DxApplet =>
        DxAppletDescribe(dxProject.id,
                         dxobj.id,
                         name,
                         folder,
                         created,
                         modified,
                         Some(properties),
                         details,
                         inputSpec,
                         outputSpec)
      case _: DxWorkflow =>
        DxAppletDescribe(dxProject.id,
                         dxobj.id,
                         name,
                         folder,
                         created,
                         modified,
                         Some(properties),
                         details,
                         inputSpec,
                         outputSpec)
      case _: DxFile =>
        val archivalState = jsv.asJsObject.fields.get("archivalState") match {
          case None              => throw new Exception("'archivalState' field is missing")
          case Some(JsString(x)) => DxArchivalState.withNameIgnoreCase(x)
          case Some(other)       => throw new Exception(s"malformed archivalState field ${other}")
        }
        DxFileDescribe(
            dxProject.id,
            dxobj.id,
            name,
            folder,
            created,
            modified,
            size.get,
            archivalState,
            Some(properties),
            details,
            None
        )
      case other =>
        throw new Exception(s"unsupported object ${other}")
    }
  }

  private def parseOneResult(jsv: JsValue): (DxDataObject, DxObjectDescribe) = {
    jsv.asJsObject.getFields("project", "id", "describe") match {
      case Seq(JsString(projectId), JsString(dxid), desc) =>
        val dxProj = dxApi.project(projectId)
        val dxDataObj = dxApi.dataObject(dxid, Some(dxProj))
        val dxDesc = parseDescribe(desc, dxDataObj, dxProj)
        dxDataObj match {
          case dataObject: CachingDxObject[_] =>
            dataObject.cacheDescribe(dxDesc)
          case _ =>
            // TODO: make all data objects caching, and throw exception here
            ()
        }
        (dxDataObj, dxDesc)
      case _ =>
        throw new Exception(s"""|malformed result: expecting {project, id, describe} fields, got:
                                |${jsv.prettyPrint}
                                |""".stripMargin)
    }
  }

  private def buildScope(dxProject: DxProject,
                         folder: Option[String],
                         recurse: Boolean): JsValue = {
    val part1 = Map("project" -> JsString(dxProject.getId))
    val part2 = folder match {
      case None       => Map.empty
      case Some(path) => Map("folder" -> JsString(path))
    }
    val part3 =
      if (recurse)
        Map("recurse" -> JsBoolean(true))
      else
        Map("recurse" -> JsBoolean(false))
    JsObject(part1 ++ part2 ++ part3)
  }

  // Submit a request for a limited number of objects
  private def submitRequest(
      scope: Option[JsValue],
      dxProject: Option[DxProject],
      cursor: Option[JsValue],
      klass: Option[String],
      tagConstraints: Vector[String],
      nameConstraints: Vector[String],
      withInputOutputSpec: Boolean,
      idConstraints: Vector[String],
      extraFields: Set[Field.Value]
  ): (Map[DxDataObject, DxObjectDescribe], Option[JsValue]) = {
    var fields = Set(Field.Name, Field.Folder, Field.Size, Field.ArchivalState, Field.Properties)
    fields ++= extraFields
    if (withInputOutputSpec) {
      fields ++= Set(Field.InputSpec, Field.OutputSpec)
    }
    val reqFields =
      Map("visibility" -> JsString("either"), "describe" -> DxObject.requestFields(fields))
    val projField = dxProject match {
      case None    => Map.empty
      case Some(p) => Map("project" -> JsString(p.getId))
    }
    val scopeField = scope match {
      case None    => Map.empty
      case Some(s) => Map("scope" -> s)
    }
    val limitField = limit match {
      case None      => Map.empty
      case Some(lim) => Map("limit" -> JsNumber(lim))
    }
    val cursorField = cursor match {
      case None              => Map.empty
      case Some(cursorValue) => Map("starting" -> cursorValue)
    }
    val classField = klass match {
      case None    => Map.empty
      case Some(k) => Map("class" -> JsString(k))
    }
    val tagsField =
      if (tagConstraints.isEmpty) {
        Map.empty
      } else {
        Map("tags" -> JsArray(tagConstraints.map(JsString(_))))
      }

    val namePcreField =
      if (nameConstraints.isEmpty) {
        Map.empty
      } else if (nameConstraints.size == 1) {
        // Just one name, no need to use regular expressions
        Map("name" -> JsString(nameConstraints(0)))
      } else {
        // Make a conjunction of all the legal names. For example:
        // ["Nice", "Foo", "Bar"] ===>
        //  [(Nice)|(Foo)|(Bar)]
        val orAll = nameConstraints
          .map { x =>
            s"(${x})"
          }
          .mkString("|")
        Map("name" -> JsObject("regexp" -> JsString(s"[${orAll}]")))
      }

    val idField =
      if (idConstraints.isEmpty) {
        Map.empty
      } else {
        Map("id" -> JsArray(idConstraints.map { x: String =>
          JsString(x)
        }))
      }

    val repJs = dxApi.findDataObjects(
        reqFields ++ projField ++ scopeField ++ cursorField ++ limitField ++ classField ++ tagsField ++
          namePcreField ++ idField
    )
    val next: Option[JsValue] = repJs.fields.get("next") match {
      case None                  => None
      case Some(JsNull)          => None
      case Some(other: JsObject) => Some(other)
      case Some(other)           => throw new Exception(s"malformed ${other.prettyPrint}")
    }
    val results: Vector[(DxDataObject, DxObjectDescribe)] =
      repJs.fields.get("results") match {
        case None                   => throw new Exception(s"missing results field ${repJs}")
        case Some(JsArray(results)) => results.map(parseOneResult)
        case Some(other)            => throw new Exception(s"malformed results field ${other.prettyPrint}")
      }

    (results.toMap, next)
  }

  /**
    *
    * @param dxProject project to search in; None = search across all projects
    * @param folder folder to search in; None = root folder ("/")
    * @param recurse recurse into subfolders
    * @param classRestriction object classes to search
    * @param withTags objects must have these tags
    * @param nameConstraints object name has to be one of these strings
    * @param withInputOutputSpec should the IO spec be described?
    * @param idConstraints object must have one of these IDs
    * @param extraFields extra fields to describe
    * @return
    */
  def apply(dxProject: Option[DxProject],
            folder: Option[String],
            recurse: Boolean,
            classRestriction: Option[String] = None,
            withTags: Vector[String] = Vector.empty,
            nameConstraints: Vector[String] = Vector.empty,
            withInputOutputSpec: Boolean,
            idConstraints: Vector[String] = Vector.empty,
            extraFields: Set[Field.Value] = Set.empty): Map[DxDataObject, DxObjectDescribe] = {
    classRestriction.foreach { k =>
      if (!(Set("record", "file", "applet", "workflow") contains k))
        throw new Exception("class limitation must be one of {record, file, applet, workflow}")
    }

    val scope: Option[JsValue] = dxProject match {
      case None    => None
      case Some(p) => Some(buildScope(p, folder, recurse))
    }
    var allResults = Map.empty[DxDataObject, DxObjectDescribe]
    var cursor: Option[JsValue] = None
    do {
      val (results, next) = submitRequest(scope,
                                          dxProject,
                                          cursor,
                                          classRestriction,
                                          withTags,
                                          nameConstraints,
                                          withInputOutputSpec,
                                          idConstraints,
                                          extraFields)
      allResults = allResults ++ results
      cursor = next
    } while (cursor.isDefined)

    if (nameConstraints.isEmpty) {
      allResults
    } else {
      // Ensure the the data objects have names in the allowed set
      val allowedNames = nameConstraints.toSet
      allResults.filter {
        case (_, desc) => allowedNames contains desc.name
      }
    }
  }
}
