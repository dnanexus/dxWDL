package dx.api

import spray.json.{JsArray, JsBoolean, JsNull, JsNumber, JsObject, JsString, JsValue}

case class DxFindApps(dxApi: DxApi, limit: Option[Int] = None) {
  private def parseDescribe(id: String, jsv: JsValue): DxAppDescribe = {
    val name: String = jsv.asJsObject.fields.get("name") match {
      case None                 => throw new Exception("name field missing")
      case Some(JsString(name)) => name
      case other                => throw new Exception(s"malformed name field ${other}")
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

    DxAppDescribe(id, name, created, modified, Some(properties), details, inputSpec, outputSpec)

  }

  private def parseOneResult(jsv: JsValue): DxApp = {
    jsv.asJsObject.getFields("id", "describe") match {
      case Seq(JsString(dxid), desc) =>
        val dxApp = dxApi.app(dxid)
        val dxDesc = parseDescribe(dxApp.id, desc)
        dxApp.cacheDescribe(dxDesc)
        dxApp
      case _ =>
        throw new Exception(s"""|malformed result: expecting {id, describe} fields, got:
                                |${jsv.prettyPrint}
                                |""".stripMargin)
    }
  }

  // Submit a request for a limited number of objects
  private def submitRequest(
      cursor: Option[JsValue],
      published: Option[Boolean],
      propertyConstraints: Vector[String],
      nameConstraints: Vector[String],
      withInputOutputSpec: Boolean,
      idConstraints: Vector[String],
      extraFields: Set[Field.Value]
  ): (Vector[DxApp], Option[JsValue]) = {
    val fields = Set(Field.Name, Field.Properties) ++ extraFields ++ (
        if (withInputOutputSpec) {
          Set(Field.InputSpec, Field.OutputSpec)
        } else {
          Set.empty
        }
    )
    val requestFields = Map(
        "visibility" -> JsString("either"),
        "describe" -> DxObject.requestFields(fields),
        "limit" -> JsNumber(limit.getOrElse(dxApi.limit))
    )
    val cursorField = cursor match {
      case None              => Map.empty
      case Some(cursorValue) => Map("starting" -> cursorValue)
    }
    val publishedField = published match {
      case None    => Map.empty
      case Some(b) => Map("published" -> JsBoolean(b))
    }
    val propertiesField =
      if (propertyConstraints.isEmpty) {
        Map.empty
      } else {
        Map("properties" -> JsObject(propertyConstraints.map { prop =>
          prop -> JsBoolean(true)
        }.toMap))
      }
    val nameField =
      if (nameConstraints.isEmpty) {
        Map.empty
      } else if (nameConstraints.size == 1) {
        // Just one name, no need to use regular expressions
        Map("name" -> JsString(nameConstraints(0)))
      } else {
        // Make a conjunction of all the legal names. For example:
        // ["Nice", "Foo", "Bar"] => ^Nice$|^Foo$|^Bar$
        val orRegexp = nameConstraints.map(x => s"^${x}$$").mkString("|")
        Map("name" -> JsObject("regexp" -> JsString(orRegexp)))
      }
    val idField =
      if (idConstraints.isEmpty) {
        Map.empty
      } else {
        Map("id" -> JsArray(idConstraints.map { x: String =>
          JsString(x)
        }))
      }
    val response = dxApi.findApps(
        requestFields ++ cursorField ++ publishedField ++ propertiesField ++ nameField ++ idField
    )
    val next: Option[JsValue] = response.fields.get("next") match {
      case None                  => None
      case Some(JsNull)          => None
      case Some(other: JsObject) => Some(other)
      case Some(other)           => throw new Exception(s"malformed ${other.prettyPrint}")
    }
    val results = response.fields.get("results") match {
      case None                   => throw new Exception(s"missing results field ${response}")
      case Some(JsArray(results)) => results.map(parseOneResult)
      case Some(other)            => throw new Exception(s"malformed results field ${other.prettyPrint}")
    }
    (results, next)
  }

  /**
    * @param published if true, only published apps are returned;
    *                  if false, only unpublished apps are returned
    * @param withProperties object must have these properties
    * @param nameConstraints object name has to be one of these strings
    * @param withInputOutputSpec should the IO spec be described?
    * @param idConstraints object must have one of these IDs
    * @param extrafields describe these extra fields
    * @return
    */
  def apply(published: Option[Boolean] = None,
            withProperties: Vector[String] = Vector.empty,
            nameConstraints: Vector[String] = Vector.empty,
            withInputOutputSpec: Boolean,
            idConstraints: Vector[String] = Vector.empty,
            extrafields: Set[Field.Value] = Set.empty): Vector[DxApp] = {
    var allResults = Set.empty[DxApp]
    var cursor: Option[JsValue] = None
    do {
      val (results, next) = submitRequest(cursor,
                                          published,
                                          withProperties,
                                          nameConstraints,
                                          withInputOutputSpec,
                                          idConstraints,
                                          extrafields)
      allResults = allResults ++ results
      cursor = next
    } while (cursor.isDefined)

    if (nameConstraints.isEmpty) {
      allResults.toVector
    } else {
      // Ensure the the data objects have names in the allowed set
      val allowedNames = nameConstraints.toSet
      allResults.filter(dxApp => allowedNames.contains(dxApp.describe().name)).toVector
    }
  }
}
