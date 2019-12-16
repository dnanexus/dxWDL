package dxWDL.dx

import com.dnanexus.{DXAPI, DXContainer, DXDataObject, DXProject}
import com.fasterxml.jackson.databind.JsonNode
import spray.json._

import dxWDL.base.{Verbose}

case class DxFindDataObjects(limit: Option[Int],
                             verbose: Verbose) {

    private def parseParamSpec(jsv: JsValue) : IOParameter = {
        val ioClass: DxIOClass.Value = jsv.asJsObject.fields.get("class") match {
            case None => throw new Exception("class field missing")
            case Some(JsString(ioClass)) => DxIOClass.fromString(ioClass)
            case other => throw new Exception(s"malformed class field ${other}")
        }

        val name: String = jsv.asJsObject.fields.get("name") match {
            case None => throw new Exception("name field missing")
            case Some(JsString(name)) => name
            case other => throw new Exception(s"malformed name field ${other}")
        }

        val optional : Boolean = jsv.asJsObject.fields.get("optional") match {
            case None => false
            case Some(JsBoolean(flag)) => flag
            case other => throw new Exception(s"malformed optional field ${other}")
        }
        IOParameter(name, ioClass, optional)
    }

    private def parseDescribe(jsv: JsValue,
                              dxobj : DXDataObject,
                              dxProject: DXProject) : DxDescribe = {
        val size = jsv.asJsObject.fields.get("size") match {
            case None => None
            case Some(JsNumber(size)) => Some(size.toLong)
            case Some(other) => throw new Exception(s"malformed size field ${other}")
        }
        val name: String = jsv.asJsObject.fields.get("name") match {
            case None => throw new Exception("name field missing")
            case Some(JsString(name)) => name
            case other => throw new Exception(s"malformed name field ${other}")
        }
        val folder = jsv.asJsObject.fields.get("folder") match {
            case None => throw new Exception("folder field missing")
            case Some(JsString(folder)) => folder
            case Some(other) => throw new Exception(s"malformed folder field ${other}")
        }
        val properties : Map[String, String] = jsv.asJsObject.fields.get("properties") match {
            case None => Map.empty
            case Some(JsObject(fields)) =>
                fields.map{
                    case (key, JsString(value)) =>
                        key -> value
                    case (key, other) =>
                        throw new Exception(s"key ${key} has malformed property ${other}")
                }.toMap
            case Some(other) => throw new Exception(s"malformed properties field ${other}")
        }
        val inputSpec : Option[Vector[IOParameter]] = jsv.asJsObject.fields.get("inputSpec") match {
            case None => None
            case Some(JsNull) => None
            case Some(JsArray(iSpecVec)) =>
                Some(iSpecVec.map(parseParamSpec).toVector)
            case Some(other) =>
                throw new Exception(s"malformed inputSpec field ${other}")
        }
        val outputSpec : Option[Vector[IOParameter]]= jsv.asJsObject.fields.get("outputSpec") match {
            case None => None
            case Some(JsNull) => None
            case Some(JsArray(oSpecVec)) =>
                Some(oSpecVec.map(parseParamSpec).toVector)
            case Some(other) =>
                throw new Exception(s"malformed output field ${other}")
        }
        val created : Long = jsv.asJsObject.fields.get("created") match {
            case None => throw new Exception("'created' field is missing")
            case Some(JsNumber(date)) => date.toLong
            case Some(other) => throw new Exception(s"malformed created field ${other}")
        }
        val modified : Long = jsv.asJsObject.fields.get("modified") match {
            case None => throw new Exception("'modified' field is missing")
            case Some(JsNumber(date)) => date.toLong
            case Some(other) => throw new Exception(s"malformed created field ${other}")
        }

        DxDescribe(name, folder, size,
                   dxProject.asInstanceOf[DXContainer], dxobj,
                   created, modified,
                   properties, inputSpec, outputSpec, None)
    }

    private def parseOneResult(jsv : JsValue) : (DXDataObject, DxDescribe) = {
        jsv.asJsObject.getFields("project", "id", "describe") match {
            case Seq(JsString(projectId), JsString(dxid), desc) =>
                val dxProj = DXProject.getInstance(projectId)
                val dxobj = DxUtils.convertToDxObject(dxid, Some(dxProj)).get
                (dxobj, parseDescribe(desc, dxobj, dxProj))
            case _ => throw new Exception(
                s"""|malformed result: expecting {project, id, describe} fields, got:
                    |${jsv.prettyPrint}
                    |""".stripMargin)
        }
    }

    private def buildScope(dxProject : DXProject,
                           folder : Option[String],
                           recurse : Boolean) : JsValue = {
        val part1 = Map("project" -> JsString(dxProject.getId))
        val part2 = folder match {
            case None => Map.empty
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
    private def submitRequest(scope : JsValue,
                              dxProject: DXProject,
                              cursor: Option[JsValue],
                              klass: Option[String],
                              propertyConstraints: Vector[String],
                              nameConstraints : Vector[String],
                              withInputOutputSpec : Boolean) : (Map[DXDataObject, DxDescribe], Option[JsValue]) = {
        val describeFields = Map("name" -> JsBoolean(true),
                                 "folder" -> JsBoolean(true),
                                 "size" -> JsBoolean(true),
                                 "properties" -> JsBoolean(true))
        val ioSpec =
            if (withInputOutputSpec)
                Map("inputSpec" -> JsBoolean(true),
                    "outputSpec" -> JsBoolean(true))
            else
                Map.empty

        val reqFields = Map("visibility" -> JsString("either"),
                            "project" -> JsString(dxProject.getId),
                            "describe" -> JsObject(describeFields ++ ioSpec),
                            "scope" -> scope)
        val limitField = limit match {
            case None => Map.empty
            case Some(lim) =>  Map("limit" -> JsNumber(lim))
        }
        val cursorField = cursor match {
            case None => Map.empty
            case Some(cursorValue) => Map("starting" -> cursorValue)
        }
        val classField = klass match {
            case None => Map.empty
            case Some(k) => Map("class" -> JsString(k))
        }
        val propertiesField =
            if (propertyConstraints.isEmpty) {
                Map.empty
            } else {
                Map("properties" -> JsObject(
                        propertyConstraints.map{
                            prop => prop -> JsBoolean(true)
                        }.toMap))
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
                val orAll = nameConstraints.map{x => s"(${x})"}.mkString("|")
                Map("name" -> JsObject(
                        "regexp" -> JsString(s"[${orAll}]")))
            }

        val request = JsObject(reqFields ++ cursorField ++ limitField
                                   ++ classField ++ propertiesField
                                   ++ namePcreField)

        //Utils.trace(verbose.on, s"submitRequest:\n ${request.prettyPrint}")

        val response = DXAPI.systemFindDataObjects(DxUtils.jsonNodeOfJsValue(request),
                                                   classOf[JsonNode],
                                                   DxUtils.dxEnv)
        val repJs:JsValue = DxUtils.jsValueOfJsonNode(response)

        val next : Option[JsValue] = repJs.asJsObject.fields.get("next") match {
            case None => None
            case Some(JsNull) => None
            case Some(other : JsObject) => Some(other)
            case Some(other) => throw new Exception(s"malformed ${other.prettyPrint}")
        }
        val results : Vector[(DXDataObject, DxDescribe)] =
            repJs.asJsObject.fields.get("results") match {
                case None => throw new Exception(s"missing results field ${repJs}")
                case Some(JsArray(results)) => results.map(parseOneResult)
                case Some(other) => throw new Exception(s"malformed results field ${other.prettyPrint}")
            }

        (results.toMap, next)
    }

    def apply(dxProject : DXProject,
              folder : Option[String],
              recurse: Boolean,
              klassRestriction : Option[String],
              withProperties : Vector[String], // object must have these properties
              nameConstraints : Vector[String], // the object name has to be one of these strings
              withInputOutputSpec : Boolean  // should the IO spec be described?
    ) : Map[DXDataObject, DxDescribe] = {
        klassRestriction.map{ k =>
            if (!(Set("record", "file", "applet", "workflow") contains k))
                throw new Exception("class limitation must be one of {record, file, applet, workflow}")
        }
        val scope = buildScope(dxProject, folder, recurse)

        var allResults = Map.empty[DXDataObject, DxDescribe]
        var cursor : Option[JsValue] = None
        do {
            val (results, next) = submitRequest(scope, dxProject, cursor, klassRestriction,
                                                withProperties,
                                                nameConstraints,
                                                withInputOutputSpec)
            allResults = allResults ++ results
            cursor = next
        } while (cursor != None);

        if (nameConstraints.isEmpty) {
            allResults
        } else {
            // Ensure the the data objects have names in the allowed set
            val allowedNames = nameConstraints.toSet
            allResults.filter{
                case (appletOrWorkflow, desc) => allowedNames contains desc.name
            }
        }
    }
}
