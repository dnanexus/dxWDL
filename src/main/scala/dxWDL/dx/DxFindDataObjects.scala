package dxWDL.dx

import com.dnanexus.{DXAPI, DXDataObject, DXProject, IOClass}
import com.fasterxml.jackson.databind.JsonNode
import spray.json._

import dxWDL.base.Verbose

case class DxFindDataObjects(limit: Option[Int],
                             verbose: Verbose) {

    private def parseParamSpec(jsv: JsValue) : IOParameter = {
        val ioClass: IOClass = jsv.asJsObject.fields.get("class") match {
            case None => throw new Exception("class field missing")
            case Some(JsString(ioClass)) => IOClass.create(ioClass)
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
        val creationDate : java.util.Date = jsv.asJsObject.fields.get("created") match {
            case None => throw new Exception("'created' field is missing")
            case Some(JsNumber(date)) => new java.util.Date(date.toLong)
            case Some(other) => throw new Exception(s"malformed created field ${other}")
        }
        DxDescribe(name, folder, size,
                   Some(dxProject), dxobj, creationDate,
                   properties, inputSpec, outputSpec)
    }

    private def parseOneResult(jsv : JsValue) : (DXDataObject, DxDescribe) = {
        jsv.asJsObject.getFields("project", "id", "describe") match {
            case Seq(JsString(projectId), JsString(dxid), desc) =>
                val dxProj = DXProject.getInstance(projectId)
                val dxobj = DxUtils.convertToDxObject(dxid).get
                val dxobjWithProj = DXDataObject.getInstance(dxobj.getId, dxProj)
                (dxobjWithProj, parseDescribe(desc, dxobj, dxProj))
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
                Map.empty
        JsObject(part1 ++ part2 ++ part3)
    }

    // Submit a request for a limited number of objects
    private def submitRequest(scope : JsValue,
                              dxProject: DXProject,
                              cursor: Option[JsValue],
                              klass: Option[String]) : (Map[DXDataObject, DxDescribe], Option[JsValue]) = {
        val reqFields = Map("visibility" -> JsString("either"),
                            "project" -> JsString(dxProject.getId),
                            "describe" -> JsObject("name" -> JsBoolean(true),
                                                   "folder" -> JsBoolean(true),
                                                   "size" -> JsBoolean(true),
                                                   "properties" -> JsBoolean(true),
                                                   "inputSpec" -> JsBoolean(true),
                                                   "outputSpec" -> JsBoolean(true)),
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
        val request = JsObject(reqFields ++ cursorField ++ limitField ++ classField)
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
              klassRestriction : Option[String]) : Map[DXDataObject, DxDescribe] = {
        klassRestriction.map{ k =>
            if (!(Set("record", "file", "applet", "workflow") contains k))
                throw new Exception("class limitation must be one of {record, file, applet, workflow}")
        }
        val scope = buildScope(dxProject, folder, recurse)

        var allResults = Map.empty[DXDataObject, DxDescribe]
        var cursor : Option[JsValue] = None
        do {
            val (results, next) = submitRequest(scope, dxProject, cursor, klassRestriction)
            allResults = allResults ++ results
            cursor = next
        } while (cursor != None);
        allResults
    }
}
