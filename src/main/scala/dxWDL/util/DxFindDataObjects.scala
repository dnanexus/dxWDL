package dxWDL.util

import com.dnanexus.{DXAPI, DXDataObject, DXProject}
import com.fasterxml.jackson.databind.JsonNode
import spray.json._


object DxFindDataObjects {
    private def parseDescribe(jsv: JsValue,
                              dxobj : DXDataObject,
                              dxProject: DXProject) : DxDescribe = {
        jsv.asJsObject.getFields("name", "folder", "size", "properties", "inputSpec", "outputSpec") match {
            case Seq(JsString(name), JsString(folder), JsNumber(size),
                     properties, inputSpec, outputSpec) =>
                System.out.println(s"properties=${properties}")
                System.out.println(s"inputSpec=${inputSpec}")
                System.out.println(s"outputSpec=${outputSpec}")
                DxDescribe(name, folder, size.toLong, dxProject, dxobj,
                           Map.empty, // properties
                           Vector.empty, // inputSpec
                           Vector.empty // outputSpec
                )
            case _ =>
                throw new Exception(s"malformed describe output ${jsv.prettyPrint}")
        }
    }

    private def parseOneResult(jsv : JsValue) : (DXDataObject, DxDescribe) = {
        jsv.asJsObject.getFields("project", "id", "describe") match {
            case Seq(JsString(projectId), JsString(dxid), desc) =>
                val dxProj = DXProject.getInstance(projectId)
                val dxobj = DxDescribe.convertToDxObject(dxid).get
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
                              limit: Int,
                              cursor: Option[JsValue]) : (Map[DXDataObject, DxDescribe], Option[JsValue]) = {
        val reqFields = Map("visibility" -> JsString("either"),
                          "project" -> JsString(dxProject.getId),
                          "describe" -> JsObject("name" -> JsBoolean(true),
                                                 "folder" -> JsBoolean(true),
                                                 "size" -> JsBoolean(true),
                                                 "properties" -> JsBoolean(true),
                                                 "inputSpec" -> JsBoolean(true),
                                                 "outputSpec" -> JsBoolean(true)),
                          "scope" -> scope,
                          "limit" -> JsNumber(limit))
        val cursorField = cursor match {
            case None => Map.empty
            case Some(cursorValue) => Map("starting" -> cursorValue)
        }
        val request = JsObject(reqFields ++ cursorField)
        val response = DXAPI.systemFindDataObjects(Utils.jsonNodeOfJsValue(request),
                                                   classOf[JsonNode],
                                                   Utils.dxEnv)
        val repJs:JsValue = Utils.jsValueOfJsonNode(response)

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
              recurse: Boolean) : Map[DXDataObject, DxDescribe] = {
        val scope = buildScope(dxProject, folder, recurse)
        val limit = 10

        var allResults = Map.empty[DXDataObject, DxDescribe]
        var cursor : Option[JsValue] = None
        do {
            val (results, next) = submitRequest(scope, dxProject, limit, cursor)
            allResults = allResults ++ results
            cursor = next
        } while (cursor != None);
        allResults
    }
}

/*

DxObjectDirectory
        val dxObjectsInFolder: List[DXDataObject] = DXSearch.findDataObjects()
            .inFolder(dxProject, folder)
            .withVisibility(DXSearch.VisibilityQuery.EITHER)
            .includeDescribeOutput(DXDataObject.DescribeOptions.get().withProperties())
            .execute().asList().asScala.toList

        val dxAppletsInProject: List[DXDataObject] = DXSearch.findDataObjects()
            .inProject(dxProject)
            .withVisibility(DXSearch.VisibilityQuery.EITHER)
            .withProperty(CHECKSUM_PROP)
            .withClassApplet
            .includeDescribeOutput(DXDataObject.DescribeOptions.get().withProperties())
            .execute().asList().asScala.toList

WorkflowOutputReorg
    def bulkGetFilenames(files: Seq[DXFile], dxProject: DXProject) : Vector[String] = {
        val info:List[DXDataObject] = DXSearch.findDataObjects()
            .withIdsIn(files.asJava)
            .inProject(dxProject)
            .includeDescribeOutput()
            .execute().asList().asScala.toList
        info.map(_.getCachedDescribe().getName()).toVector
    }

 */
