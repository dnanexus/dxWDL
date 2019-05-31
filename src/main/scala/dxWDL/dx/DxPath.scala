/**
  Methods to convert between logical dx paths, and resolved file-ids.

  Logical dx:path           dxURL
  genome_ref:/A/B/C.txt     dx://proj-xxxx:file-yyyy
  viral_ngs:/stats/flu.txt  dx://proj-zzzz:file-vvvv

  Optionally, the logical path can be added to the dxURL. for example:

  genome_ref:/A/B/C.txt     dx://proj-xxxx:file-yyyy::/A/B/C.txt
*/
package dxWDL.dx

import com.dnanexus.{DXAPI, DXDataObject, DXFile, DXProject, DXRecord}
import com.fasterxml.jackson.databind.JsonNode
import scala.collection.mutable.HashMap
import spray.json._

import dxWDL.base.Utils.{DX_URL_PREFIX}
import DxUtils.{jsonNodeOfJsValue, jsValueOfJsonNode}

object DxPath {
    // Lookup cache for projects. This saves
    // repeated searches for projects we already found.
    private val projectDict = HashMap.empty[String, DXProject]

    def lookupProject(projName: String): DXProject = {
        if (projName.startsWith("project-")) {
            // A project ID
            return DXProject.getInstance(projName)
        }
        if (projectDict contains projName) {
            //System.err.println(s"Cached project ${projName}")
            return projectDict(projName)
        }

        // A project name, resolve it
        val req = JsObject("name" -> JsString(projName),
                           "level" -> JsString("VIEW"),
                           "limit" -> JsNumber(2))
        val rep = DXAPI.systemFindProjects(jsonNodeOfJsValue(req),
                                           classOf[JsonNode],
                                           DxUtils.dxEnv)
        val repJs:JsValue = jsValueOfJsonNode(rep)

        val results = repJs.asJsObject.fields.get("results") match {
            case Some(JsArray(x)) => x
            case _ => throw new Exception(
                s"Bad response from systemFindProject API call (${repJs.prettyPrint}), when resolving project ${projName}.")
        }
        if (results.length > 1)
            throw new Exception(s"Found more than one project named ${projName}")
        if (results.length == 0)
            throw new Exception(s"Project ${projName} not found")
        val dxProject = results(0).asJsObject.fields.get("id") match {
            case Some(JsString(id)) => DXProject.getInstance(id)
            case _ => throw new Exception(s"Bad response from SystemFindProject API call ${repJs.prettyPrint}")
        }
        projectDict(projName) = dxProject
        return dxProject
    }


    private def lookupObject(dxProject: DXProject,
                             objName: String): DXDataObject = {
        // If the object is a file-id (or something like it), then
        // shortcircuit the expensive API call call.
        DxUtils.convertToDxObject(objName, Some(dxProject)) match {
            case None => ()
            case Some(dxobj) => return dxobj
        }

        val objPath = s"${DX_URL_PREFIX}${dxProject.getId}:${objName}"
        val found: Map[String, DXDataObject] = DxBulkResolve.apply(Vector(objPath), dxProject)
        if (found.size == 0)
            throw new Exception(s"Object ${objName} not found in path ${objPath}")
        if (found.size > 1)
            throw new Exception(s"Found more than one dx:object named ${objName} in path ${objPath}")
        return found.values.head
    }

    private def lookupDxPath(dxPath: String) : DXDataObject = {
        val components = dxPath.split(":").toList
        components match {
            case Nil =>
                throw new Exception(s"Path ${dxPath} is invalid")
            case List(objName) =>
                val crntProj = DxUtils.dxEnv.getProjectContext()
                lookupObject(crntProj, objName)
            case projName :: tail =>
                val objName = tail.mkString(":")
                val dxProject = lookupProject(projName)
                lookupObject(dxProject, objName)
        }
    }

    // strip the prefix, and the optional suffix
    private def lookupDxURL(buf: String) : DXDataObject = {
        assert(buf.startsWith(DX_URL_PREFIX))
        val s = buf.substring(DX_URL_PREFIX.length)
        val index = s.indexOf("::")
        val s1 =
            if (index == -1) {
                s
            } else {
                s.substring(0, index)
            }
        lookupDxPath(s1)
    }

    // More accurate types
    def lookupDxURLRecord(buf: String) : DXRecord = {
        val dxObj = lookupDxURL(buf)
        if (!dxObj.isInstanceOf[DXRecord])
            throw new Exception(s"Found dx:object of the wrong type ${dxObj}")
        dxObj.asInstanceOf[DXRecord]
    }

    def lookupDxURLFile(buf: String) : DXFile = {
        val dxObj = lookupDxURL(buf)
        if (!dxObj.isInstanceOf[DXFile])
            throw new Exception(s"Found dx:object of the wrong type ${dxObj}")
        dxObj.asInstanceOf[DXFile]
    }

    def dxDataObjectToURL(dxObj: DXDataObject) : String = {
        s"${DX_URL_PREFIX}${dxObj.getId}"
    }
}
