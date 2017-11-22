package dxWDL

import com.dnanexus.{DXAPI, DXApplet, DXDataObject, DXFile, DXProject, DXRecord, DXSearch, DXWorkflow}
import com.fasterxml.jackson.databind.JsonNode
import java.nio.file.Paths
import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import spray.json._
import Utils.{DX_URL_PREFIX, jsonNodeOfJsValue, jsValueOfJsonNode, trace}

object DxPath {
    // Lookup cache for projects. This saves
    // repeated searches for projects we already found.
    val projectDict = HashMap.empty[String, DXProject]

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
                           "limit" -> JsNumber(2))
        val rep = DXAPI.systemFindProjects(jsonNodeOfJsValue(req),
                                           classOf[JsonNode])
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


    private def lookupObject(dxProject: Option[DXProject],
                             objName: String): DXDataObject = {
        if (objName.startsWith("applet-")) {
            return DXApplet.getInstance(objName)
        }
        if (objName.startsWith("file-")) {
            return DXFile.getInstance(objName)
        }
        if (objName.startsWith("record-")) {
            return DXRecord.getInstance(objName)
        }
        if (objName.startsWith("workflow-")) {
            return DXWorkflow.getInstance(objName)
        }
        if (objName.startsWith("gtable-")) {
            throw new Exception(s"gtables not supported proj=${dxProject} obj=${objName}")
        }

        val fullPath = Paths.get(objName)
        trace(true, s"lookupObject: ${fullPath.toString}")
        val parent = fullPath.getParent
        var folder = "/"
        if (parent != null) {
            folder = parent.toString
            if (!folder.startsWith("/"))
                folder = "/" + folder
        }
        val baseName = fullPath.getFileName.toString
        val proj = dxProject match  {
            case Some(p) => p
            case None => throw new Exception("Object lookup requires project context")
        }
        val found:List[DXDataObject] =
            DXSearch.findDataObjects().nameMatchesExactly(baseName)
                .inFolder(proj, folder).execute().asList().asScala.toList
        if (found.length == 0)
            throw new Exception(s"Object ${objName} not found in path ${proj.getId}:${folder}")
        if (found.length > 1)
            throw new Exception(s"Found more than one dx:object named ${objName} in path ${proj.getId}:${folder}")
        return found(0)
    }

    def lookupFile(dxProject: Option[DXProject], fileName: String): DXFile = {
        val dxObj:DXDataObject = lookupObject(dxProject, fileName)
        if (!dxObj.isInstanceOf[DXFile])
            throw new Exception(s"Found dx:object of the wrong type ${dxObj}")
        dxObj.asInstanceOf[DXFile]
    }

    private def lookupDxPath(dxPath: String) : DXDataObject = {
        val components = dxPath.split(":/")
        if (components.length > 2) {
            throw new Exception(s"Path ${dxPath} cannot have more than two components")
        } else if (components.length == 2) {
            val projName = components(0)
            val objName = components(1)
            val dxProject = lookupProject(projName)
            lookupObject(Some(dxProject), objName)
        } else if (components.length == 1) {
            val objName = components(0)
            lookupObject(None, objName)
        } else {
            throw new Exception(s"Path ${dxPath} is invalid")
        }
    }


    private def lookupDxURL(buf: String) : DXDataObject = {
        assert(buf.startsWith(DX_URL_PREFIX))
        val s = buf.substring(DX_URL_PREFIX.length)
        lookupDxPath(s)
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

    def dxFileToPath(dxFile: DXFile) : String = {
        val fid = dxFile.getId
        val projId = dxFile.getProject.getId
        s"${DX_URL_PREFIX}${projId}:${fid}"
    }
}
