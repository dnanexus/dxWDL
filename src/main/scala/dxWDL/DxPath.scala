package dxWDL

import com.dnanexus.{DXAPI, DXFile, DXProject, DXDataObject, DXSearch}
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
            DXProject.getInstance(projName)
        } else {
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
            dxProject
        }
    }


    def lookupFile(dxProject: Option[DXProject], fileName: String): DXFile = {
        if (fileName.startsWith("file-")) {
            // A file ID
            DXFile.getInstance(fileName)
        } else {
            val fullPath = Paths.get(fileName)
            trace(true, s"lookupFile: ${fullPath.toString}")
            val parent = fullPath.getParent
            var folder = "/"
            if (parent != null) {
                folder = parent.toString
                if (!folder.startsWith("/"))
                    folder = "/" + folder
            }
            val baseName = fullPath.getFileName.toString
            val found:List[DXFile] = dxProject match  {
                case Some(x) =>
                    DXSearch.findDataObjects().nameMatchesExactly(baseName)
                        .inFolder(x, folder).withClassFile().execute().asList().asScala.toList
                case None =>
                    throw new Exception("File lookup requires project context")
            }
            if (found.length == 0)
                throw new Exception(s"File ${fileName} not found in project ${dxProject}")
            if (found.length > 1)
                throw new Exception(s"Found more than one file named ${fileName} in project ${dxProject}")
            found(0)
        }
    }

    def lookupDxPath(dxPath: String) : DXFile = {
        val components = dxPath.split(":/")
        if (components.length > 2) {
            throw new Exception(s"Path ${dxPath} cannot have more than two components")
        } else if (components.length == 2) {
            val projName = components(0)
            val fileName = components(1)
            val dxProject = lookupProject(projName)
            lookupFile(Some(dxProject), fileName)
        } else if (components.length == 1) {
            val fileName = components(0)
            lookupFile(None, fileName)
        } else {
            throw new Exception(s"Path ${dxPath} is invalid")
        }
    }


    def lookupDxURL(buf: String) : DXDataObject = {
        assert(buf.startsWith(DX_URL_PREFIX))
        val s = buf.substring(DX_URL_PREFIX.length)
        lookupDxPath(s)
    }
}
