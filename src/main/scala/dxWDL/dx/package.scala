package dxWDL.dx

import com.dnanexus.DXAPI
import com.fasterxml.jackson.databind.JsonNode
import spray.json._

import dxWDL.base.Utils

object DxIOClass extends Enumeration {
    val INT, FLOAT, STRING, BOOLEAN, FILE,
        ARRAY_OF_INTS, ARRAY_OF_FLOATS, ARRAY_OF_STRINGS, ARRAY_OF_BOOLEANS, ARRAY_OF_FILES,
        HASH, OTHER = Value

    def fromString(s : String) : DxIOClass.Value = {
        s match {
            // primitives
            case "int" => INT
            case "float" => FLOAT
            case "string" => STRING
            case "boolean" => BOOLEAN
            case "file" => FILE

            // arrays of primitives
            case "array:int" => ARRAY_OF_INTS
            case "array:float" => ARRAY_OF_FLOATS
            case "array:string" => ARRAY_OF_STRINGS
            case "array:boolean"=> ARRAY_OF_BOOLEANS
            case "array:file" => ARRAY_OF_FILES

            // hash
            case "hash" => HASH

            // we don't deal with anything else
            case other => OTHER
        }
    }
}

case class IOParameter(name: String,
                       ioClass: DxIOClass.Value,
                       optional : Boolean)

// Extra fields for describe
object Field extends Enumeration {
    val Details, Parts, Properties = Value
}

sealed trait DxObjectDescribe {
    val id  : String
    val name : String
    val created : Long
    val modified : Long
    val properties: Option[Map[String, String]]
    val details : Option[JsValue]

    def getCreationDate() : java.util.Date = new java.util.Date(created)
}

sealed trait DxObject {
    val id : String
    def getId : String = id
    def describe(fields : Set[Field.Value]) : DxObjectDescribe
}

object DxObject {
    def parseJsonProperties(props : JsValue) : Map[String, String] = {
        props.asJsObject.fields.map{
            case (k,JsString(v)) => k -> v
            case (_,_) =>
                throw new Exception(s"malform JSON properties ${props}")
        }.toMap
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
    def parseFileParts(jsv: JsValue) : Map[Int, DxFilePart] = {
        //System.out.println(jsv.prettyPrint)
        jsv.asJsObject.fields.map{
            case (partNumber, partDesc) =>
                val dxPart = partDesc.asJsObject.getFields("md5", "size", "state") match {
                    case Seq(JsString(md5), JsNumber(size), JsString(state)) =>
                        DxFilePart(state, size.toLong, md5)
                    case _ => throw new Exception(s"malformed part description ${partDesc.prettyPrint}")
                }
                partNumber.toInt -> dxPart
        }.toMap
    }


    private def checkedGetIoSpec(jsv: JsValue) : IOParameter = {
        jsv.asJsObject.getFields("name", "class", "optional") match {
            case Seq(JsString(name), JsString(klass),JsBoolean(opt)) =>
                val ioClass = DxIOClass.fromString(klass)
                IOParameter(name, ioClass, opt)
            case other =>
                throw new Exception(s"Malformed io spec ${other}")
        }
    }

    def parseIOSpec(specs : Vector[JsValue]) : Vector[IOParameter] = {
        specs.map(ioSpec => checkedGetIoSpec(ioSpec)).toVector
    }

    def maybeSpecifyProject(project : Option[DxProject]) : Map[String, JsValue] = {
        project match {
            case None =>
                // we don't know the project.
                Map.empty
            case Some(p) =>
                // We know the project, this makes the search more efficient.
                Map("project" -> JsString(p.id))
        }
    }

    def requestFields(fields : Set[Field.Value]) : Map[String, JsValue] = {
        val baseFields = Map("id" -> JsTrue,
                             "name" -> JsTrue,
                             "created" -> JsTrue,
                             "modified" -> JsTrue)
        val optFields = fields.map{
            case Field.Details =>
                "details" -> JsTrue
            case Field.Parts =>
                "parts" -> JsTrue
            case Field.Properties =>
                "properties" -> JsTrue
        }.toMap
        optFields ++ baseFields
    }
}

sealed trait DxDataObject extends DxObject

object DxDataObject {
    // We are expecting string like:
    //    record-FgG51b00xF63k86F13pqFv57
    //    file-FV5fqXj0ffPB9bKP986j5kVQ
    //
    def getInstance(id : String,
                    container: Option[DxProject] = None) : DxDataObject = {
        val parts = id.split("-")
        if (parts.length != 2)
            throw new IllegalArgumentException(s"${id} is not of the form class-alphnumeric{24}")
        val klass = parts(0)
        val numLetters = parts(1)
        if (!numLetters.matches("[A-Za-z0-9]{24}"))
            throw new IllegalArgumentException(s"${numLetters} does not match [A-Za-z0-9]{24}")

        klass match {
            case "project" => DxProject(id)
            case "file" => DxFile(id, container)
            case "record" => DxRecord(id, container)
            case "app" => DxApp(id)
            case "applet" => DxApplet(id, container)
            case "workflow" => DxWorkflow(id, container)
            case _ =>
                throw new IllegalArgumentException(s"${id} does not belong to a know class")
        }
    }

    // convenience methods
    def getInstance(id : String,
                    container: DxProject) : DxDataObject = {
        getInstance(id, Some(container))
    }

    def isDataObject(id : String) : Boolean = {
        try {
            val o = getInstance(id, None)
            Utils.ignore(o)
            true
        } catch {
            case e : IllegalArgumentException =>
                false
        }
    }
}

case class DxRecordDescribe(project : String,
                            id  : String,
                            name : String,
                            folder: String,
                            created : Long,
                            modified : Long,
                            properties: Option[Map[String, String]],
                            details : Option[JsValue]) extends DxObjectDescribe

case class DxRecord(id : String,
                    project : Option[DxProject]) extends DxDataObject {
    def describe(fields : Set[Field.Value] = Set.empty) : DxRecordDescribe = {
        val request = JsObject("fields" -> JsObject(DxObject.requestFields(fields)))
        val response = DXAPI.recordDescribe(id,
                                            DxUtils.jsonNodeOfJsValue(request),
                                            classOf[JsonNode],
                                            DxUtils.dxEnv)
        val descJs:JsValue = DxUtils.jsValueOfJsonNode(response)
        val desc = descJs.asJsObject.getFields("project", "id", "name", "folder",
                                               "created", "modified") match {
            case Seq(JsString(project), JsString(id),
                     JsString(name), JsString(folder), JsNumber(created), JsNumber(modified)) =>
                DxRecordDescribe(project,
                                 id,
                                 name,
                                 folder,
                                 created.toLong,
                                 modified.toLong,
                                 None,
                                 None)
        }

        val details = descJs.asJsObject.fields.get("details")
        val props = descJs.asJsObject.fields.get("properties").map(DxObject.parseJsonProperties)
        desc.copy(details = details, properties = props)
    }
}

object DxRecord {
    def getInstance(id : String) : DxRecord = {
         DxDataObject.getInstance(id) match {
             case r : DxRecord => r
             case _ =>
                 throw new IllegalArgumentException(s"${id} isn't a record")
         }
    }
}


case class DxFilePart(state: String,
                      size: Long,
                      md5: String)

case class DxFileDescribe(project : String,
                          id : String,
                          name : String,
                          folder: String,
                          created : Long,
                          modified : Long,
                          size : Long,
                          properties : Option[Map[String, String]],
                          details : Option[JsValue],
                          parts : Option[Map[Int, DxFilePart]]) extends DxObjectDescribe

case class DxFile(id : String,
                  project : Option[DxProject]) extends DxDataObject {

    def describe(fields : Set[Field.Value] = Set.empty) : DxFileDescribe = {
        val projSpec = DxObject.maybeSpecifyProject(project)
        val baseFields = DxObject.requestFields(fields)
        val allFields = baseFields ++ Map("project" -> JsTrue,
                                          "folder"  -> JsTrue,
                                          "size" -> JsTrue)
        val request = JsObject(projSpec + ("fields" -> JsObject(allFields)))
        val response = DXAPI.fileDescribe(id,
                                          DxUtils.jsonNodeOfJsValue(request),
                                          classOf[JsonNode],
                                          DxUtils.dxEnv)
        val descJs:JsValue = DxUtils.jsValueOfJsonNode(response)
        val desc = descJs.asJsObject.getFields("project", "id", "name", "folder",
                                               "created", "modified", "size") match {
            case Seq(JsString(project), JsString(id),
                     JsString(name), JsString(folder), JsNumber(created), JsNumber(modified), JsNumber(size)) =>
                DxFileDescribe(project,
                               id,
                               name,
                               folder,
                               created.toLong,
                               modified.toLong,
                               size.toLong,
                               None,
                               None,
                               None)
            case _ =>
                throw new Exception(s"Malformed JSON ${descJs}")
        }

        val details = descJs.asJsObject.fields.get("details")
        val props = descJs.asJsObject.fields.get("properties").map(DxObject.parseJsonProperties)
        val parts = descJs.asJsObject.fields.get("parts").map(DxObject.parseFileParts)
        desc.copy(details = details, properties = props, parts = parts)
    }

    def getLinkAsJson : JsValue = {
        project match {
            case None =>
                JsObject("$dnanexus_link" -> JsString(id))
            case Some(p) =>
                JsObject( "$dnanexus_link" -> JsObject(
                             "project" -> JsString(p.id),
                             "id" -> JsString(id)
                         ))
        }
    }
}

object DxFile {
    def getInstance(id : String) : DxFile = {
        DxDataObject.getInstance(id) match {
             case f : DxFile => f
             case _ =>
                throw new IllegalArgumentException(s"${id} isn't a file")
        }
    }

    def getInstance(id : String, project : DxProject) : DxFile = {
        DxDataObject.getInstance(id, Some(project)) match {
             case f : DxFile => f
             case _ =>
                throw new IllegalArgumentException(s"${id} isn't a file")
        }
    }
}

case class FolderContents(dataObjects: Vector[DxDataObject],
                          subFolders : Vector[String])

case class DxProjectDescribe(project : String,
                             id : String,
                             name : String,
                             folder: String,
                             created : Long,
                             modified : Long,
                             properties : Option[Map[String, String]],
                             details : Option[JsValue]) extends DxObjectDescribe

// A project is a subtype of a container
case class DxProject(id: String) extends DxDataObject {
    def describe(fields : Set[Field.Value] = Set.empty) : DxProjectDescribe = {
        val request = JsObject("fields" -> JsObject(DxObject.requestFields(fields)))
        val response =
            if (id.startsWith("project-"))
                DXAPI.projectDescribe(id,
                                      DxUtils.jsonNodeOfJsValue(request),
                                      classOf[JsonNode],
                                      DxUtils.dxEnv)
            else
                DXAPI.containerDescribe(id,
                                        DxUtils.jsonNodeOfJsValue(request),
                                        classOf[JsonNode],
                                        DxUtils.dxEnv)
        val descJs:JsValue = DxUtils.jsValueOfJsonNode(response)
        val desc = descJs.asJsObject.getFields("name", "created", "modified") match {
            case Seq(JsString(name), JsNumber(created), JsNumber(modified)) =>
                DxProjectDescribe(id,
                                  id,
                                  name,
                                  "/",
                                  created.toLong,
                                  modified.toLong,
                                  None,
                                  None)
            case _ =>
                throw new Exception(s"malformed JSON ${descJs}")
        }
        val details = descJs.asJsObject.fields.get("details")
        val props = descJs.asJsObject.fields.get("properties").map(DxObject.parseJsonProperties)
        desc.copy(details = details, properties = props)
    }

    def listFolder(path : String) : FolderContents = {
        val request = JsObject("folder" -> JsString(path))
        val response = id match {
            case _ if (id.startsWith("container-")) =>
                DXAPI.containerListFolder(id,
                                          DxUtils.jsonNodeOfJsValue(request),
                                          classOf[JsonNode],
                                          DxUtils.dxEnv)
            case _  if (id.startsWith("project-")) =>
                DXAPI.projectListFolder(id,
                                        DxUtils.jsonNodeOfJsValue(request),
                                        classOf[JsonNode],
                                        DxUtils.dxEnv)
            case _ =>
                throw new Exception(s"invalid project id ${id}" )
        }
        val repJs:JsValue = DxUtils.jsValueOfJsonNode(response)

        // extract object ids
        val objsJs = repJs.asJsObject.fields("objects") match {
            case JsArray(a) => a
            case _ => throw new Exception("not an array")
        }
        val objs = objsJs.map{
            case JsObject(fields) =>
                fields.get("id") match {
                    case Some(JsString(id)) => DxDataObject.getInstance(id, Some(this))
                    case other =>
                        throw new Exception(s"malformed json reply ${other}")
                }
            case other =>
                throw new Exception(s"malformed json reply ${other}")
        }.toVector

        // extract sub folders
	val subdirsJs = repJs.asJsObject.fields("folders") match {
            case JsArray(a) => a
            case _ => throw new Exception("not an array")
        }
        val subdirs = subdirsJs.map{
            case JsString(x) => x
            case other =>
                throw new Exception(s"malformed json reply ${other}")
        }.toVector

        FolderContents(objs, subdirs)
    }

    def newFolder(folderPath : String, parents : Boolean) : Unit = {
        val request = JsObject("project" -> JsString(id),
                               "folder" -> JsString(folderPath),
                               "parents" -> (if (parents) JsTrue else JsFalse))
        val response = id match {
            case _ if (id.startsWith("container-")) =>
                DXAPI.containerNewFolder(id,
                                         DxUtils.jsonNodeOfJsValue(request),
                                         classOf[JsonNode],
                                         DxUtils.dxEnv)
            case _  if (id.startsWith("project-")) =>
                DXAPI.projectNewFolder(id,
                                       DxUtils.jsonNodeOfJsValue(request),
                                       classOf[JsonNode],
                                       DxUtils.dxEnv)
            case _ =>
                throw new Exception(s"invalid project id ${id}" )
        }
        val repJs:JsValue = DxUtils.jsValueOfJsonNode(response)
        Utils.ignore(repJs)
    }

    def moveObjects(objs: Vector[DxDataObject], destinationFolder : String) : Unit = {
        val request = JsObject("objects" -> JsArray(objs.map{ x => JsString(x.id) }.toVector),
                               "folders" -> JsArray(Vector.empty[JsString]),
                               "destination" -> JsString(destinationFolder))
        val response = id match {
            case _ if (id.startsWith("container-")) =>
                DXAPI.containerMove(id,
                                    DxUtils.jsonNodeOfJsValue(request),
                                    classOf[JsonNode],
                                    DxUtils.dxEnv)
            case _  if (id.startsWith("project-")) =>
                DXAPI.projectMove(id,
                                  DxUtils.jsonNodeOfJsValue(request),
                                  classOf[JsonNode],
                                  DxUtils.dxEnv)
            case _ =>
                throw new Exception(s"invalid project id ${id}" )
        }
        val repJs:JsValue = DxUtils.jsValueOfJsonNode(response)
        Utils.ignore(repJs)
    }

    def removeObjects(objs : Vector[DxDataObject]) : Unit = {
        val request = JsObject("objects" -> JsArray(objs.map{ x => JsString(x.id) }.toVector),
                               "force" -> JsFalse)
        val response = id match {
            case _ if (id.startsWith("container-")) =>
                DXAPI.containerRemoveObjects(id,
                                             DxUtils.jsonNodeOfJsValue(request),
                                             classOf[JsonNode],
                                             DxUtils.dxEnv)
            case _  if (id.startsWith("project-")) =>
                DXAPI.projectRemoveObjects(id,
                                           DxUtils.jsonNodeOfJsValue(request),
                                           classOf[JsonNode],
                                           DxUtils.dxEnv)
            case _ =>
                throw new Exception(s"invalid project id ${id}" )
        }
        val repJs:JsValue = DxUtils.jsValueOfJsonNode(response)
        Utils.ignore(repJs)
    }
}

object DxProject {
    def getInstance(id : String) : DxProject = {
        DxDataObject.getInstance(id) match {
             case p : DxProject => p
             case _ =>
                throw new IllegalArgumentException(s"${id} isn't a project")
        }
    }
}

// Objects that can be run on the platform
sealed trait DxExecutable extends DxDataObject

case class DxAppletDescribe(project : String,
                            id : String,
                            name : String,
                            folder: String,
                            created : Long,
                            modified : Long,
                            properties : Option[Map[String, String]],
                            details : Option[JsValue],
                            inputSpec : Option[Vector[IOParameter]],
                            outputSpec : Option[Vector[IOParameter]]) extends DxObjectDescribe

case class DxApplet(id : String,
                    project : Option[DxProject]) extends DxExecutable {
    def describe(fields : Set[Field.Value] = Set.empty) : DxAppletDescribe = {
        val projSpec = DxObject.maybeSpecifyProject(project)
        val baseFields = DxObject.requestFields(fields)
        val allFields = baseFields ++ Map("project" -> JsTrue,
                                          "folder"  -> JsTrue,
                                          "size" -> JsTrue,
                                          "inputSpec" -> JsTrue,
                                          "outputSpec" -> JsTrue)
        val request = JsObject(projSpec + ("fields" -> JsObject(allFields)))
        val response = DXAPI.appletDescribe(id,
                                            DxUtils.jsonNodeOfJsValue(request),
                                            classOf[JsonNode],
                                            DxUtils.dxEnv)
        val descJs:JsValue = DxUtils.jsValueOfJsonNode(response)
        val desc = descJs.asJsObject.getFields("project", "id", "name", "folder",
                                               "created", "modified",
                                               "inputSpec", "outputSpec") match {
            case Seq(JsString(project), JsString(id),
                     JsString(name), JsString(folder), JsNumber(created),
                     JsNumber(modified), JsNumber(size),
                     JsArray(inputSpec), JsArray(outputSpec)) =>
                DxAppletDescribe(project,
                                 id,
                                 name,
                                 folder,
                                 created.toLong,
                                 modified.toLong,
                                 None,
                                 None,
                                 Some(DxObject.parseIOSpec(inputSpec.toVector)),
                                 Some(DxObject.parseIOSpec(outputSpec.toVector)))
            case _ =>
                throw new Exception(s"Malformed JSON ${descJs}")
        }

        val details = descJs.asJsObject.fields.get("details")
        val props = descJs.asJsObject.fields.get("properties").map(DxObject.parseJsonProperties)
        desc.copy(details = details, properties = props)
    }
}

object DxApplet {
    def getInstance(id : String) : DxApplet = {
        DxDataObject.getInstance(id) match {
             case a : DxApplet => a
             case _ =>
                throw new IllegalArgumentException(s"${id} isn't an applet")
        }
    }
}

case class DxAppDescribe(id  : String,
                         name : String,
                         created : Long,
                         modified : Long,
                         properties: Option[Map[String, String]],
                         details : Option[JsValue],
                         inputSpec : Option[Vector[IOParameter]],
                         outputSpec : Option[Vector[IOParameter]]) extends DxObjectDescribe

case class DxApp(id : String) extends DxExecutable {
    def describe(fields : Set[Field.Value] = Set.empty) : DxAppDescribe = {
        val baseFields = DxObject.requestFields(fields)
        val allFields = baseFields ++ Map("inputSpec" -> JsTrue,
                                          "outputSpec" -> JsTrue)
        val request = JsObject("fields" -> JsObject(allFields))
        val response = DXAPI.appDescribe(id,
                                         DxUtils.jsonNodeOfJsValue(request),
                                         classOf[JsonNode],
                                         DxUtils.dxEnv)
        val descJs:JsValue = DxUtils.jsValueOfJsonNode(response)
        val desc = descJs.asJsObject.getFields("id", "name",
                                               "created", "modified",
                                               "inputSpec", "outputSpec") match {
            case Seq(JsString(id), JsString(name),
                     JsNumber(created), JsNumber(modified),
                     JsArray(inputSpec), JsArray(outputSpec)) =>
                DxAppDescribe(id,
                              name,
                              created.toLong,
                              modified.toLong,
                              None,
                              None,
                              Some(DxObject.parseIOSpec(inputSpec.toVector)),
                              Some(DxObject.parseIOSpec(outputSpec.toVector)))
            case _ =>
                throw new Exception(s"Malformed JSON ${descJs}")
        }

        val details = descJs.asJsObject.fields.get("details")
        val props = descJs.asJsObject.fields.get("properties").map(DxObject.parseJsonProperties)
        desc.copy(details = details, properties = props)
    }
}

object DxApp {
    def getInstance(id : String) : DxApp = {
        DxDataObject.getInstance(id) match {
             case a : DxApp => a
             case _ =>
                throw new IllegalArgumentException(s"${id} isn't an app")
        }
    }
}

case class DxWorkflowDescribe(project : String,
                              id  : String,
                              name : String,
                              folder : String,
                              created : Long,
                              modified : Long,
                              properties: Option[Map[String, String]],
                              details : Option[JsValue],
                              inputSpec : Option[Vector[IOParameter]],
                              outputSpec : Option[Vector[IOParameter]]) extends DxObjectDescribe

case class DxWorkflow(id : String,
                      project : Option[DxProject]) extends DxExecutable {
    def describe(fields : Set[Field.Value] = Set.empty) : DxWorkflowDescribe = {
        val projSpec = DxObject.maybeSpecifyProject(project)
        val baseFields = DxObject.requestFields(fields)
        val allFields = baseFields ++ Map("project" -> JsTrue,
                                          "folder"  -> JsTrue,
                                          "size" -> JsTrue,
                                          "inputSpec" -> JsTrue,
                                          "outputSpec" -> JsTrue)
        val request = JsObject(projSpec + ("fields" -> JsObject(allFields)))
        val response = DXAPI.workflowDescribe(id,
                                              DxUtils.jsonNodeOfJsValue(request),
                                              classOf[JsonNode],
                                              DxUtils.dxEnv)
        val descJs:JsValue = DxUtils.jsValueOfJsonNode(response)
        val desc = descJs.asJsObject.getFields("project", "id", "name", "folder",
                                               "created", "modified",
                                               "inputSpec", "outputSpec") match {
            case Seq(JsString(project), JsString(id),
                     JsString(name), JsString(folder), JsNumber(created),
                     JsNumber(modified), JsNumber(size),
                     JsArray(inputSpec), JsArray(outputSpec)) =>
                DxWorkflowDescribe(project,
                                   id,
                                   name,
                                   folder,
                                   created.toLong,
                                   modified.toLong,
                                   None,
                                   None,
                                   Some(DxObject.parseIOSpec(inputSpec.toVector)),
                                   Some(DxObject.parseIOSpec(outputSpec.toVector)))
            case _ =>
                throw new Exception(s"Malformed JSON ${descJs}")
        }

        val details = descJs.asJsObject.fields.get("details")
        val props = descJs.asJsObject.fields.get("properties").map(DxObject.parseJsonProperties)
        desc.copy(details = details, properties = props)
    }

    def close() : Unit = {
        DXAPI.workflowClose(id,
                            classOf[JsonNode],
                            DxUtils.dxEnv)
    }

    def newRun(input : JsValue, name : String) : DxAnalysis = {
        val request = JsObject(
            "name" -> JsString(name),
            "input" -> input.asJsObject)
        val response = DXAPI.workflowRun(id,
                                         DxUtils.jsonNodeOfJsValue(request),
                                         classOf[JsonNode],
                                         DxUtils.dxEnv)
        val repJs:JsValue = DxUtils.jsValueOfJsonNode(response)
	repJs.asJsObject.fields.get("id") match {
            case None =>
                throw new Exception("id not returned in response")
            case Some(JsString(x)) =>
                DxAnalysis.getInstance(x)
            case Some(other) =>
                throw new Exception(s"malformed json response ${other}")
        }
    }
}

object DxWorkflow {
    def getInstance(id : String) : DxWorkflow = {
        DxDataObject.getInstance(id) match {
             case wf : DxWorkflow => wf
             case _ =>
                throw new IllegalArgumentException(s"${id} isn't a workflow")
        }
    }
}

// Actual executions on the platform. There are jobs and analyses
sealed trait DxExecution extends DxObject


case class DxAnalysisDescribe(project : String,
                              id  : String,
                              name : String,
                              folder : String,
                              created : Long,
                              modified : Long,
                              properties: Option[Map[String, String]],
                              details : Option[JsValue]) extends DxObjectDescribe

case class DxAnalysis(id : String,
                      project : Option[DxProject]) extends DxObject with DxExecution {
    def describe(fields : Set[Field.Value] = Set.empty) : DxAnalysisDescribe = {
        val projSpec = DxObject.maybeSpecifyProject(project)
        val baseFields = DxObject.requestFields(fields)
        val allFields = baseFields ++ Map("project" -> JsTrue,
                                          "folder"  -> JsTrue)
        val request = JsObject(projSpec + ("fields" -> JsObject(allFields)))
        val response = DXAPI.analysisDescribe(id,
                                              DxUtils.jsonNodeOfJsValue(request),
                                              classOf[JsonNode],
                                              DxUtils.dxEnv)
        val descJs:JsValue = DxUtils.jsValueOfJsonNode(response)
        val desc = descJs.asJsObject.getFields("project", "id", "name", "folder",
                                               "created", "modified") match {
            case Seq(JsString(project), JsString(id),
                     JsString(name), JsString(folder), JsNumber(created),
                     JsNumber(modified), JsNumber(size)) =>
                DxWorkflowDescribe(project,
                                   id,
                                   name,
                                   folder,
                                   created.toLong,
                                   modified.toLong,
                                   None,
                                   None)
            case _ =>
                throw new Exception(s"Malformed JSON ${descJs}")
        }

        val details = descJs.asJsObject.fields.get("details")
        val props = descJs.asJsObject.fields.get("properties").map(DxObject.parseJsonProperties)
        desc.copy(details = details, properties = props)
    }

    def setProperties(props: Map[String, String]) : Unit = {
        val request = JsObject(
            "properties" -> JsObject(props.map{ case (k,v) =>
                                         k -> JsString(v)
                                     })
        )
        val response = DXAPI.workflowRun(id,
                                         DxUtils.jsonNodeOfJsValue(request),
                                         classOf[JsonNode],
                                         DxUtils.dxEnv)
        val repJs:JsValue = DxUtils.jsValueOfJsonNode(response)
        Utils.ignore(repJs)
    }
}

object DxAnalysis {
    def getInstance(id : String) : DxAnalysis = {
        if (id.startsWith("analysis-"))
            return DxAnalysis(id)
        throw new IllegalArgumentException(s"${id} isn't an analysis")
    }
}


case class DxJobDescribe(project : String,
                         id  : String,
                         name : String,
                         created : Long,
                         modified : Long,
                         properties: Option[Map[String, String]],
                         details : Option[JsValue],
                         applet : DxApplet,
                         parentJob : Option[DxJob],
                         analysis : Option[DxAnalysis]) extends DxObjectDescribe

case class DxJob(id : String) extends DxObject with DxExecution {
    def describe(fields : Set[Field.Value] = Set.empty) : DxJobDescribe = ???
}

object DxJob {
    def getInstance(id : String) : DxJob = {
        if (id.startsWith("job-"))
            return DxJob(id)
        throw new IllegalArgumentException(s"${id} isn't a job")
    }
}


// A stand in for the DxWorkflow.Stage inner class (we don't have a constructor for it)
case class DxWorkflowStage(id: String) {
    def getId() = id

    def getInputReference(inputName:String) : JsValue = {
        JsObject("$dnanexus_link" -> JsObject(
                     "stage" -> JsString(id),
                     "inputField" -> JsString(inputName)))
    }
    def getOutputReference(outputName:String) : JsValue = {
        JsObject("$dnanexus_link" -> JsObject(
                     "stage" -> JsString(id),
                     "outputField" -> JsString(outputName)))
    }
}
