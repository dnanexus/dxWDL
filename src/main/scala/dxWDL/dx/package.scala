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

case class DxFilePart(state: String,
                      size: Long,
                      md5: String)

// This is similar to DXDataObject.Describe
case class DxDescribe(name : String,
                      folder: String,
                      size : Option[Long],
                      container: DxProject, // a project or a container
                      dxobj : DxObject,
                      created : Long,
                      modified : Long,
                      properties: Map[String, String],
                      inputSpec : Option[Vector[IOParameter]],
                      outputSpec : Option[Vector[IOParameter]],
                      parts : Option[Map[Int, DxFilePart]],
                      details : Option[JsValue],
                      applet : Option[DxApplet],
                      parentJob : Option[DxJob],
                      analysis : Option[DxAnalysis]) {
    def getCreationDate() : java.util.Date = new java.util.Date(created)
    def getApplet() : DxApplet = applet.get
    def getParentJob() : DxJob = parentJob.get
    def getAnalysis() : DxAnalysis = analysis.get
}

// Extra fields for describe
object Field extends Enumeration {
    val Details, Parts, Applet, ParentJob, Analysis = Value
}

sealed trait DxObject {
    val id : String
    def getId() : String = id

    def describe() : DxDescribe = {
        val results = DxBulkDescribe.apply(Vector(this), Vector.empty)
        assert(results.size == 1)
        results.values.head
    }

    def describe(field: Field.Value) : DxDescribe = {
        val results = DxBulkDescribe.apply(Vector(this), Vector(field))
        assert(results.size == 1)
        results.values.head
    }

    def describe(fields: Vector[Field.Value]) : DxDescribe = {
        val results = DxBulkDescribe.apply(Vector(this), fields)
        assert(results.size == 1)
        results.values.head
    }
}

sealed trait DxDataObject extends DxObject

object DxDataObject {
    // We are expecting string like:
    //    record-FgG51b00xF63k86F13pqFv57
    //    file-FV5fqXj0ffPB9bKP986j5kVQ
    //
    def getInstance(id : String,
                    container: Option[DxProject]) : DxDataObject = {
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

    def getInstance(id : String) : DxDataObject = {
        getInstance(id, None)
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

case class DxRecord(id : String,
                    project : Option[DxProject]) extends DxDataObject
object DxRecord {
    def getInstance(id : String) : DxRecord = {
         DxDataObject.getInstance(id) match {
             case r : DxRecord => r
             case _ =>
                 throw new IllegalArgumentException(s"${id} isn't a record")
         }
    }
}


case class DxFile(id : String,
                  project : Option[DxProject]) extends DxDataObject {
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
}

case class FolderContents(dataObjects: Vector[DxDataObject],
                          subFolders : Vector[String])

// A project is a subtype of a container
case class DxProject(id: String) extends DxDataObject {

    override def describe() : DxDescribe = {
        val response = DXAPI.projectDescribe(id, classOf[JsonNode], DxUtils.dxEnv)
        val descJs:JsValue = DxUtils.jsValueOfJsonNode(response)
        val desc = descJs.asJsObject.getFields("name", "size", "created", "modified") match {
            case Seq(JsString(name), JsNumber(created), JsNumber(modified)) =>
                DxDescribe(name,
                           "/",
                           Some(0),
                           this,
                           this,
                           created.toLong,
                           modified.toLong,
                           Map.empty,
                           None,
                           None,
                           None,
                           None,
                           None,
                           None,
                           None)
            case other =>
                throw new Exception(s"malformed JSON ${other}")
        }
        desc
    }

    override def describe(field: Field.Value) : DxDescribe = {
        throw new Exception("not supported")
    }

    override def describe(fields: Vector[Field.Value]) : DxDescribe = {
        throw new Exception("not supported")
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

case class DxApplet(id : String,
                    project : Option[DxProject]) extends DxExecutable

object DxApplet {
    def getInstance(id : String) : DxApplet = {
        DxDataObject.getInstance(id) match {
             case a : DxApplet => a
             case _ =>
                throw new IllegalArgumentException(s"${id} isn't an applet")
        }
    }
}

case class DxApp(id : String) extends DxExecutable

object DxApp {
    def getInstance(id : String) : DxApp = {
        DxDataObject.getInstance(id) match {
             case a : DxApp => a
             case _ =>
                throw new IllegalArgumentException(s"${id} isn't an app")
        }
    }
}

case class DxWorkflow(id : String,
                      project : Option[DxProject]) extends DxExecutable {
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

case class DxAnalysis(id : String) extends DxObject with DxExecution {
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

case class DxJob(id : String) extends DxObject with DxExecution

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
