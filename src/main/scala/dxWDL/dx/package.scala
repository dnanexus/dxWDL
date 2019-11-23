package dxWDL.dx

import spray.json._

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
                      details : Option[JsValue])

// Extra fields for describe
object Field extends Enumeration {
    val Details, Parts = Value
}

sealed trait DxObject {
    val id : String
    def getId() : String = id

    def describe() : DxDescribe = {
        val results = DxBulkDescribe.apply(Vector(this), Vector.empty)
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
    // core implementation
    def getInstance(id : String,
                    container: Option[DxProject]) : DxDataObject = {
        id match {
            case _ if id.startsWith("container-") =>
                DxProject(id)
            case _ if id.startsWith("project-") =>
                DxProject(id)
            case _ if id.startsWith("file-") =>
                DxFile(id, container)
            case _ if id.startsWith("record-") =>
                DxRecord(id, container)
            case _ if id.startsWith("applet-") =>
                DxApplet(id, container)
            case _ if id.startsWith("workflow-") =>
                DxWorkflow(id, container)
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

    private val prefixes = List("container-", "project-", "file-", "record-", "applet-", "workflow-")
    def isDataObject(id : String) : Boolean = {
        prefixes.exists{ p => id.startsWith(p) }
    }
}

case class DxRecord(id : String,
                    proj : Option[DxProject]) extends DxDataObject
object DxRecord {
    def getInstance(id : String) : DxRecord = {
        if (id.startsWith("record-"))
            return DxRecord(id, None)
        throw new IllegalArgumentException(s"${id} isn't a record")
    }
}


case class DxFile(id : String,
                  proj : Option[DxProject]) extends DxDataObject {
    def getLinkAsJson : JsValue = {
        proj match {
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
        if (id.startsWith("file-"))
            return DxFile(id, None)
        throw new IllegalArgumentException(s"${id} isn't a file")
    }
}

case class FolderContents(dataObjects: List[DxDataObject],
                          subfolders : List[String])

// A project is a subtype of a container
case class DxProject(id: String) extends DxDataObject {
    def listFolder(path : String) : FolderContents = ???

    def newFolder(folderPath : String, parents : Boolean) : Unit = ???

    def move(files: Vector[DxDataObject], destinationFolder : String) : Unit = ???

    def removeObjects(objs : Vector[DxDataObject]) : Unit = ???
}

object DxProject {
    def getInstance(id : String) : DxProject = {
        if (id.startsWith("container-"))
            return DxProject(id)
        if (id.startsWith("project-"))
            return DxProject(id)
        throw new IllegalArgumentException(s"${id} isn't a container")
    }
}

// Objects that can be run on the platform
sealed trait DxExecutable extends DxDataObject
case class DxApplet(id : String,
                    proj : Option[DxProject]) extends DxExecutable
object DxApplet {
    def getInstance(id : String) : DxApplet = {
        if (id.startsWith("applet-"))
            return DxApplet(id, None)
        throw new IllegalArgumentException(s"${id} isn't an applet")
    }
}

case class DxApp(id : String) extends DxExecutable

case class DxWorkflow(id : String,
                      proj : Option[DxProject]) extends DxExecutable {
    def close() : Unit = ???
}

object DxWorkflow {
    def getInstance(id : String) : DxWorkflow = {
        if (id.startsWith("workflow-"))
            return DxWorkflow(id, None)
        throw new IllegalArgumentException(s"${id} isn't a workflow")
    }
}

// Actual executions on the platform. There are jobs and analyses
sealed trait DxExecution extends DxObject

case class DxAnalysis(id : String) extends DxObject with DxExecution

object DxAnalysis {
    def getInstance(id : String) : DxAnalysis = {
        if (id.startsWith("analysis-"))
            return DxAnalysis(id)
        throw new IllegalArgumentException(s"${id} isn't a job")
    }
}

case class DxJob(id : String) extends DxObject with DxExecution {
    def getApplet() : DxApplet = ???
    def getParentJob() : DxJob = ???
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
