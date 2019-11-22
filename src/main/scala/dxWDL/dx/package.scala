package dxWDL.dx

import com.dnanexus._
import scala.collection.immutable.TreeMap
import spray.json._
import wom.types._

import dxWDL.base.WomTypeSerialization

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
                      id : String,
                      folder: String,
                      size : Option[Long],
                      container: DxContainer, // a project or a container
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
    def getId : String = id

    def describe(extraFields: Vector[Field.Value] = Vector.empty[Field.Value]) : DxDescribe = {
        DxBulkDescribe.apply(Vector(id), extraFields)
    }
}

sealed trait DxDataObject extends DxObject

case class DxContainer(id: String) extends DxDataObject
object DxContainer {
    def getInstance(id : String) : DxContainer = {
        if (id.startsWith("container-"))
            return DxContainer(id, None)
        if (id.startsWith("project-"))
            return DxProject(id, None)
        throw new IllegalArgumentException(s"${id} isn't a container")
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
                  proj : Option[DxProject]) extends DxDataObject
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
case class DxProject(id: String) extends DxContainer {
    def getInstance(id : String) : DxProject = {
        if (id.startsWith("project-"))
            return DxProject(id, None)
        throw new IllegalArgumentException(s"${id} isn't a project")
    }

    def listFolder(path : String) : FolderContents = ???

    def newFolder(folderPath : String, parents : Boolean) : Unit = ???

    def move(files: List[DxDataObject], destinationFolder : String) : Unit = ???
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

case class DxWorkflow(id : String,
                      proj : Option[DxProject]) extends DxExecutable
object DxWorkflow {
    def getInstance(id : String) : DxWorkflow = {
        if (id.startsWith("workflow-"))
            return DxApplet(id, None)
        throw new IllegalArgumentException(s"${id} isn't a workflow")
    }
}

// Actual executions on the platform. There are jobs and analyses
sealed trait DxExecution
case class DxAnalysis(id : String) extends DxObject with DxExecution
case class DxJob(id : String) extends DxObject with DxExecution {
    def getApplet() : DxApplet = ???
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
