package dxWDL.dx

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

trait DxObjectDescribe {
    val id  : String
    val name : String
    val created : Long
    val modified : Long
    val properties: Option[Map[String, String]]
    val details : Option[JsValue]

    def getCreationDate() : java.util.Date = new java.util.Date(created)
}

trait DxObject {
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

// Objects that can be run on the platform
trait DxExecutable extends DxDataObject

// Actual executions on the platform. There are jobs and analyses
trait DxExecution extends DxObject

trait DxDataObject extends DxObject

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
