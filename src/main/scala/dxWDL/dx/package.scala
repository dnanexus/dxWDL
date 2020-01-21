package dxWDL.dx

import spray.json._

object DxIOClass extends Enumeration {
  val INT, FLOAT, STRING, BOOLEAN, FILE, ARRAY_OF_INTS, ARRAY_OF_FLOATS, ARRAY_OF_STRINGS,
      ARRAY_OF_BOOLEANS, ARRAY_OF_FILES, HASH, OTHER = Value

  def fromString(s: String): DxIOClass.Value = {
    s match {
      // primitives
      case "int"     => INT
      case "float"   => FLOAT
      case "string"  => STRING
      case "boolean" => BOOLEAN
      case "file"    => FILE

      // arrays of primitives
      case "array:int"     => ARRAY_OF_INTS
      case "array:float"   => ARRAY_OF_FLOATS
      case "array:string"  => ARRAY_OF_STRINGS
      case "array:boolean" => ARRAY_OF_BOOLEANS
      case "array:file"    => ARRAY_OF_FILES

      // hash
      case "hash" => HASH

      // we don't deal with anything else
      case other => OTHER
    }
  }
}

case class IOParameter(
    name: String,
    ioClass: DxIOClass.Value,
    optional: Boolean,
    help: Option[String] = None,
    patterns: Option[Vector[String]] = None
)

// Extra fields for describe
object Field extends Enumeration {
  val Analysis, Applet, ArchivalState, Created, Details, Folder, Id, InputSpec, Modified, Name,
      OutputSpec, ParentJob, Parts, Project, Properties, Size, Stages = Value
}

trait DxObjectDescribe {
  val id: String
  val name: String
  val created: Long
  val modified: Long
  val properties: Option[Map[String, String]]
  val details: Option[JsValue]

  def getCreationDate(): java.util.Date = new java.util.Date(created)
}

trait DxObject {
  val id: String
  def getId: String = id
  def describe(fields: Set[Field.Value]): DxObjectDescribe
}

object DxObject {
  def parseJsonProperties(props: JsValue): Map[String, String] = {
    props.asJsObject.fields.map {
      case (k, JsString(v)) => k -> v
      case (_, _) =>
        throw new Exception(s"malform JSON properties ${props}")
    }.toMap
  }

  def parseIoParam(jsv: JsValue): IOParameter = {
    val ioParam = jsv.asJsObject.getFields("name", "class") match {
      case Seq(JsString(name), JsString(klass)) =>
        val ioClass = DxIOClass.fromString(klass)
        IOParameter(name, ioClass, false)
      case other =>
        throw new Exception(s"Malformed io spec ${other}")
    }

    val optFlag = jsv.asJsObject.fields.get("optional") match {
      case Some(JsBoolean(b)) => b
      case None               => false
    }

    val help = jsv.asJsObject.fields.get("help") match {
      case Some(JsString(s)) => Some(s)
      case _                 => None
    }
    val patterns = jsv.asJsObject.fields.get("patterns") match {
      case Some(JsArray(a)) =>
        Some(a.flatMap {
          case JsString(s) => Some(s)
          case _           => None
        })
      case _ => None
    }
    ioParam.copy(optional = optFlag, patterns = patterns, help = help)
  }

  def parseIOSpec(specs: Vector[JsValue]): Vector[IOParameter] = {
    specs.map(ioSpec => parseIoParam(ioSpec)).toVector
  }

  def maybeSpecifyProject(project: Option[DxProject]): Map[String, JsValue] = {
    project match {
      case None =>
        // we don't know the project.
        Map.empty
      case Some(p) =>
        // We know the project, this makes the search more efficient.
        Map("project" -> JsString(p.id))
    }
  }

  def requestFields(fields: Set[Field.Value]): JsValue = {
    val fieldStrings = fields.map {
      case Field.Analysis      => "analysis"
      case Field.Applet        => "applet"
      case Field.ArchivalState => "archivalState"
      case Field.Created       => "created"
      case Field.Details       => "details"
      case Field.Folder        => "folder"
      case Field.Id            => "id"
      case Field.InputSpec     => "inputSpec"
      case Field.Modified      => "modified"
      case Field.Name          => "name"
      case Field.OutputSpec    => "outputSpec"
      case Field.ParentJob     => "parentJob"
      case Field.Parts         => "parts"
      case Field.Project       => "project"
      case Field.Properties    => "properties"
      case Field.Size          => "size"
      case Field.Stages        => "stages"
    }.toVector
    val m: Map[String, JsValue] = fieldStrings.map { x =>
      x -> JsTrue
    }.toMap
    JsObject(m)
  }

  // We are expecting string like:
  //    record-FgG51b00xF63k86F13pqFv57
  //    file-FV5fqXj0ffPB9bKP986j5kVQ
  //
  def getInstance(id: String, container: Option[DxProject] = None): DxObject = {
    val parts = id.split("-")
    if (parts.length != 2)
      throw new IllegalArgumentException(
          s"${id} is not of the form class-alphnumeric{24}"
      )
    val klass = parts(0)
    val numLetters = parts(1)
    if (!numLetters.matches("[A-Za-z0-9]{24}"))
      throw new IllegalArgumentException(
          s"${numLetters} does not match [A-Za-z0-9]{24}"
      )

    klass match {
      case "project"   => DxProject(id)
      case "container" => DxProject(id)
      case "file"      => DxFile(id, container)
      case "record"    => DxRecord(id, container)
      case "app"       => DxApp(id)
      case "applet"    => DxApplet(id, container)
      case "workflow"  => DxWorkflow(id, container)
      case "job"       => DxJob(id, container)
      case "analysis"  => DxAnalysis(id, container)
      case _ =>
        throw new IllegalArgumentException(
            s"${id} does not belong to a know class"
        )
    }
  }

  // convenience methods
  def getInstance(id: String, container: DxProject): DxObject = {
    getInstance(id, Some(container))
  }

  def isDataObject(id: String): Boolean = {
    try {
      val o = getInstance(id, None)
      o.isInstanceOf[DxDataObject]
    } catch {
      case e: IllegalArgumentException =>
        false
    }
  }
}

trait DxDataObject extends DxObject

// Objects that can be run on the platform
trait DxExecutable extends DxDataObject

// Actual executions on the platform. There are jobs and analyses
trait DxExecution extends DxObject

// A stand in for the DxWorkflow.Stage inner class (we don't have a constructor for it)
case class DxWorkflowStage(id: String) {
  def getId() = id

  def getInputReference(inputName: String): JsValue = {
    JsObject(
        "$dnanexus_link" -> JsObject("stage" -> JsString(id), "inputField" -> JsString(inputName))
    )
  }
  def getOutputReference(outputName: String): JsValue = {
    JsObject(
        "$dnanexus_link" -> JsObject("stage" -> JsString(id), "outputField" -> JsString(outputName))
    )
  }
}
