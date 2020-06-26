package dx.api

import spray.json._

// Extra fields for describe
object Field extends Enumeration {
  type Field = Value
  val Access, Analysis, Applet, ArchivalState, AvailableInstanceTypes, BillTo, Categories, Created,
      Description, Details, DeveloperNotes, Folder, Id, IgnoreReuse, Inputs, InputSpec, Modified,
      Name, Outputs, OutputSpec, ParentJob, Parts, Project, Properties, Region, RunSpec, Size,
      Stages, Summary, Tags, Title, Types, Version = Value
}

trait DxObjectDescribe {
  val id: String
  val name: String
  val created: Long
  val modified: Long
  val properties: Option[Map[String, String]]
  val details: Option[JsValue]

  def getCreationDate: java.util.Date = new java.util.Date(created)
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
    }
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
      case Field.Access                 => "access"
      case Field.Analysis               => "analysis"
      case Field.Applet                 => "applet"
      case Field.ArchivalState          => "archivalState"
      case Field.AvailableInstanceTypes => "availableInstanceTypes"
      case Field.BillTo                 => "billTo"
      case Field.Categories             => "categories"
      case Field.Created                => "created"
      case Field.Description            => "description"
      case Field.DeveloperNotes         => "developerNotes"
      case Field.Details                => "details"
      case Field.Folder                 => "folder"
      case Field.Id                     => "id"
      case Field.IgnoreReuse            => "ignoreReuse"
      case Field.Inputs                 => "inputs"
      case Field.InputSpec              => "inputSpec"
      case Field.Modified               => "modified"
      case Field.Name                   => "name"
      case Field.Outputs                => "outputs"
      case Field.OutputSpec             => "outputSpec"
      case Field.ParentJob              => "parentJob"
      case Field.Parts                  => "parts"
      case Field.Project                => "project"
      case Field.Properties             => "properties"
      case Field.Region                 => "region"
      case Field.RunSpec                => "runSpec"
      case Field.Size                   => "size"
      case Field.Stages                 => "stages"
      case Field.Summary                => "summary"
      case Field.Tags                   => "tags"
      case Field.Title                  => "title"
      case Field.Types                  => "types"
      case Field.Version                => "version"
    }.toVector
    val m: Map[String, JsValue] = fieldStrings.map { x =>
      x -> JsTrue
    }.toMap
    JsObject(m)
  }
}

trait DxDataObject extends DxObject

// Objects that can be run on the platform. These are apps, applets, and workflows.
trait DxExecutable extends DxDataObject

// Actual executions on the platform. There are jobs and analyses
trait DxExecution extends DxObject
