package dx.api

import spray.json._
import wdlTools.util.{Enum, JsUtils}

import scala.reflect.ClassTag

object ExecutableState extends Enum {
  type ExecutableState = Value
  val Runnable, In_Progress, Partially_Failed, Done, Failed, Terminating, Terminated = Value
}

case class SubtotalPriceInfo(subtotalPrice: Double, priceComputedAt: Long)

object DescriptionField extends Enum {
  type DescriptionField = Value
  val Analysis, BillTo, Class, Created, DelayWorkspaceDestruction, Details, Executable,
      ExecutableName, Folder, Id, Input, LaunchedBy, Modified, Name, OriginalInput, Output,
      ParentAnalysis, ParentJob, PriceComputedAt, Project, Properties, RootExecution, RunInput,
      Stage, Stages, State, SubtotalPriceInfo, Tags, TotalPrice, Workflow, Workspace =
    Value
}

trait DescriptionFieldSpec[T] {
  val key: String

  def parse(jsValue: JsValue): T
}

abstract class JsValueFieldSpec(val key: String) extends DescriptionFieldSpec[JsValue] {
  def parse(jsValue: JsValue): JsValue = jsValue
}

abstract class JsMapFieldSpec(val key: String) extends DescriptionFieldSpec[Map[String, JsValue]] {
  def parse(jsValue: JsValue): Map[String, JsValue] = {
    JsUtils.getFields(jsValue)
  }
}

abstract class OptionalJsMapFieldSpec(val key: String)
    extends DescriptionFieldSpec[Option[Map[String, JsValue]]] {
  def parse(jsValue: JsValue): Option[Map[String, JsValue]] = {
    jsValue match {
      case JsNull => None
      case _      => Some(JsUtils.getFields(jsValue))
    }
  }
}

abstract class StringFieldSpec(val key: String) extends DescriptionFieldSpec[String] {
  def parse(jsValue: JsValue): String = {
    JsUtils.getString(jsValue)
  }
}

abstract class OptionalStringFieldSpec(val key: String)
    extends DescriptionFieldSpec[Option[String]] {
  def parse(jsValue: JsValue): Option[String] = {
    jsValue match {
      case JsNull => None
      case _      => Some(JsUtils.getString(jsValue))
    }
  }
}

abstract class StringArrayFieldSpec(val key: String) extends DescriptionFieldSpec[Vector[String]] {
  def parse(jsValue: JsValue): Vector[String] = {
    JsUtils.getValues(jsValue).map(JsUtils.getString(_))
  }
}

abstract class StringMapFieldSpec(val key: String)
    extends DescriptionFieldSpec[Map[String, String]] {
  def parse(jsValue: JsValue): Map[String, String] = {
    JsUtils.getFields(jsValue).view.mapValues(JsUtils.getString(_)).toMap
  }
}

abstract class BooleanFieldSpec(val key: String) extends DescriptionFieldSpec[Boolean] {
  def parse(jsValue: JsValue): Boolean = {
    JsUtils.getBoolean(jsValue)
  }
}

abstract class LongFieldSpec(val key: String) extends DescriptionFieldSpec[Long] {
  def parse(jsValue: JsValue): Long = {
    JsUtils.getLong(jsValue)
  }
}

abstract class DoubleFieldSpec(val key: String) extends DescriptionFieldSpec[Double] {
  def parse(jsValue: JsValue): Double = {
    JsUtils.getDouble(jsValue)
  }
}

abstract class EnumFieldSpec[T <: Enum](val key: String, resolve: String => T)
    extends DescriptionFieldSpec[T] {
  def parse(jsValue: JsValue): T = {
    val strValue = JsUtils.getString(jsValue, Some(key))
    resolve(strValue)
  }
}

object AnalysisFieldSpec extends OptionalStringFieldSpec("analysis")
object BillToFieldSpec extends StringFieldSpec("billTo")
object ClassFieldSpec extends StringFieldSpec("class")
object CreatedFieldSpec extends LongFieldSpec("created")
object DelayWorkspaceDestructionSpec extends BooleanFieldSpec("delayWorkspaceDestruction")
object DetailsFieldSpec extends JsValueFieldSpec("details")
object ExecutableFieldSpec extends StringFieldSpec("executable")
object ExecutableNameFieldSpec extends StringFieldSpec("executableName")
object FolderFieldSpec extends StringFieldSpec("folder")
object IdFieldSpec extends StringFieldSpec("id")
object InputFieldSpec extends JsMapFieldSpec("input")
object LaunchedByFieldSpec extends StringFieldSpec("launchedBy")
object ModifiedFieldSpec extends LongFieldSpec("modified")
object NameFieldSpec extends StringFieldSpec("name")
object OriginalInputFieldSpec extends JsMapFieldSpec("originalInput")
object OutputFieldSpec extends OptionalJsMapFieldSpec("output")
object ParentAnalysisSpec extends OptionalStringFieldSpec("parentAnalysis")
object ParentJobSpec extends OptionalStringFieldSpec("parentJob")
object PriceComputedAtSpec extends LongFieldSpec("priceComputedAt")
object ProjectFieldSpec extends StringFieldSpec("project")
object PropertiesFieldSpec extends StringMapFieldSpec("properties")
object RootExecutionFieldSpec extends StringFieldSpec("rootExecution")
object RunInputFieldSpec extends JsMapFieldSpec("runInput")
object StageFieldSpec extends OptionalStringFieldSpec("stage")
object StagesFieldSpec extends DescriptionFieldSpec[Map[String, String]] {
  override val key = "stages"
  override def parse(jsValue: JsValue): Map[String, String] = {
    JsUtils
      .getValues(jsValue)
      .map { jsv =>
        jsv.asJsObject.getFields("id", "execution") match {
          case Seq(JsString(id), execution) =>
            id -> JsUtils.getString(execution, Some("id"))
          case other =>
            throw new Exception(s"invalid stage ${other}")
        }
      }
      .toMap
  }
}
object StateFieldSpec
    extends EnumFieldSpec[ExecutableState.ExecutableState](
        "state",
        ExecutableState.withNameIgnoreCase
    )
object SubtotalPriceInfoSpec extends DescriptionFieldSpec[SubtotalPriceInfo] {
  override val key = "subtotalPriceInfo"
  override def parse(jsValue: JsValue): SubtotalPriceInfo = {
    val subtotalPrice = JsUtils.getDouble(jsValue, Some("subtotalPrice"))
    val priceComputedAt = JsUtils.getLong(jsValue, Some("priceComputedAt"))
    SubtotalPriceInfo(subtotalPrice, priceComputedAt)
  }
}
object TagsFieldSpec extends StringArrayFieldSpec("tags")
object TotalPriceSpec extends LongFieldSpec("totalPrice")
object WorkflowSpec extends DescriptionFieldSpec[String] {
  override val key: String = "workflow"
  override def parse(jsValue: JsValue): String = {
    JsUtils.getString(jsValue, Some("id"))
  }
}
object WorkspaceFieldSpec extends StringFieldSpec("workspace")

object DescriptionFieldSpec {
  val Specs: Map[DescriptionField.DescriptionField, DescriptionFieldSpec[_]] = Map(
      DescriptionField.Analysis -> AnalysisFieldSpec,
      DescriptionField.BillTo -> BillToFieldSpec,
      DescriptionField.Class -> ClassFieldSpec,
      DescriptionField.Created -> CreatedFieldSpec,
      DescriptionField.Details -> DetailsFieldSpec,
      DescriptionField.DelayWorkspaceDestruction -> DelayWorkspaceDestructionSpec,
      DescriptionField.Executable -> ExecutableFieldSpec,
      DescriptionField.ExecutableName -> ExecutableNameFieldSpec,
      DescriptionField.Folder -> FolderFieldSpec,
      DescriptionField.Id -> IdFieldSpec,
      DescriptionField.Input -> InputFieldSpec,
      DescriptionField.LaunchedBy -> LaunchedByFieldSpec,
      DescriptionField.Modified -> ModifiedFieldSpec,
      DescriptionField.Name -> NameFieldSpec,
      DescriptionField.OriginalInput -> OriginalInputFieldSpec,
      DescriptionField.Output -> OutputFieldSpec,
      DescriptionField.ParentAnalysis -> ParentAnalysisSpec,
      DescriptionField.ParentJob -> ParentJobSpec,
      DescriptionField.PriceComputedAt -> PriceComputedAtSpec,
      DescriptionField.Project -> ProjectFieldSpec,
      DescriptionField.Properties -> PropertiesFieldSpec,
      DescriptionField.RootExecution -> RootExecutionFieldSpec,
      DescriptionField.RunInput -> RunInputFieldSpec,
      DescriptionField.Stage -> StageFieldSpec,
      DescriptionField.Stages -> StagesFieldSpec,
      DescriptionField.State -> StateFieldSpec,
      DescriptionField.SubtotalPriceInfo -> SubtotalPriceInfoSpec,
      DescriptionField.Tags -> TagsFieldSpec,
      DescriptionField.TotalPrice -> TotalPriceSpec,
      DescriptionField.Workflow -> WorkflowSpec,
      DescriptionField.Workspace -> WorkspaceFieldSpec
  )
}

trait NewDxObjectDescribe {
  val id: String

  protected var cachedDescribeJs: Map[DescriptionField.DescriptionField, JsValue] = Map.empty
  protected var cachedDescribe: Map[DescriptionField.DescriptionField, Any] = Map.empty
  protected val globalFields: Set[DescriptionField.DescriptionField] =
    Set(DescriptionField.Id, DescriptionField.Class)

  protected def defaultFields: Set[DescriptionField.DescriptionField]

  protected def otherFields: Set[DescriptionField.DescriptionField]

  private lazy val allowedFields = globalFields | defaultFields | otherFields

  def hasField(field: DescriptionField.DescriptionField): Boolean = {
    allowedFields.contains(field)
  }

  protected def callDescribe(request: Map[String, JsValue]): JsObject

  protected def createRequest(
      fields: Set[DescriptionField.DescriptionField]
  ): Map[String, JsValue] = {
    Map("fields" -> JsObject(fields.map { field =>
      val spec = DescriptionFieldSpec.Specs(field)
      spec.key -> JsTrue
    }.toMap))
  }

  /**
    * Describes this object.
    * @param fields set of fields to describe.
    * @param includeDefaults whether to add the object type's default describe fields to `fields`.
    * @param refresh whether to re-describe any fields that have been cached.
    * @param reset whether to clear the cached fields before issuing the new describe call.
    */
  def describe(fields: Set[DescriptionField.DescriptionField] = Set.empty,
               includeDefaults: Boolean = true,
               refresh: Boolean = false,
               reset: Boolean = false): Unit = {
    val refreshFields: Set[DescriptionField.DescriptionField] = if (refresh) {
      cachedDescribeJs.keySet
    } else {
      Set.empty
    }
    if (reset) {
      cachedDescribeJs = Map.empty
      cachedDescribe = Map.empty
    }
    val allFields = fields | refreshFields | (if (includeDefaults) defaultFields else Set.empty)
    val invalidFields = allFields.diff(allowedFields)
    if (invalidFields.nonEmpty) {
      throw new Exception(s"Invalid fields ${invalidFields.mkString(",")}")
    }
    if (allFields.nonEmpty) {
      val request = createRequest(allFields)
      val response = callDescribe(request)
      cachedDescribeJs ++= response.fields
    }
  }

  /**
    * Sets/updates the description from a field-value map.
    * @param description the field-value map.
    * @param reset whether to clear all cached values before updating.
    * @return the previous values of any overwritten fields
    */
  def setOrUpdateDescription(
      description: Map[DescriptionField.DescriptionField, JsValue],
      reset: Boolean = false
  ): Map[DescriptionField.DescriptionField, JsValue] = {
    if (reset) {
      val overwrittenValues = cachedDescribeJs
      cachedDescribeJs = description
      cachedDescribe = Map.empty
      overwrittenValues
    } else {
      val overwrittenValues = if (cachedDescribe.nonEmpty) {
        val (overwritten, retained) = cachedDescribe.keySet.partition { field =>
          description.contains(field) && cachedDescribeJs(field) != description(field)
        }
        cachedDescribe = cachedDescribe.view.filterKeys(retained).toMap
        cachedDescribeJs.view.filterKeys(overwritten).toMap
      } else {
        Map.empty[DescriptionField.DescriptionField, JsValue]
      }
      cachedDescribeJs ++= description
      overwrittenValues
    }
  }

  protected def getCachedField[T: ClassTag[T]](
      field: DescriptionField.DescriptionField
  ): Option[T] = {
    if (cachedDescribe.contains(field)) {
      cachedDescribe(field) match {
        case value: T => Some(value)
        case _        => throw new Exception(s"Invalid type for field ${field}")
      }
    } else if (cachedDescribeJs.contains(field)) {
      DescriptionFieldSpec.Specs(field) match {
        case spec: DescriptionFieldSpec[T] =>
          val value = spec.parse(cachedDescribeJs(field))
          cachedDescribe += field -> value
          Some(value)
        case _ =>
          throw new Exception(s"Invalid type for field ${field}")
      }
    } else {
      None
    }
  }

  def getField[T: ClassTag[T]](field: DescriptionField.DescriptionField): T = {
    if (!cachedDescribeJs.contains(field)) {
      describe(Set(field), includeDefaults = false)
    }
    getCachedField(field).get
  }
}

abstract class BaseDxObjectDescribe extends NewDxObjectDescribe {
  override protected val defaultFields: Set[DescriptionField.DescriptionField] = {
    super.defaultFields | Set(
        DescriptionField.Created,
        DescriptionField.Modified
    )
  }

  override protected val otherFields: Set[DescriptionField.DescriptionField] = {
    super.otherFields | Set(
        DescriptionField.Tags,
        DescriptionField.Properties
    )
  }

  def created: Long = getField[Long](DescriptionField.Created)

  def modified: Long = getField[Long](DescriptionField.Modified)

  def tags: Vector[String] = getField[Vector[String]](DescriptionField.Tags)

  protected def callSetProperties(request: Map[String, JsValue]): Unit

  def setProperties(newProperties: Map[String, String]): Unit = {
    val jsProperties = JsObject(newProperties.view.mapValues(s => JsString(s)).toMap)
    callSetProperties(Map("properties" -> jsProperties))
    cachedDescribeJs += (DescriptionField.Properties -> jsProperties)
    cachedDescribe += (DescriptionField.Properties -> newProperties)
  }

  def properties: Map[String, String] = getField[Map[String, String]](DescriptionField.Properties)

  def details: JsValue = getField[JsValue](DescriptionField.Details)
}

trait NewDxDataObjectDescribe extends NewDxObjectDescribe {
  def project: Option[DxProject]
}

abstract class BaseDxDataObjectDescribe(dxProject: Option[DxProject], dxApi: DxApi = DxApi.get)
    extends BaseDxObjectDescribe
    with NewDxDataObjectDescribe {

  override def project: Option[DxProject] =
    dxProject.orElse(getCachedField[String](DescriptionField.Project).map(DxProject(dxApi, _)))

  override protected val globalFields: Set[DescriptionField.DescriptionField] = {
    super.globalFields | Set(DescriptionField.Project)
  }

  override protected val defaultFields: Set[DescriptionField.DescriptionField] = {
    super.defaultFields | Set(
        DescriptionField.Name,
        DescriptionField.Project,
        DescriptionField.Folder
    )
  }

  override protected def createRequest(
      fields: Set[DescriptionField.DescriptionField]
  ): Map[String, JsValue] = {
    super.createRequest(fields) ++ project
      .map(proj => Map("project" -> JsString(proj.id)))
      .getOrElse(Map.empty)
  }

  def name: String = getField[String](DescriptionField.Name)

  def folder: String = getField[String](DescriptionField.Folder)
}
