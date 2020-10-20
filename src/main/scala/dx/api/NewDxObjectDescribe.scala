package dx.api

import spray.json._
import wdlTools.util.{Enum, JsUtils}

import scala.reflect.ClassTag

object ExecutableState extends Enum {
  type ExecutableState = Value
  val Runnable, In_Progress, Partially_Failed, Done, Failed, Terminating, Terminated = Value
}

case class SubtotalPriceInfo(subtotalPrice: Double, priceComputedAt: Long)

case class PricingPolicy(unit: String, unitPrice: Double)

case class RegionOptions(region: String, appletId: String, resourcesId: String, pricingPolicy: Option[PricingPolicy])

object DescriptionField extends Enum {
  type DescriptionField = Value
  val Aliases, Analysis, AuthorizedUsers, BillTo, Class, Created, CreatedBy, DelayWorkspaceDestruction, Deleted,
      Details, Executable, ExecutableName, Folder, HttpsApp, Id, IgnoreReuse, Input, Installed, Installs,
      IsDeveloperFor, LaunchedBy, Modified, Name, OpenSource, OriginalInput, Output, ParentAnalysis, ParentJob,
      PriceComputedAt, Project, Properties, RegionalOptions, RootExecution, RunInput, Stage, Stages, State,
      SubtotalPriceInfo, Tags, TotalPrice, Version, Workflow, Workspace = Value
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

object AliasesFieldSpec extends StringArrayFieldSpec("aliases")
object AnalysisFieldSpec extends OptionalStringFieldSpec("analysis")
object AuthorizedUsersFieldSpec extends StringArrayFieldSpec("authorizedUsers")
object BillToFieldSpec extends StringFieldSpec("billTo")
object ClassFieldSpec extends StringFieldSpec("class")
object CreatedFieldSpec extends LongFieldSpec("created")
object CreatedByFieldSpec extends StringFieldSpec("createdBy")
object DelayWorkspaceDestructionFieldSpec extends BooleanFieldSpec("delayWorkspaceDestruction")
object DeletedFieldSpec extends BooleanFieldSpec("deleted")
object DetailsFieldSpec extends JsValueFieldSpec("details")
object ExecutableFieldSpec extends StringFieldSpec("executable")
object ExecutableNameFieldSpec extends StringFieldSpec("executableName")
object FolderFieldSpec extends StringFieldSpec("folder")
object IdFieldSpec extends StringFieldSpec("id")
object IgnoreReuseFieldSpec extends BooleanFieldSpec("ignoreReuse")
object InputFieldSpec extends JsMapFieldSpec("input")
object InstalledFieldSpec extends BooleanFieldSpec("installed")
object InstallsFieldSpec extends LongFieldSpec("installs")
object IsDeveloperForFieldSpec extends BooleanFieldSpec("isDeveloperFor")
object LaunchedByFieldSpec extends StringFieldSpec("launchedBy")
object ModifiedFieldSpec extends LongFieldSpec("modified")
object NameFieldSpec extends StringFieldSpec("name")
object OpenSourceFieldSpec extends BooleanFieldSpec("openSource")
object OriginalInputFieldSpec extends JsMapFieldSpec("originalInput")
object OutputFieldSpec extends OptionalJsMapFieldSpec("output")
object ParentAnalysisFieldSpec extends OptionalStringFieldSpec("parentAnalysis")
object ParentJobFieldSpec extends OptionalStringFieldSpec("parentJob")
object PriceComputedAtFieldSpec extends LongFieldSpec("priceComputedAt")
object ProjectFieldSpec extends StringFieldSpec("project")
object PropertiesFieldSpec extends StringMapFieldSpec("properties")
object RegionalOptionsFieldSpec extends DescriptionFieldSpec[Vector[RegionOptions]] {
  override val key: String = "regionaLOptions"
  override def parse(jsValue: JsValue): Vector[RegionOptions] = {
    jsValue.asJsObject.fields.map {
      case (region, options) =>
        val pricingPolicy = options.asJsObject.fields.get("pricingPolicy").map {
          val fields = JsUtils.getFields(_)

        }
        RegionOptions(region, JsUtils.getString(options, Some("applet")), JsUtils.getString(options, Some("resources")), pricingPolicy)
    }
  }
}
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
object SubtotalPriceInfoFieldSpec extends DescriptionFieldSpec[SubtotalPriceInfo] {
  override val key = "subtotalPriceInfo"
  override def parse(jsValue: JsValue): SubtotalPriceInfo = {
    val subtotalPrice = JsUtils.getDouble(jsValue, Some("subtotalPrice"))
    val priceComputedAt = JsUtils.getLong(jsValue, Some("priceComputedAt"))
    SubtotalPriceInfo(subtotalPrice, priceComputedAt)
  }
}
object TagsFieldSpec extends StringArrayFieldSpec("tags")
object TotalPriceFieldSpec extends LongFieldSpec("totalPrice")
object VersionFieldSpec extends StringFieldSpec("version")
object WorkflowFieldSpec extends DescriptionFieldSpec[String] {
  override val key: String = "workflow"
  override def parse(jsValue: JsValue): String = {
    JsUtils.getString(jsValue, Some("id"))
  }
}
object WorkspaceFieldSpec extends StringFieldSpec("workspace")

object DescriptionFieldSpec {
  val Specs: Map[DescriptionField.DescriptionField, DescriptionFieldSpec[_]] = Map(
      DescriptionField.Aliases -> AliasesFieldSpec,
      DescriptionField.Analysis -> AnalysisFieldSpec,
      DescriptionField.AuthorizedUsers -> AuthorizedUsersFieldSpec,
      DescriptionField.BillTo -> BillToFieldSpec,
      DescriptionField.Class -> ClassFieldSpec,
      DescriptionField.Created -> CreatedFieldSpec,
      DescriptionField.CreatedBy -> CreatedByFieldSpec,
      DescriptionField.Deleted -> DeletedFieldSpec,
      DescriptionField.Details -> DetailsFieldSpec,
      DescriptionField.DelayWorkspaceDestruction -> DelayWorkspaceDestructionFieldSpec,
      DescriptionField.Executable -> ExecutableFieldSpec,
      DescriptionField.ExecutableName -> ExecutableNameFieldSpec,
      DescriptionField.Folder -> FolderFieldSpec,
      DescriptionField.Id -> IdFieldSpec,
      DescriptionField.IgnoreReuse -> IgnoreReuseFieldSpec,
      DescriptionField.Input -> InputFieldSpec,
      DescriptionField.Installed -> InstalledFieldSpec,
      DescriptionField.Installs -> InstallsFieldSpec,
      DescriptionField.IsDeveloperFor -> IsDeveloperForFieldSpec,
      DescriptionField.LaunchedBy -> LaunchedByFieldSpec,
      DescriptionField.Modified -> ModifiedFieldSpec,
      DescriptionField.Name -> NameFieldSpec,
      DescriptionField.OriginalInput -> OriginalInputFieldSpec,
      DescriptionField.OpenSource -> OpenSourceFieldSpec,
      DescriptionField.Output -> OutputFieldSpec,
      DescriptionField.ParentAnalysis -> ParentAnalysisFieldSpec,
      DescriptionField.ParentJob -> ParentJobFieldSpec,
      DescriptionField.PriceComputedAt -> PriceComputedAtFieldSpec,
      DescriptionField.Project -> ProjectFieldSpec,
      DescriptionField.Properties -> PropertiesFieldSpec,
      DescriptionField.RegionalOptions -> RegionalOptionsFieldSpec,
      DescriptionField.RootExecution -> RootExecutionFieldSpec,
      DescriptionField.RunInput -> RunInputFieldSpec,
      DescriptionField.Stage -> StageFieldSpec,
      DescriptionField.Stages -> StagesFieldSpec,
      DescriptionField.State -> StateFieldSpec,
      DescriptionField.SubtotalPriceInfo -> SubtotalPriceInfoFieldSpec,
      DescriptionField.Tags -> TagsFieldSpec,
      DescriptionField.TotalPrice -> TotalPriceFieldSpec,
      DescriptionField.Version -> VersionFieldSpec,
      DescriptionField.Workflow -> WorkflowFieldSpec,
      DescriptionField.Workspace -> WorkspaceFieldSpec
  )
}

trait NewDxObjectDescribe {
  val id: String

  protected var cachedDescribeJs: Map[DescriptionField.DescriptionField, JsValue] = Map.empty
  protected var cachedDescribe: Map[DescriptionField.DescriptionField, Option[Any]] = Map.empty
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
        case Some(value: T) => Some(value)
        case None           => None
        case _              => throw new Exception(s"Invalid type for field ${field}")
      }
    } else if (cachedDescribeJs.contains(field)) {
      val value = cachedDescribeJs(field) match {
        case JsNull =>
          None
        case _ =>
          DescriptionFieldSpec.Specs(field) match {
            case spec: DescriptionFieldSpec[T] =>
              Some(spec.parse(cachedDescribeJs(field)))
            case _ =>
              throw new Exception(s"Invalid type for field ${field}")
          }
      }
      cachedDescribe += field -> value
      value
    } else {
      None
    }
  }

  def getOptionalField[T: ClassTag[T]](field: DescriptionField.DescriptionField): Option[T] = {
    if (!cachedDescribeJs.contains(field)) {
      describe(Set(field), includeDefaults = false)
    }
    getCachedField(field)
  }

  def getField[T: ClassTag[T]](field: DescriptionField.DescriptionField): T = {
    getOptionalField[T](field).get
  }
}

abstract class BaseDxObjectDescribe extends NewDxObjectDescribe {
  override protected val defaultFields: Set[DescriptionField.DescriptionField] = {
    super.defaultFields | Set(
        DescriptionField.Created,
        DescriptionField.Modified
    )
  }

  override protected val otherFields: Set[DescriptionField.DescriptionField] = Set(
      DescriptionField.Details
  )

  def created: Long = getField[Long](DescriptionField.Created)

  def modified: Long = getField[Long](DescriptionField.Modified)

  def details: Option[JsValue] = getOptionalField[JsValue](DescriptionField.Details)
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

  override protected val otherFields: Set[DescriptionField.DescriptionField] = {
    super.otherFields | Set(
        DescriptionField.Tags,
        DescriptionField.Properties
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

  def tags: Vector[String] = getField[Vector[String]](DescriptionField.Tags)

  protected def callAddTags(request: Map[String, JsValue]): Unit

  protected def callRemoveTags(request: Map[String, JsValue]): Unit

  /**
    * Sets tags on this object.
    * @param add new tags to add
    * @param remove tags to remove
    * @param reset whether to delete all existing tags before setting the new ones
    * @return the updated Vector of tags
    */
  def setTags(add: Vector[String] = Vector.empty,
              remove: Set[String] = Set.empty,
              reset: Boolean = false): Vector[String] = {
    val currentTags = tags
    val toRemove = remove | (if (reset) currentTags.toSet else Set.empty)
    val toAdd = add.filterNot(tag => currentTags.contains(tag) || remove.contains(tag))
    if (toAdd.nonEmpty) {
      val request = Map("tags" -> JsArray(toAdd.map(JsString(_))))
      callAddTags(request)
    }
    if (toRemove.nonEmpty) {
      val request = Map("tags" -> JsArray(toRemove.map(JsString(_)).toVector))
      callRemoveTags(request)
    }
    val newTags = (currentTags ++ toAdd).filterNot(toRemove.contains)
    cachedDescribeJs += DescriptionField.Tags -> JsArray(newTags.map(JsString(_)))
    cachedDescribe += DescriptionField.Tags -> Some(newTags)
    newTags
  }

  def properties: Map[String, String] = getField[Map[String, String]](DescriptionField.Properties)

  protected def callSetProperties(request: Map[String, JsValue]): Unit

  /**
    * Sets properties on this object
    * @param add keys to add/update
    * @param remove keys to remove
    * @param reset whether to delete all existing properties before setting the new ones
    * @return the updated properties
    */
  def setProperties(add: Map[String, String] = Map.empty,
                    remove: Set[String] = Set.empty,
                    reset: Boolean = false): Map[String, String] = {
    val currentProperties = properties
    val toRemove = remove | (if (reset) currentProperties.keySet else Set.empty)
    val toAddOrUpdate = add.filter {
      case (key, value) =>
        currentProperties.get(key) match {
          case Some(curValue) if value != curValue => true
          case None                                => true
          case _                                   => false
        }
    }
    val request = Map(
        "properties" -> JsObject(
            toAddOrUpdate.view.mapValues(JsString(_)).toMap ++ toRemove.map(_ -> JsNull).toMap
        )
    )
    callSetProperties(request)
    val newProperties = currentProperties.view
      .filterKeys(!currentProperties.contains(_))
      .toMap ++ toAddOrUpdate
    cachedDescribeJs += DescriptionField.Properties -> newProperties.view
      .mapValues(JsString(_))
      .toMap
    cachedDescribe += DescriptionField.Properties -> newProperties
    newProperties
  }
}
