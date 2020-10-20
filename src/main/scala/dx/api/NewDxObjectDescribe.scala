package dx.api

import dx.AppInternalException
import spray.json._
import wdlTools.util.{Enum, JsUtils}
import wdlTools.util.CollectionUtils.IterableExtensions

import scala.reflect.ClassTag

object ExecutionState extends Enum {
  type ExecutionState = Value
  val Runnable, In_Progress, Partially_Failed, Done, Failed, Terminating, Terminated = Value
}

object ObjectState extends Enum {
  type ObjectState = Value
  val Open, Closed = Value
}

case class AppletCreatedBy(userId: String, jobId: Option[String], executableID: Option[String])

case class DnsConfig(hostname: String)

case class HttpsAppInfo(sharedAccess: String, ports: Vector[Long], dns: DnsConfig)

case class PricingPolicy(unit: String, unitPrice: Double)

case class RegionOptions(region: String,
                         appletId: String,
                         resourcesId: String,
                         pricingPolicy: Option[PricingPolicy])

case class SubtotalPriceInfo(subtotalPrice: Double, priceComputedAt: Long)

object DescriptionField extends Enum {
  type DescriptionField = Value
  // format: off
  val Access, Aliases, Analysis, AppCreatedBy, AppletCreatedBy, AuthorizedUsers, BillTo, Categories,
      Class, Created, DelayWorkspaceDestruction, Deleted, Description, Details, DeveloperNotes, DxApiVersion,
      Executable, ExecutableName, ExecutionState, Folder, Hidden, HttpsApp, Id, IgnoreReuse, Input, InputSpec,
      Installed, Installs,  IsDeveloperFor, LaunchedBy, LineItemPerTest, Links, Modified, Name, ObjectState,
      OpenSource, OriginalInput, Output, OutputSpec, ParentAnalysis, ParentJob, PriceComputedAt, Project,
      Properties, Published, RegionalOptions, RootExecution, RunInput, RunSpec, Sponsored, SponsoredUntil,
      Stage, Stages, SubtotalPriceInfo, Summary, Tags, Title, TotalPrice, Types, Version, Workflow,
      Workspace = Value
  // format: on
}

trait DescriptionFieldSpec[T] {
  val key: String

  def parse(jsValue: JsValue): T
}

case class JsValueFieldSpec(key: String) extends DescriptionFieldSpec[JsValue] {
  def parse(jsValue: JsValue): JsValue = jsValue
}

case class JsMapFieldSpec(key: String) extends DescriptionFieldSpec[Map[String, JsValue]] {
  def parse(jsValue: JsValue): Map[String, JsValue] = {
    JsUtils.getFields(jsValue)
  }
}

case class OptionalJsMapFieldSpec(key: String)
    extends DescriptionFieldSpec[Option[Map[String, JsValue]]] {
  def parse(jsValue: JsValue): Option[Map[String, JsValue]] = {
    jsValue match {
      case JsNull => None
      case _      => Some(JsUtils.getFields(jsValue))
    }
  }
}

case class StringFieldSpec(key: String) extends DescriptionFieldSpec[String] {
  def parse(jsValue: JsValue): String = {
    JsUtils.getString(jsValue)
  }
}

case class OptionalStringFieldSpec(key: String) extends DescriptionFieldSpec[Option[String]] {
  def parse(jsValue: JsValue): Option[String] = {
    jsValue match {
      case JsNull => None
      case _      => Some(JsUtils.getString(jsValue))
    }
  }
}

case class StringArrayFieldSpec(key: String) extends DescriptionFieldSpec[Vector[String]] {
  def parse(jsValue: JsValue): Vector[String] = {
    JsUtils.getValues(jsValue).map(JsUtils.getString(_))
  }
}

case class StringMapFieldSpec(key: String) extends DescriptionFieldSpec[Map[String, String]] {
  def parse(jsValue: JsValue): Map[String, String] = {
    JsUtils.getFields(jsValue).view.mapValues(JsUtils.getString(_)).toMap
  }
}

case class BooleanFieldSpec(key: String) extends DescriptionFieldSpec[Boolean] {
  def parse(jsValue: JsValue): Boolean = {
    JsUtils.getBoolean(jsValue)
  }
}

case class LongFieldSpec(key: String) extends DescriptionFieldSpec[Long] {
  def parse(jsValue: JsValue): Long = {
    JsUtils.getLong(jsValue)
  }
}

case class DoubleFieldSpec(key: String) extends DescriptionFieldSpec[Double] {
  def parse(jsValue: JsValue): Double = {
    JsUtils.getDouble(jsValue)
  }
}

object AppletCreatedByFieldSpec extends DescriptionFieldSpec[AppletCreatedBy] {
  override val key: String = "createdBy"
  override def parse(jsValue: JsValue): AppletCreatedBy = {
    val fields = JsUtils.getFields(jsValue)
    AppletCreatedBy(
        JsUtils.getString(fields("user")),
        fields.get("job").map(JsUtils.getString(_)),
        fields.get("executable").map(JsUtils.getString(_))
    )
  }
}

case class EnumFieldSpec[T <: Enum](key: String, resolve: String => T)
    extends DescriptionFieldSpec[T] {
  def parse(jsValue: JsValue): T = {
    val strValue = JsUtils.getString(jsValue, Some(key))
    resolve(strValue)
  }
}

object HttpsAppFieldSpec extends DescriptionFieldSpec[HttpsAppInfo] {
  override val key: String = "httpsApp"
  override def parse(jsValue: JsValue): HttpsAppInfo = {
    val fields = JsUtils.getFields(jsValue)
    val dnsConfig = DnsConfig(JsUtils.getString(fields("dns"), Some("hostname")))
    HttpsAppInfo(
        JsUtils.getString(fields("shared_access")),
        JsUtils.getValues(fields("ports")).map(JsUtils.getLong(_)),
        dnsConfig
    )
  }
}

object RegionalOptionsFieldSpec extends DescriptionFieldSpec[Vector[RegionOptions]] {
  override val key: String = "regionalOptions"
  override def parse(jsValue: JsValue): Vector[RegionOptions] = {
    jsValue.asJsObject.fields.map {
      case (region, JsObject(options)) =>
        val pricingPolicy = options.get("pricingPolicy").map { obj =>
          obj.asJsObject.getFields("unit", "unitPrice") match {
            case Seq(JsString(unit), JsNumber(price)) =>
              PricingPolicy(unit, price.toDouble)
          }
        }
        RegionOptions(
            region,
            JsUtils.getString(options("applet")),
            JsUtils.getString(options("resources")),
            pricingPolicy
        )
    }
  }.toVector
}

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

object SubtotalPriceInfoFieldSpec extends DescriptionFieldSpec[SubtotalPriceInfo] {
  override val key = "subtotalPriceInfo"
  override def parse(jsValue: JsValue): SubtotalPriceInfo = {
    val subtotalPrice = JsUtils.getDouble(jsValue, Some("subtotalPrice"))
    val priceComputedAt = JsUtils.getLong(jsValue, Some("priceComputedAt"))
    SubtotalPriceInfo(subtotalPrice, priceComputedAt)
  }
}

object WorkflowFieldSpec extends DescriptionFieldSpec[String] {
  override val key: String = "workflow"
  override def parse(jsValue: JsValue): String = {
    JsUtils.getString(jsValue, Some("id"))
  }
}

object DescriptionFieldSpec {
  val Specs: Map[DescriptionField.DescriptionField, DescriptionFieldSpec[_]] = Map(
      DescriptionField.Access -> JsValueFieldSpec("access"),
      DescriptionField.Aliases -> StringArrayFieldSpec("aliases"),
      DescriptionField.Analysis -> OptionalStringFieldSpec("analysis"),
      DescriptionField.AppCreatedBy -> StringFieldSpec("createdBy"),
      DescriptionField.AppletCreatedBy -> AppletCreatedByFieldSpec,
      DescriptionField.AuthorizedUsers -> StringArrayFieldSpec("authorizedUsers"),
      DescriptionField.BillTo -> StringFieldSpec("billTo"),
      DescriptionField.Categories -> StringArrayFieldSpec("categories"),
      DescriptionField.Class -> StringFieldSpec("class"),
      DescriptionField.Created -> LongFieldSpec("created"),
      DescriptionField.DelayWorkspaceDestruction -> BooleanFieldSpec("delayWorkspaceDestruction"),
      DescriptionField.Deleted -> BooleanFieldSpec("deleted"),
      DescriptionField.Description -> StringFieldSpec("description"),
      DescriptionField.Details -> JsValueFieldSpec("details"),
      DescriptionField.DeveloperNotes -> StringFieldSpec("developerNotes"),
      DescriptionField.DxApiVersion -> StringFieldSpec("dxApi"),
      DescriptionField.Executable -> StringFieldSpec("executable"),
      DescriptionField.ExecutableName -> StringFieldSpec("executableName"),
      DescriptionField.ExecutionState -> EnumFieldSpec[ExecutionState.ExecutionState](
          "state",
          ExecutionState.withNameIgnoreCase
      ),
      DescriptionField.Folder -> StringFieldSpec("folder"),
      DescriptionField.Hidden -> BooleanFieldSpec("hidden"),
      DescriptionField.HttpsApp -> HttpsAppFieldSpec,
      DescriptionField.Id -> StringFieldSpec("id"),
      DescriptionField.IgnoreReuse -> BooleanFieldSpec("ignoreReuse"),
      DescriptionField.Input -> JsMapFieldSpec("input"),
      DescriptionField.InputSpec -> JsValueFieldSpec("inputSpec"),
      DescriptionField.Installed -> BooleanFieldSpec("installed"),
      DescriptionField.Installs -> LongFieldSpec("installs"),
      DescriptionField.IsDeveloperFor -> BooleanFieldSpec("isDeveloperFor"),
      DescriptionField.LaunchedBy -> StringFieldSpec("launchedBy"),
      DescriptionField.LineItemPerTest -> BooleanFieldSpec("lineItemPerTest"),
      DescriptionField.Links -> StringArrayFieldSpec("links"),
      DescriptionField.Modified -> LongFieldSpec("modified"),
      DescriptionField.Name -> StringFieldSpec("name"),
      DescriptionField.ObjectState -> EnumFieldSpec[ObjectState.ObjectState](
          "state",
          ObjectState.withNameIgnoreCase
      ),
      DescriptionField.OpenSource -> BooleanFieldSpec("openSource"),
      DescriptionField.OriginalInput -> JsMapFieldSpec("originalInput"),
      DescriptionField.Output -> OptionalJsMapFieldSpec("output"),
      DescriptionField.OutputSpec -> JsValueFieldSpec("outputSpec"),
      DescriptionField.ParentAnalysis -> OptionalStringFieldSpec("parentAnalysis"),
      DescriptionField.ParentJob -> OptionalStringFieldSpec("parentJob"),
      DescriptionField.PriceComputedAt -> LongFieldSpec("priceComputedAt"),
      DescriptionField.Project -> StringFieldSpec("project"),
      DescriptionField.Properties -> StringMapFieldSpec("properties"),
      DescriptionField.Published -> BooleanFieldSpec("published"),
      DescriptionField.RegionalOptions -> RegionalOptionsFieldSpec,
      DescriptionField.RootExecution -> StringFieldSpec("rootExecution"),
      DescriptionField.RunInput -> JsMapFieldSpec("runInput"),
      DescriptionField.RunSpec -> JsValueFieldSpec("runSpec"),
      DescriptionField.Sponsored -> BooleanFieldSpec("sponsored"),
      DescriptionField.SponsoredUntil -> LongFieldSpec("sponsoredUntil"),
      DescriptionField.Stage -> OptionalStringFieldSpec("stage"),
      DescriptionField.Stages -> StagesFieldSpec,
      DescriptionField.SubtotalPriceInfo -> SubtotalPriceInfoFieldSpec,
      DescriptionField.Summary -> StringFieldSpec("summary"),
      DescriptionField.Tags -> StringArrayFieldSpec("tags"),
      DescriptionField.Title -> StringFieldSpec("title"),
      DescriptionField.TotalPrice -> LongFieldSpec("totalPrice"),
      DescriptionField.Types -> StringArrayFieldSpec("types"),
      DescriptionField.Version -> StringFieldSpec("version"),
      DescriptionField.Workflow -> WorkflowFieldSpec,
      DescriptionField.Workspace -> StringFieldSpec("workspace")
  )
  // Make sure we didn't forget to add any specs
  assert(DescriptionField.values.forall(Specs.contains))
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

trait NewDxApplication {
  val dxProject: Option[DxProject] = None
  val dxApi: DxApi

  protected def callRun(request: Map[String, JsValue]): JsObject

  def newRun(name: String,
             input: JsValue,
             instanceType: Option[String] = None,
             project: Option[DxProject] = None,
             folder: String = "/",
             dependsOn: Vector[String] = Vector.empty,
             tags: Vector[String] = Vector.empty,
             properties: Map[String, String] = Map.empty,
             details: Option[JsValue] = None,
             systemRequirements: Option[JsValue] = None,
             executionPolicy: Option[JsValue] = None,
             timeoutPolicyByExecutable: Option[JsValue] = None,
             delayWorkspaceDestruction: Option[Boolean] = None,
             allowSsh: Vector[String] = Vector.empty,
             debugOn: Vector[String] = Vector.empty,
             singleContext: Option[Boolean] = None,
             ignoreReuse: Option[Boolean] = None,
             nonce: Option[String] = None): DxJob = {
    val requiredFields = Map(
        "name" -> JsString(name),
        "input" -> input,
        "folder" -> JsString(folder)
    )
    val systemRequirementsFields =
      if (systemRequirements.isDefined) {
        Map("systemRequirements" -> systemRequirements.get)
      } else if (instanceType.isDefined) {
        Map(
            "systemRequirements" -> JsObject(
                "main" -> JsObject("instanceType" -> JsString(instanceType.get))
            )
        )
      } else {
        Map.empty
      }
    val optionalFields = Vector(
        project.orElse(dxProject).map(proj => "project" -> JsString(proj.id)),
        dependsOn.asOption.map(d => "dependsOn" -> JsArray(d.iterator.map(JsString(_)).toVector)),
        details.map(d => "details" -> d),
        executionPolicy.map(ep => "executionPolicy" -> ep),
        timeoutPolicyByExecutable.map(ep => "timeoutPolicyByExecutable" -> ep),
        delayWorkspaceDestruction.map(dwd => "delayWorkspaceDestruction" -> JsBoolean(dwd)),
        tags.asOption.map(t => "tags" -> JsArray(t.iterator.map(JsString(_)).toVector)),
        properties.asOption.map(p =>
          "properties" -> JsObject(p.iterator.map {
            case (key, value) => key -> JsString(value)
          }.toMap)
        ),
        allowSsh.asOption.map(ssh => "allowSSH" -> JsArray(ssh.iterator.map(JsString(_)).toVector)),
        debugOn.asOption.map(d =>
          "debug" -> JsObject("debugOn" -> JsArray(d.iterator.map(JsString(_)).toVector))
        ),
        singleContext.map(sc => "singleContext" -> JsBoolean(sc)),
        ignoreReuse.map(ir => "ignoreReuse" -> JsBoolean(ir)),
        nonce.map(n => "nonce" -> JsString(n))
    ).flatten.toMap
    val request = requiredFields ++ systemRequirementsFields ++ optionalFields
    val response = callRun(request)
    val jobId: String = response.fields.get("id") match {
      case Some(JsString(x)) => x
      case _ =>
        throw new AppInternalException(s"Bad format returned from jobNew ${response.prettyPrint}")
    }
    dxApi.job(jobId)
  }
}
