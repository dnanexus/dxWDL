package dx.compiler.wdl

import dx.api.{DxApi, DxPath, DxUtils, InstanceTypeRequest}
import dx.compiler.ir.RunSpec._
import dx.compiler.ir.{ExecutableKind, ExecutableKindNative, ExecutableType}
import dx.core.ir.Value
import dx.core.languages.wdl
import dx.core.languages.wdl.{DxRuntimeHint, Runtime}
import wdlTools.eval.WdlValues._
import wdlTools.eval.{Eval, EvalException, Hints, Meta}
import wdlTools.syntax.WdlVersion
import wdlTools.types.WdlTypes._
import wdlTools.types.{TypedAbstractSyntax => TAT}

import scala.util.matching.Regex

/**
  * A unification of WDL Runtime and Hints, with version-specific support.
  */
object RuntimeTranslator {
  // This is the parent key for the object containing all DNAnexus-specific
  // options in the hints section (2.0 and later)
  val DxKey = "dnanexus"
  // dx-specific keys that are used in meta
  val ExecutableType = "type"
  val ExecutableId = "id"
  val ExecutableTypeNative = "native"
  val AppName = "name"
  val AppletProject = "project"
  val AppletPath = "path"

  case object Access
      extends DxRuntimeHint(
          Some("dx_access"),
          "access",
          Vector(T_Object)
      )
  case object App
      extends DxRuntimeHint(
          None,
          "app",
          Vector(T_String, T_Object)
      )
  case object IgnoreReuse
      extends DxRuntimeHint(
          Some("dx_ignore_reuse"),
          "ignore_reuse",
          Vector(T_Boolean)
      )
  case object Restart
      extends DxRuntimeHint(
          Some("dx_restart"),
          "restart",
          Vector(T_Int, T_Object)
      )
  case object Stream
      extends DxRuntimeHint(
          Some("dx_stream"),
          "stream",
          Vector(T_Boolean)
      )
  case object Timeout
      extends DxRuntimeHint(
          Some("dx_timeout"),
          "timeout",
          Vector(T_String, T_Object)
      )
  // case object Regions
  // This key is used in the restart object value to represent "*"
  val AllKey = "All"
}

case class RuntimeTranslator(wdlVersion: WdlVersion,
                             runtimeSection: Option[TAT.RuntimeSection],
                             hintsSection: Option[TAT.MetaSection],
                             metaSection: Option[TAT.MetaSection],
                             defaultAttrs: Map[String, V],
                             evaluator: Eval,
                             dxApi: DxApi = DxApi.get) {
  private lazy val runtime =
    Runtime(wdlVersion, runtimeSection, hintsSection, evaluator, defaultAttrs)
  private lazy val meta: Meta = Meta.create(wdlVersion, metaSection)

  def translate(id: String, wdlType: Option[T] = None): Option[Value] = {
    try {
      (runtime.get(id), wdlType) match {
        case (Some(value), None) =>
          Some(wdl.Utils.toIRValue(value))
        case (Some(value), Some(t)) =>
          Some(wdl.Utils.toIRValue(value, t))
      }
    } catch {
      case _: EvalException =>
        // the value is an expression that requires evaluation at runtime
        None
    }
  }

  def translateExecutableKind: Option[ExecutableKind] = {
    def kindFromId(id: String): Option[ExecutableKind] = {
      val (executableType, _) = DxUtils.parseDataObjectId(id)
      Some(ExecutableKindNative(ExecutableType.withNameIgnoreCase(executableType), Some(id)))
    }
    if (wdlVersion >= WdlVersion.V2) {
      runtime.getDxHint(RuntimeTranslator.App) match {
        case None => ()
        case Some(V_String(id)) =>
          return try {
            kindFromId(id)
          } catch {
            case _: dx.IllegalArgumentException =>
              if (id.startsWith("/")) {
                Some(ExecutableKindNative(ExecutableType.Applet, path = Some(id)))
              } else {
                Some(ExecutableKindNative(ExecutableType.App, name = Some(id)))
              }
          }
        case Some(V_Object(fields)) =>
          def getStringField(name: String): Option[String] = {
            fields.get(name) match {
              case Some(V_String(s)) => Some(s)
              case None              => None
              case other             => throw new Exception(s"Invalid ${name} ${other}")
            }
          }
          val id = getStringField(RuntimeTranslator.ExecutableId)
          val name = getStringField(RuntimeTranslator.AppName)
          val project = getStringField(RuntimeTranslator.AppletProject)
          val path = getStringField(RuntimeTranslator.AppletPath)
          getStringField(RuntimeTranslator.ExecutableType) match {
            case Some(executableType) =>
              Some(
                  ExecutableKindNative(
                      ExecutableType.withNameIgnoreCase(executableType),
                      id,
                      name,
                      project,
                      path
                  )
              )
            case None if name.isDefined =>
              Some(ExecutableKindNative(ExecutableType.App, id, name))
            case None if project.isDefined || path.isDefined =>
              Some(ExecutableKindNative(ExecutableType.Applet, id, project = project, path = path))
            case None if id.isDefined =>
              kindFromId(id.get)
            case _ =>
              throw new Exception("Not enough information to determine native app(let)")
          }
      }
    }
    (meta.get(RuntimeTranslator.ExecutableType), meta.get(RuntimeTranslator.ExecutableId)) match {
      case (Some(V_String(RuntimeTranslator.ExecutableTypeNative)), Some(V_String(id))) =>
        kindFromId(id)
      case _ => None
    }
  }

  def translateInstanceType: InstanceType = {
    runtime.parseInstanceType match {
      case None => DynamicInstanceType
      case Some(InstanceTypeRequest(None, None, None, None, None, None)) =>
        DefaultInstanceType
      case Some(InstanceTypeRequest(dxInstanceType, memoryMB, diskGB, diskType, cpu, gpu)) =>
        StaticInstanceType(dxInstanceType, memoryMB, diskGB, diskType, cpu, gpu)
    }
  }

  def translateContainer: ContainerImage = {
    if (!runtime.containerDefined) {
      return NoImage
    }
    val img =
      try {
        // try to find a Docker image specified as a dx URL
        // there will be an exception if the value requires
        // evaluation at runtime
        runtime.container.collectFirst {
          case uri if uri.startsWith(DxPath.DxUriPrefix) =>
            val dxfile = dxApi.resolveDxUriFile(uri)
            DxFileDockerImage(uri, dxfile)
        }
      } catch {
        case _: EvalException => None
      }
    img.getOrElse(NetworkDockerImage)
  }

  private def unwrapString(value: V): String = {
    value match {
      case V_String(s) => s
      case _           => throw new Exception(s"Expected V_Array(V_String), got ${value}")
    }
  }

  private def unwrapStringArray(value: V): Vector[String] = {
    value match {
      case V_Array(a) => a.map(unwrapString)
      case _          => throw new Exception(s"Expected V_String, got ${value}")
    }
  }

  private def unwrapBoolean(value: V): Boolean = {
    value match {
      case V_Boolean(b) => b
      case _            => throw new Exception(s"Expected V_Boolean, got ${value}")
    }
  }

  private def unwrapInt(value: V): Long = {
    value match {
      case V_Int(i) => i
      case _        => throw new Exception(s"Expected V_Int, got ${value}")
    }
  }

  def translateAccess: Option[AccessRequirement] = {
    runtime.getDxHint(RuntimeTranslator.Access).map {
      case V_Object(fields) =>
        AccessRequirement(
            network = fields.get("network").map(unwrapStringArray).getOrElse(Vector.empty),
            project = fields.get("project").map(unwrapString),
            allProjects = fields.get("allProjects").map(unwrapString),
            developer = fields.get("developer").map(unwrapBoolean),
            projectCreation = fields.get("projectCreation").map(unwrapBoolean)
        )
    }
  }

  def translateIgnoreReuse: Option[IgnoreReuseRequirement] = {
    runtime.getDxHint(RuntimeTranslator.IgnoreReuse).map {
      case V_Boolean(ignoreReuse) => IgnoreReuseRequirement(ignoreReuse)
    }
  }

  def translateRestart: Option[RestartRequirement] = {
    runtime.getDxHint(RuntimeTranslator.Restart).map {
      case V_Int(n) => RestartRequirement(default = Some(n))
      case V_Object(fields) =>
        RestartRequirement(
            fields.get("max").map(unwrapInt),
            fields.get("default").map(unwrapInt),
            fields
              .get("errors")
              .map {
                case V_Object(errFields) =>
                  errFields.map {
                    case (s, V_Int(i)) => s -> i
                    case other =>
                      throw new Exception(s"Invalid restart map entry ${other}")
                  }
                case _ => throw new Exception("Invalid restart map")
              }
              .getOrElse(Map.empty)
        )
    }
  }

  private val durationRegexp = s"^(?:(\\d+)D)?(?:(\\d+)H)?(?:(\\d+)M)?".r
  //val durationFields = Vector("days", "hours", "minutes")

  private def parseDuration(duration: String): TimeoutRequirement = {
    durationRegexp.findFirstMatchIn(duration) match {
      case Some(result: Regex.Match) =>
        def group(i: Int): Option[Long] = {
          result.group(i) match {
            case null      => None
            case s: String => Some(s.toLong)
          }
        }
        TimeoutRequirement(group(1), group(2), group(3))
      case _ => throw new Exception(s"Invalid ISO Duration ${duration}")
    }
  }

  def translateTimeout: Option[TimeoutRequirement] = {
    runtime.getDxHint(RuntimeTranslator.Restart).map {
      case V_String(s) => parseDuration(s)
      case V_Object(fields) =>
        TimeoutRequirement(
            fields.get("days").map(unwrapInt),
            fields.get("hours").map(unwrapInt),
            fields.get("minutes").map(unwrapInt)
        )
    }
  }

  def translateStream: Option[StreamingRequirement] = {
    runtime
      .get(Hints.Keys.LocalizationOptional)
      .orElse(runtime.getDxHint(RuntimeTranslator.Stream))
      .map {
        case V_Boolean(b) => StreamingRequirement(b)
      }
  }

  def translateRequirements: Vector[Requirement] = {
    Vector(
        translateAccess,
        translateIgnoreReuse,
        translateRestart,
        translateTimeout,
        translateStream
    ).flatten
  }
}
