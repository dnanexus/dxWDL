package dx.core.languages.wdl

import dx.api.{DiskType, InstanceTypeRequest}
import wdlTools.eval.WdlValues._
import wdlTools.eval.{
  Eval,
  EvalException,
  Hints,
  RuntimeAttributes,
  VBindings,
  WdlValueBindings,
  Runtime => WdlRuntime,
  RuntimeAttributes => WdlRuntimeAttributes,
  Utils => EUtils
}
import wdlTools.syntax.WdlVersion
import wdlTools.types.WdlTypes._
import wdlTools.types.{TypedAbstractSyntax => TAT}

object DxMetaHints {
  val ParameterMetaStream: String = "stream"
  val ParameterHintStream = "dx_stream"
}

abstract class DxRuntimeHint(val runtimeKey: Option[String],
                             val hintsKey: String,
                             val wdlTypes: Vector[T])

object Runtime {
  val MiB: Double = 1024 * 1024
  val GiB: Double = 1024 * 1024 * 1024
  val DxHintsKey = "dnanexus"
  case object InstanceType
      extends DxRuntimeHint(Some("dx_instance_type"), "instance_type", Vector(T_String))
}

case class Runtime[B <: VBindings[B]](wdlVersion: WdlVersion,
                                      runtimeSection: Option[TAT.RuntimeSection],
                                      hintsSection: Option[TAT.MetaSection],
                                      evaluator: Eval,
                                      defaultAttrs: VBindings[B],
                                      ctx: Option[WdlValueBindings] = None) {
  private lazy val runtimeAttrs: WdlRuntimeAttributes[B] = {
    val runtime = runtimeSection.map(r => WdlRuntime.create(Some(r), evaluator, ctx))
    val hints = hintsSection.map(h => Hints.create(Some(h)))
    RuntimeAttributes[B](runtime, hints, defaultAttrs)
  }

  def get(id: String): Option[V] = runtimeAttrs.get(id)

  // This is the parent key for the object containing all DNAnexus-specific
  // options in the hints section (2.0 and later)
  private lazy val dxHints: ValueMap = wdlVersion match {
    case WdlVersion.V2 if runtimeAttrs.contains(Runtime.DxHintsKey) =>
      runtimeAttrs.get(Runtime.DxHintsKey) match {
        case Some(V_Object(fields)) =>
          ValueMap(fields)
        case other =>
          throw new Exception(s"Invalid value for hints.${Runtime.DxHintsKey} ${other}")
      }
    case _ => ValueMap.empty
  }

  def getDxHint(hint: DxRuntimeHint): Option[V] = {
    wdlVersion match {
      case WdlVersion.V2 if dxHints.contains(hint.hintsKey) =>
        dxHints.get(hint.hintsKey, hint.wdlTypes)
      case _ if hint.runtimeKey.isDefined =>
        runtimeAttrs.get(hint.runtimeKey.get, hint.wdlTypes)
      case _ =>
        None
    }
  }

  def containerDefined: Boolean = {
    runtimeAttrs.contains(WdlRuntime.Keys.Docker)
  }

  def container: Vector[String] = {
    runtimeAttrs.runtime.map(_.container).getOrElse(Vector.empty)
  }

  def parseInstanceType: InstanceTypeRequest = {
    getDxHint(Runtime.InstanceType) match {
      case None => ()
      case Some(V_String(dxInstanceType)) =>
        return InstanceTypeRequest(Some(dxInstanceType))
      case other => throw new Exception(s"Invalid dxInstanceType ${other}")
    }

    val memory = runtimeAttrs.runtime.flatMap(_.memory)
    val memoryMB = memory.map(mem => EUtils.floatToInt(mem.toDouble / Runtime.MiB))
    // we don't provide multiple disk mounts - instead we just add up all the
    // requested disk space
    val diskGB =
      runtimeAttrs.runtime.map(r => EUtils.floatToInt(r.disks.map(_.size).sum / Runtime.GiB))
    val diskType =
      runtimeAttrs.runtime.map(_.disks.flatMap(_.diskType.map(_.toUpperCase)).distinct) match {
        case Some(Vector(diskType))       => Some(DiskType.withNameIgnoreCase(diskType))
        case Some(v) if v.contains("SSD") => Some(DiskType.SSD)
        case _                            => None
      }
    val cpu = runtimeAttrs.runtime.flatMap(_.cpu).map(EUtils.floatToInt)
    val gpu = runtimeAttrs.get(WdlRuntime.Keys.Gpu, Vector(T_Boolean)) match {
      case None               => None
      case Some(V_Boolean(b)) => Some(b)
    }
    InstanceTypeRequest(None, memoryMB, diskGB, diskType, cpu, gpu)
  }

  def safeParseInstanceType: Option[InstanceTypeRequest] = {
    try {
      Some(parseInstanceType)
    } catch {
      case _: EvalException =>
        // The generated code will need to calculate the instance type at runtime
        None
    }
  }
}
