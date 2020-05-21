package dxWDL.compiler

import cats.data.Validated.{Invalid, Valid}
import common.validation.ErrorOr.ErrorOr
import wom.callable.CallableTaskDefinition
import wom.callable.Callable._
import wom.callable.MetaValueElement._
import wom.callable.MetaValueElement
import wom.expression.{ValueAsAnExpression, WomExpression}
import wom.types._
import wom.values._

import dxWDL.base._
import dxWDL.dx._
import dxWDL.util._
import IR.CVar

import scala.util.matching.Regex

case class GenerateIRTask(verbose: Verbose,
                          typeAliases: Map[String, WdlTypes.T],
                          language: Language.Value,
                          defaultRuntimeAttrs: WdlRuntimeAttrs) {
  val verbose2: Boolean = verbose.containsKey("GenerateIR")

  private class DynamicInstanceTypesException private (ex: Exception) extends RuntimeException(ex) {
    def this() = this(new RuntimeException("Runtime instance type calculation required"))
  }

  def evalWomExpression(expr: WomExpression): WdlValues.V = {
    val result: ErrorOr[WdlValues.V] =
      expr.evaluateValue(Map.empty[String, WdlValues.V], wom.expression.NoIoFunctionSet)
    result match {
      case Invalid(_)         => throw new DynamicInstanceTypesException()
      case Valid(x: WdlValues.V) => x
    }
  }

  // Figure out which instance to use.
  //
  // Extract three fields from the task:
  // RAM, disk space, and number of cores. These are WDL expressions
  // that, in the general case, could be calculated only at runtime.
  // At compile time, constants expressions are handled. Some can
  // only be evaluated at runtime.
  private def calcInstanceType(task: CallableTaskDefinition): IR.InstanceType = {
    def evalAttr(attrName: String): Option[WdlValues.V] = {
      task.runtimeAttributes.attributes.get(attrName) match {
        case None =>
          // Check the overall defaults, there might be a setting over there
          defaultRuntimeAttrs.m.get(attrName)
        case Some(expr) => Some(evalWomExpression(expr))
      }
    }

    try {
      val dxInstanceType = evalAttr(IR.HINT_INSTANCE_TYPE)
      val memory = evalAttr("memory")
      val diskSpace = evalAttr("disks")
      val cores = evalAttr("cpu")
      val gpu = evalAttr("gpu")
      val iTypeDesc = InstanceTypeDB.parse(dxInstanceType, memory, diskSpace, cores, gpu)
      IR.InstanceTypeConst(iTypeDesc.dxInstanceType,
                           iTypeDesc.memoryMB,
                           iTypeDesc.diskGB,
                           iTypeDesc.cpu,
                           iTypeDesc.gpu)
    } catch {
      case e: DynamicInstanceTypesException =>
        // The generated code will need to calculate the instance type at runtime
        IR.InstanceTypeRuntime
    }
  }

  // Process a docker image, if there is one
  def triageDockerImage(dockerExpr: Option[WomExpression]): IR.DockerImage = {
    dockerExpr match {
      case None =>
        IR.DockerImageNone
      case Some(expr) if WdlValues.VAnalysis.isExpressionConst(WdlTypes.T_String, expr) =>
        val wdlConst = WdlValues.VAnalysis.evalConst(WdlTypes.T_String, expr)
        wdlConst match {
          case WdlValues.V_String(url) if url.startsWith(Utils.DX_URL_PREFIX) =>
            // A constant image specified with a DX URL
            val dxfile = DxPath.resolveDxURLFile(url)
            IR.DockerImageDxFile(url, dxfile)
          case _ =>
            // Probably a public docker image
            IR.DockerImageNetwork
        }
      case _ =>
        // Image will be downloaded from the network
        IR.DockerImageNetwork
    }
  }

  private def unwrapTaskMeta(
      meta: Map[String, MetaValueElement],
      adjunctFiles: Option[Vector[Adjuncts.AdjunctFile]]
  ): Vector[IR.TaskAttr] = {
    val appAttrs = meta.flatMap {
      case (IR.META_TITLE, MetaValueElementString(text))       => Some(IR.TaskAttrTitle(text))
      case (IR.META_DESCRIPTION, MetaValueElementString(text)) => Some(IR.TaskAttrDescription(text))
      case (IR.META_SUMMARY, MetaValueElementString(text))     => Some(IR.TaskAttrSummary(text))
      case (IR.META_DEVELOPER_NOTES, MetaValueElementString(text)) =>
        Some(IR.TaskAttrDeveloperNotes(text))
      case (IR.META_VERSION, MetaValueElementString(text))   => Some(IR.TaskAttrVersion(text))
      case (IR.META_DETAILS, MetaValueElementObject(fields)) => Some(IR.TaskAttrDetails(fields))
      case (IR.META_OPEN_SOURCE, MetaValueElementBoolean(b)) => Some(IR.TaskAttrOpenSource(b))
      case (IR.META_CATEGORIES, MetaValueElementArray(array)) =>
        Some(IR.TaskAttrCategories(array.map {
          case MetaValueElementString(text) => text
          case other                        => throw new Exception(s"Invalid category: ${other}")
        }))
      case (IR.META_TYPES, MetaValueElementArray(array)) =>
        Some(IR.TaskAttrTypes(array.map {
          case MetaValueElementString(text) => text
          case other                        => throw new Exception(s"Invalid type: ${other}")
        }))
      case (IR.META_TAGS, MetaValueElementArray(array)) =>
        Some(IR.TaskAttrTags(array.map {
          case MetaValueElementString(text) => text
          case other                        => throw new Exception(s"Invalid tag: ${other}")
        }))
      case (IR.META_PROPERTIES, MetaValueElementObject(fields)) =>
        Some(IR.TaskAttrProperties(fields.mapValues {
          case MetaValueElementString(text) => text
          case other                        => throw new Exception(s"Invalid property value: ${other}")
        }))
      case _ => None
    }.toVector

    // Fill in missing attributes from adjunct files
    appAttrs ++ (adjunctFiles match {
      case Some(adj) =>
        adj.flatMap {
          case Adjuncts.Readme(text) if !meta.contains(IR.META_DESCRIPTION) =>
            Some(IR.TaskAttrDescription(text))
          case Adjuncts.DeveloperNotes(text) if !meta.contains(IR.META_DEVELOPER_NOTES) =>
            Some(IR.TaskAttrDeveloperNotes(text))
          case _ => None
        }.toVector
      case None => Vector.empty
    })
  }

  private def unwrapWomString(value: WdlValues.V): String = {
    value match {
      case WdlValues.V_String(s) => s
      case _            => throw new Exception("Expected WdlValues.V_String")
    }
  }

  private def unwrapWomStringArray(array: WdlValues.V): Vector[String] = {
    array match {
      case WdlValues.V_Array(WomMaybeEmptyArrayType(WdlTypes.T_String), strings) =>
        strings.map(unwrapWdlValues.V_String).toVector
      case _ => throw new Exception("Expected WdlValues.V_Array")
    }
  }

  private def unwrapWomBoolean(value: WdlValues.V): Boolean = {
    value match {
      case WdlValues.V_Boolean(b) => b
      case _             => throw new Exception("Expected WdlValues.V_Boolean")
    }
  }

  private def unwrapWomInteger(value: WomValue): Int = {
    value match {
      case WdlValues.V_Int(i) => i
      case _             => throw new Exception("Expected WdlValues.V_Int")
    }
  }

  val durationRegexp = s"^(?:(\\d+)D)?(?:(\\d+)H)?(?:(\\d+)M)?".r
  val durationFields = Vector("days", "hours", "minutes")

  private def parseDuration(duration: String): IR.RuntimeHintTimeout = {
    durationRegexp.findFirstMatchIn(duration) match {
      case Some(result: Regex.Match) =>
        def group(i: Int): Option[Int] = {
          result.group(i) match {
            case null      => None
            case s: String => Some(s.toInt)
          }
        }
        IR.RuntimeHintTimeout(group(1), group(2), group(3))
      case _ => throw new Exception(s"Invalid ISO Duration ${duration}")
    }
  }

  private def unwrapRuntimeHints(hints: Map[String, WomExpression]): Vector[IR.RuntimeHint] = {
    val hintKeys = Set(IR.HINT_ACCESS, IR.HINT_IGNORE_REUSE, IR.HINT_RESTART, IR.HINT_TIMEOUT)

    hints
      .filterKeys(hintKeys)
      .mapValues(evalWomExpression)
      .flatMap {
        case (IR.HINT_ACCESS, WomObject(values, _)) =>
          Some(
              IR.RuntimeHintAccess(
                  network = values.get("network").map(unwrapWdlValues.V_StringArray),
                  project = values.get("project").map(unwrapWdlValues.V_String),
                  allProjects = values.get("allProjects").map(unwrapWdlValues.V_String),
                  developer = values.get("developer").map(unwrapWdlValues.V_Boolean),
                  projectCreation = values.get("projectCreation").map(unwrapWdlValues.V_Boolean)
              )
          )
        case (IR.HINT_IGNORE_REUSE, WdlValues.V_Boolean(b)) => Some(IR.RuntimeHintIgnoreReuse(b))
        case (IR.HINT_RESTART, WdlValues.V_Int(i))      => Some(IR.RuntimeHintRestart(default = Some(i)))
        case (IR.HINT_RESTART, WomObject(values, _)) =>
          Some(
              IR.RuntimeHintRestart(
                  values.get("max").map(unwrapWdlValues.V_Int),
                  values.get("default").map(unwrapWdlValues.V_Int),
                  values.get("errors").map {
                    case WdlValues.V_Map(WdlTypes.T_Map(WdlTypes.T_String, WdlTypes.T_Int), fields) =>
                      fields.map {
                        case (WdlValues.V_String(s), WdlValues.V_Int(i)) => (s -> i)
                        case other                         => throw new Exception(s"Invalid restart map entry ${other}")
                      }
                    case _ => throw new Exception("Invalid restart map")
                  }
              )
          )
        case (IR.HINT_TIMEOUT, WdlValues.V_String(s)) => Some(parseDuration(s))
        case (IR.HINT_TIMEOUT, WomObject(values, _)) =>
          Some(
              IR.RuntimeHintTimeout(values.get("days").map(unwrapWdlValues.V_Int),
                                    values.get("hours").map(unwrapWdlValues.V_Int),
                                    values.get("minutes").map(unwrapWdlValues.V_Int))
          )
        case _ => None
      }
      .toVector
  }

  // Compile a WDL task into an applet.
  //
  // Note: check if a task is a real WDL task, or if it is a wrapper for a
  // native applet.
  def apply(task: CallableTaskDefinition,
            taskSourceCode: String,
            adjunctFiles: Option[Vector[Adjuncts.AdjunctFile]]): IR.Applet = {
    Utils.trace(verbose.on, s"Compiling task ${task.name}")

    // create dx:applet input definitions. Note, some "inputs" are
    // actually expressions.
    val inputs: Vector[CVar] = task.inputs.flatMap {
      case RequiredInputDefinition(iName, womType, _, paramMeta) => {
        // This is a task "input" parameter declaration of the form:
        //     Int y
        val attr = ParameterMeta.unwrap(paramMeta, womType)
        Some(CVar(iName.value, womType, None, attr))
      }

      case OverridableInputDefinitionWithDefault(iName, womType, defaultExpr, _, paramMeta) =>
        WdlValues.VAnalysis.ifConstEval(womType, defaultExpr) match {
          case None =>
            // This is a task "input" parameter definition of the form:
            //    Int y = x + 3
            // We consider it an expression, and not an input. The
            // runtime system will evaluate it.
            None
          case Some(value) =>
            // This is a task "input" parameter definition of the form:
            //    Int y = 3
            val attr = ParameterMeta.unwrap(paramMeta, womType)
            Some(CVar(iName.value, womType, Some(value), attr))
        }

      // An input whose value should always be calculated from the default, and is
      // not allowed to be overridden.
      case FixedInputDefinitionWithDefault(iName, womType, defaultExpr, _, _) =>
        None

      case OptionalInputDefinition(iName, WdlTypes.T_Optional(womType), _, paramMeta) =>
        val attr = ParameterMeta.unwrap(paramMeta, womType)
        Some(CVar(iName.value, WdlTypes.T_Optional(womType), None, attr))
    }.toVector

    // create dx:applet outputs
    val outputs: Vector[CVar] = task.outputs.map {
      case OutputDefinition(id, womType, expr) =>
        val defaultValue = WdlValues.VAnalysis.ifConstEval(womType, expr) match {
          case None =>
            // This is an expression to be evaluated at runtime
            None
          case Some(value) =>
            // A constant, we can assign it now.
            Some(value)
        }
        CVar(id.value, womType, defaultValue)
    }.toVector

    val instanceType = calcInstanceType(task)

    val kind =
      (task.meta.get(IR.HINT_APP_TYPE), task.meta.get(IR.HINT_APP_ID)) match {
        case (Some(MetaValueElementString("native")), Some(MetaValueElementString(id))) =>
          // wrapper for a native applet.
          // make sure the runtime block is empty
          if (!task.runtimeAttributes.attributes.isEmpty)
            throw new Exception(s"Native task ${task.name} should have an empty runtime block")
          IR.AppletKindNative(id)
        case (_, _) =>
          // a WDL task
          IR.AppletKindTask(task)
      }

    // Handle any dx-specific runtime hints, other than "type" and "id" which are handled above
    val runtimeHints = unwrapRuntimeHints(task.runtimeAttributes.attributes)

    // Handle any task metadata
    val taskAttr = unwrapTaskMeta(task.meta, adjunctFiles)

    // Figure out if we need to use docker
    val docker = triageDockerImage(task.runtimeAttributes.attributes.get("docker"))

    val taskCleanedSourceCode = docker match {
      case IR.DockerImageDxFile(orgURL, dxFile) =>
        // The docker container is on the platform, we need to remove
        // the dxURLs in the runtime section, to avoid a runtime
        // lookup. For example:
        //
        //   dx://dxWDL_playground:/glnexus_internal  ->   dx://project-xxxx:record-yyyy
        val dxURL = DxUtils.dxDataObjectToURL(dxFile)
        taskSourceCode.replaceAll(orgURL, dxURL)
      case _ => taskSourceCode
    }
    val WdlCodeSnippet(selfContainedSourceCode) =
      WdlCodeGen(verbose, typeAliases, language).standAloneTask(taskCleanedSourceCode)

    val dockerFinal = docker match {
      case IR.DockerImageNone =>
        // No docker image was specified, but there might be one in the
        // overall defaults
        defaultRuntimeAttrs.m.get("docker") match {
          case None         => IR.DockerImageNone
          case Some(wValue) => triageDockerImage(Some(ValueAsAnExpression(wValue)))
        }
      case other => other
    }
    IR.Applet(task.name,
              inputs,
              outputs,
              instanceType,
              dockerFinal,
              kind,
              selfContainedSourceCode,
              Some(taskAttr),
              Some(runtimeHints))
  }
}
