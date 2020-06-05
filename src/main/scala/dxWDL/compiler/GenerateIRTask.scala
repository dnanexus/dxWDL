package dxWDL.compiler

import wdlTools.eval.WdlValues
import wdlTools.types.{TypedAbstractSyntax => TAT}
import wdlTools.types.WdlTypes
import dxWDL.base._
import dxWDL.dx._
import dxWDL.util._
import IR.CVar

import scala.util.matching.Regex

case class GenerateIRTask(verbose: Verbose,
                          typeAliases: Map[String, WdlTypes.T],
                          language: Language.Value,
                          defaultRuntimeAttrs: WdlRuntimeAttrs,
                          defaultHintAttrs: WdlHintAttrs) {
  val verbose2: Boolean = verbose.containsKey("GenerateIR")

  private class DynamicInstanceTypesException(message: String) extends RuntimeException(message) {
    // TODO: this never gets called - do you want this to be a default message? if so, make it the default
    //  value of `message`.
    def this() = this("Runtime instance type calculation required")
  }

  def evalWomExpression(expr: TAT.Expr): WdlValues.V = {
    WomValueAnalysis.ifConstEval(expr.wdlType, expr) match {
      case None    => throw new DynamicInstanceTypesException()
      case Some(x) => x
    }
  }

  // Figure out which instance to use.
  //
  // Extract three fields from the task:
  // RAM, disk space, and number of cores. These are WDL expressions
  // that, in the general case, could be calculated only at runtime.
  // At compile time, constants expressions are handled. Some can
  // only be evaluated at runtime.
  private def calcInstanceType(task: TAT.Task): IR.InstanceType = {
    def evalRuntimeAttr(attrName: String): Option[WdlValues.V] = {
      val runtimeAttributes: Map[String, TAT.Expr] = task.runtime match {
        case None                             => Map.empty
        case Some(TAT.RuntimeSection(kvs, _)) => kvs
      }
      runtimeAttributes.get(attrName) match {
        case None =>
          // Check the overall defaults, there might be a setting over there
          defaultRuntimeAttrs.m.get(attrName)
        case Some(expr) =>
          Some(evalWomExpression(expr))
      }
    }

    def evalHintAttr(attrName: String): Option[TAT.MetaValue] = {
      val hintAttributes: Map[String, TAT.MetaValue] = task.hints match {
        case None                           => Map.empty
        case Some(TAT.HintsSection(kvs, _)) => kvs
      }
      hintAttributes.get(attrName) match {
        case None =>
          // Check the overall defaults, there might be a setting over there
          defaultHintAttrs.m.get(attrName)
        case Some(value) => Some(value)
      }
    }

    try {
      val dxInstanceType =
        evalRuntimeAttr(IR.HINT_INSTANCE_TYPE).orElse(evalHintAttr(IR.HINT_INSTANCE_TYPE).map {
          case TAT.MetaValueString(s, _) => WdlValues.V_String(s)
          case _                         => throw new RuntimeException("Expected dx_instance_type to be a string")
        })
      val memory = evalRuntimeAttr("memory")
      val diskSpace = evalRuntimeAttr("disks")
      val cores = evalRuntimeAttr("cpu")
      val gpu = evalRuntimeAttr("gpu")
      val iTypeDesc = InstanceTypeDB.parse(dxInstanceType, memory, diskSpace, cores, gpu)
      IR.InstanceTypeConst(iTypeDesc.dxInstanceType,
                           iTypeDesc.memoryMB,
                           iTypeDesc.diskGB,
                           iTypeDesc.cpu,
                           iTypeDesc.gpu)
    } catch {
      case _: DynamicInstanceTypesException =>
        // The generated code will need to calculate the instance type at runtime
        IR.InstanceTypeRuntime
    }
  }

  def triageDockerImageFromValue(dockerValue: WdlValues.V): IR.DockerImage = {
    dockerValue match {
      case WdlValues.V_String(url) if url.startsWith(Utils.DX_URL_PREFIX) =>
        // A constant image specified with a DX URL
        val dxfile = DxPath.resolveDxURLFile(url)
        IR.DockerImageDxFile(url, dxfile)
      case _ =>
        // Probably a public docker image
        IR.DockerImageNetwork
    }
  }

  // Process a docker image, if there is one
  def triageDockerImage(dockerExpr: Option[TAT.Expr]): IR.DockerImage = {
    dockerExpr match {
      case None =>
        IR.DockerImageNone
      case Some(expr) if WomValueAnalysis.isExpressionConst(WdlTypes.T_String, expr) =>
        val wdlConst = WomValueAnalysis.evalConst(WdlTypes.T_String, expr)
        triageDockerImageFromValue(wdlConst)
      // TODO: handle array case
      case _ =>
        // Image will be downloaded from the network
        IR.DockerImageNetwork
    }
  }

  private def unwrapTaskMeta(
      meta: Map[String, TAT.MetaValue],
      adjunctFiles: Option[Vector[Adjuncts.AdjunctFile]]
  ): Vector[IR.TaskAttr] = {
    val appAttrs = meta.flatMap {
      case (IR.META_TITLE, TAT.MetaValueString(text, _))       => Some(IR.TaskAttrTitle(text))
      case (IR.META_DESCRIPTION, TAT.MetaValueString(text, _)) => Some(IR.TaskAttrDescription(text))
      case (IR.META_SUMMARY, TAT.MetaValueString(text, _))     => Some(IR.TaskAttrSummary(text))
      case (IR.META_DEVELOPER_NOTES, TAT.MetaValueString(text, _)) =>
        Some(IR.TaskAttrDeveloperNotes(text))
      case (IR.META_VERSION, TAT.MetaValueString(text, _)) => Some(IR.TaskAttrVersion(text))
      case (IR.META_DETAILS, TAT.MetaValueObject(fields, _)) =>
        Some(IR.TaskAttrDetails(ParameterMeta.translateMetaKVs(fields)))
      case (IR.META_OPEN_SOURCE, TAT.MetaValueBoolean(b, _)) => Some(IR.TaskAttrOpenSource(b))
      case (IR.META_CATEGORIES, TAT.MetaValueArray(array, _)) =>
        Some(IR.TaskAttrCategories(array.map {
          case TAT.MetaValueString(text, _) => text
          case other                        => throw new Exception(s"Invalid category: ${other}")
        }))
      case (IR.META_TYPES, TAT.MetaValueArray(array, _)) =>
        Some(IR.TaskAttrTypes(array.map {
          case TAT.MetaValueString(text, _) => text
          case other                        => throw new Exception(s"Invalid type: ${other}")
        }))
      case (IR.META_TAGS, TAT.MetaValueArray(array, _)) =>
        Some(IR.TaskAttrTags(array.map {
          case TAT.MetaValueString(text, _) => text
          case other                        => throw new Exception(s"Invalid tag: ${other}")
        }))
      case (IR.META_PROPERTIES, TAT.MetaValueObject(fields, _)) =>
        Some(IR.TaskAttrProperties(fields.view.mapValues {
          case TAT.MetaValueString(text, _) => text
          case other                        => throw new Exception(s"Invalid property value: ${other}")
        }.toMap))
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
        }
      case None => Vector.empty
    })
  }

  private def unwrapMetaString(value: TAT.MetaValue): String = {
    value match {
      case TAT.MetaValueString(s, _) => s
      case _                         => throw new Exception("Expected MetaValueString")
    }
  }

  private def unwrapMetaStringArray(array: TAT.MetaValue): Vector[String] = {
    array match {
      case TAT.MetaValueArray(strings, _) =>
        strings.map(unwrapMetaString)
      case _ => throw new Exception("Expected WdlValues.V_Array")
    }
  }

  private def unwrapMetaBoolean(value: TAT.MetaValue): Boolean = {
    value match {
      case TAT.MetaValueBoolean(b, _) => b
      case _                          => throw new Exception("Expected WdlValues.V_Boolean")
    }
  }

  private def unwrapMetaInteger(value: TAT.MetaValue): Int = {
    value match {
      case TAT.MetaValueInt(i, _) => i
      case _                      => throw new Exception("Expected WdlValues.V_Int")
    }
  }

  private val durationRegexp = s"^(?:(\\d+)D)?(?:(\\d+)H)?(?:(\\d+)M)?".r
  //val durationFields = Vector("days", "hours", "minutes")

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

  private def unwrapHints(hints: Map[String, TAT.MetaValue]): Vector[IR.RuntimeHint] = {
    val hintKeys = Set(IR.HINT_ACCESS, IR.HINT_IGNORE_REUSE, IR.HINT_RESTART, IR.HINT_TIMEOUT)

    hints.view
      .filterKeys(hintKeys)
      .flatMap {
        case (IR.HINT_ACCESS, TAT.MetaValueObject(values, _)) =>
          Some(
              IR.RuntimeHintAccess(
                  network = values.get("network").map(unwrapMetaStringArray),
                  project = values.get("project").map(unwrapMetaString),
                  allProjects = values.get("allProjects").map(unwrapMetaString),
                  developer = values.get("developer").map(unwrapMetaBoolean),
                  projectCreation = values.get("projectCreation").map(unwrapMetaBoolean)
              )
          )
        case (IR.HINT_IGNORE_REUSE, TAT.MetaValueBoolean(b, _)) =>
          Some(IR.RuntimeHintIgnoreReuse(b))
        case (IR.HINT_RESTART, TAT.MetaValueInt(i, _)) =>
          Some(IR.RuntimeHintRestart(default = Some(i)))
        case (IR.HINT_RESTART, TAT.MetaValueObject(values, _)) =>
          Some(
              IR.RuntimeHintRestart(
                  values.get("max").map(unwrapMetaInteger),
                  values.get("default").map(unwrapMetaInteger),
                  values.get("errors").map {
                    case TAT.MetaValueObject(fields, _) =>
                      fields.map {
                        case (s, TAT.MetaValueInt(i, _)) => s -> i
                        case other =>
                          throw new Exception(s"Invalid restart map entry ${other}")
                      }
                    case _ => throw new Exception("Invalid restart map")
                  }
              )
          )
        case (IR.HINT_TIMEOUT, TAT.MetaValueString(s, _)) => Some(parseDuration(s))
        case (IR.HINT_TIMEOUT, TAT.MetaValueObject(values, _)) =>
          Some(
              IR.RuntimeHintTimeout(values.get("days").map(unwrapMetaInteger),
                                    values.get("hours").map(unwrapMetaInteger),
                                    values.get("minutes").map(unwrapMetaInteger))
          )
        case _ => None
      }
      .toVector
  }

  private def lookupInputParam(iName: String, task: TAT.Task): Option[TAT.MetaValue] = {
    task.parameterMeta match {
      case None => None
      case Some(TAT.ParameterMetaSection(kvs, _)) =>
        kvs.get(iName)
    }
  }

  private def lookupMetaParam(iName: String, task: TAT.Task): Option[TAT.MetaValue] = {
    task.meta match {
      case None => None
      case Some(TAT.MetaSection(kvs, _)) =>
        kvs.get(iName)
    }
  }

  // Compile a WDL task into an applet.
  //
  // Note: check if a task is a real WDL task, or if it is a wrapper for a
  // native applet.
  def apply(task: TAT.Task,
            taskSourceCode: String,
            adjunctFiles: Option[Vector[Adjuncts.AdjunctFile]]): IR.Applet = {
    Utils.trace(verbose.on, s"Compiling task ${task.name}")

    // create dx:applet input definitions. Note, some "inputs" are
    // actually expressions.
    val inputs: Vector[CVar] = task.inputs.flatMap {
      case TAT.RequiredInputDefinition(iName, womType, _) => {
        // This is a task "input" parameter declaration of the form:
        //     Int y
        val paramMeta = lookupInputParam(iName, task)
        val attr = ParameterMeta.unwrap(paramMeta, womType)
        Some(CVar(iName, womType, None, attr))
      }

      case TAT.OverridableInputDefinitionWithDefault(iName, womType, defaultExpr, _) =>
        WomValueAnalysis.ifConstEval(womType, defaultExpr) match {
          case None =>
            // This is a task "input" parameter definition of the form:
            //    Int y = x + 3
            // We consider it an expression, and not an input. The
            // runtime system will evaluate it.
            None
          case Some(value) =>
            // This is a task "input" parameter definition of the form:
            //    Int y = 3
            val paramMeta = lookupInputParam(iName, task)
            val attr = ParameterMeta.unwrap(paramMeta, womType)
            Some(CVar(iName, womType, Some(value), attr))
        }

      case TAT.OptionalInputDefinition(iName, WdlTypes.T_Optional(womType), _) =>
        val paramMeta = lookupInputParam(iName, task)
        val attr = ParameterMeta.unwrap(paramMeta, womType)
        Some(CVar(iName, WdlTypes.T_Optional(womType), None, attr))
    }

    // create dx:applet outputs
    val outputs: Vector[CVar] = task.outputs.map {
      case TAT.OutputDefinition(id, womType, expr, _) =>
        val defaultValue = WomValueAnalysis.ifConstEval(womType, expr) match {
          case None =>
            // This is an expression to be evaluated at runtime
            None
          case Some(value) =>
            // A constant, we can assign it now.
            Some(value)
        }
        CVar(id, womType, defaultValue)
    }

    val instanceType = calcInstanceType(task)

    val kind =
      (lookupMetaParam(IR.HINT_APP_TYPE, task), lookupMetaParam(IR.HINT_APP_ID, task)) match {
        case (Some(TAT.MetaValueString("native", _)), Some(TAT.MetaValueString(id, _))) =>
          // wrapper for a native applet.
          // make sure the runtime block is empty
          if (task.runtime.isDefined)
            throw new Exception(s"Native task ${task.name} should have an empty runtime block")
          IR.AppletKindNative(id)
        case (_, _) =>
          // a WDL task
          IR.AppletKindTask(task)
      }

    // Handle any dx-specific runtime hints, other than "type" and "id" which are handled above
    val hintAttr: Map[String, TAT.MetaValue] = task.hints match {
      case None                           => Map.empty
      case Some(TAT.HintsSection(kvs, _)) => kvs
    }
    val runtimeHints = unwrapHints(hintAttr)

    // Handle any task metadata
    val meta = task.meta match {
      case None                          => Map.empty[String, TAT.MetaValue]
      case Some(TAT.MetaSection(kvs, _)) => kvs
    }
    val taskAttr = unwrapTaskMeta(meta, adjunctFiles)

    // Figure out if we need to use docker
    val docker = task.runtime match {
      case None                             => triageDockerImage(None)
      case Some(TAT.RuntimeSection(kvs, _)) => triageDockerImage(kvs.get("docker"))
    }

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
          case Some(wValue) => triageDockerImageFromValue(wValue)
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
