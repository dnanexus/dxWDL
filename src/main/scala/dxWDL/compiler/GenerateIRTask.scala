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

case class GenerateIRTask(verbose: Verbose,
                          typeAliases: Map[String, WomType],
                          language: Language.Value,
                          defaultRuntimeAttrs: WdlRuntimeAttrs) {
  val verbose2: Boolean = verbose.containsKey("GenerateIR")

  private class DynamicInstanceTypesException private (ex: Exception) extends RuntimeException(ex) {
    def this() = this(new RuntimeException("Runtime instance type calculation required"))
  }

  // Figure out which instance to use.
  //
  // Extract three fields from the task:
  // RAM, disk space, and number of cores. These are WDL expressions
  // that, in the general case, could be calculated only at runtime.
  // At compile time, constants expressions are handled. Some can
  // only be evaluated at runtime.
  private def calcInstanceType(task: CallableTaskDefinition): IR.InstanceType = {
    def evalAttr(attrName: String): Option[WomValue] = {
      task.runtimeAttributes.attributes.get(attrName) match {
        case None =>
          // Check the overall defaults, there might be a setting over there
          defaultRuntimeAttrs.m.get(attrName)
        case Some(expr) =>
          val result: ErrorOr[WomValue] =
            expr.evaluateValue(Map.empty[String, WomValue], wom.expression.NoIoFunctionSet)
          result match {
            case Invalid(_)         => throw new DynamicInstanceTypesException()
            case Valid(x: WomValue) => Some(x)
          }
      }
    }

    try {
      val dxInstanceType = evalAttr("dx_instance_type")
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

  // Convert a WOM Vector[MetaValueElement] of Strings into a Vector of Strings
  // Anything not a string will be filtered out.
  private def metaStringArrayToVec(array: Vector[MetaValueElement]): Vector[String] = {
    array.flatMap {
      case MetaValueElementString(str) => Some(str)
      case _                           => None
    }.toVector
  }

  // Convert a patterns WOM object value to IR
  private def metaPatternsObjToIR(obj: Map[String, MetaValueElement]): IR.IOAttrPatterns = {
    val name = obj.get("name") match {
      case Some(MetaValueElementArray(array)) =>
        Some(metaStringArrayToVec(array))
      case _ => None
    }
    val klass = obj.get("class") match {
      case Some(MetaValueElementString(value)) => Some(value)
      case _                                   => None
    }
    val tag = obj.get("tag") match {
      case Some(MetaValueElementArray(array)) =>
        Some(metaStringArrayToVec(array))
      case _ => None
    }
    // Even if all were None, create the IR.IOAttrPatterns object
    // The all none is handled in the native generation
    IR.IOAttrPatterns(IR.PatternsReprObj(name, klass, tag))
  }

  // A choices array may contain either raw values or (for data object types) annotated values,
  // which are hashes with required 'value' key and optional 'name' key. Each value must be of the
  // same type as the parameter, unless the parameter is an array, in which case choice values must
  // be of the same type as the array's contained type. For now, we only allow choices for
  // primitive- and file-type parameters, because there could be ambiguity (e.g. if a choice has a
  // 'value' key, should we treat it as a raw map value or as an annotated value?).
  //
  // choices: [true, false]
  // OR
  // choices: [{name: 'file1', value: "dx://file-XXX"}, {name: 'file2', value: "dx://file-YYY"}]
  private def metaChoicesArrayToIR(array: Vector[MetaValueElement],
                                   womType: WomType): Vector[IR.ChoiceRepr] = {
    if (array.isEmpty) {
      Vector()
    } else {
      array.map {
        case MetaValueElementObject(fields) =>
          if (!fields.contains("value")) {
            throw new Exception("Annotated choice must have a 'value' key")
          } else {
            metaChoiceValueToIR(
                womType = womType,
                value = fields("value"),
                name = fields.get("name")
            )
          }
        case rawElement: MetaValueElement =>
          metaChoiceValueToIR(womType = womType, value = rawElement)
        case _ =>
          throw new Exception(
              "Choices array must contain only raw values or annotated values (hash with "
                + "optional 'name' and required 'value' keys)"
          )
      }
    }
  }

  private def metaChoiceValueToIR(womType: WomType,
                                  value: MetaValueElement,
                                  name: Option[MetaValueElement] = None): IR.ChoiceRepr = {
    (womType, value) match {
      case (WomStringType, MetaValueElementString(str)) =>
        IR.ChoiceReprString(value = str)
      case (WomIntegerType, MetaValueElementInteger(i)) =>
        IR.ChoiceReprInteger(value = i)
      case (WomFloatType, MetaValueElementFloat(f)) =>
        IR.ChoiceReprFloat(value = f)
      case (WomBooleanType, MetaValueElementBoolean(b)) =>
        IR.ChoiceReprBoolean(value = b)
      case (WomSingleFileType, MetaValueElementString(str)) =>
        val nameStr: Option[String] = name match {
          case Some(MetaValueElementString(str)) => Some(str)
          case _                                 => None
        }
        IR.ChoiceReprFile(value = str, name = nameStr)
      case _ =>
        throw new Exception(
            "Choices keyword is only valid for primitive- and file-type parameters, and types must "
              + "match between parameter and choices"
        )
    }
  }

  // A suggestions array may contain either raw values or (for data object types) annotated values,
  // which are hashes. Each value must be of the same type as the parameter, unless the parameter
  // is an array, in which case choice values must be of the same type as the array's contained
  // type. For now, we only allow choices for primitive- and file-type parameters, because there
  // could be ambiguity (e.g. if a choice has a 'value' key, should we treat it as a raw map value
  // or as an annotated value?).
  //
  // suggestions: [true, false]
  // OR
  // suggestions: [
  //  {name: 'file1', value: "dx://file-XXX"}, {name: 'file2', value: "dx://file-YYY"}]
  private def metaSuggestionsArrayToIR(array: Vector[MetaValueElement],
                                       womType: WomType): Vector[IR.SuggestionRepr] = {
    if (array.isEmpty) {
      Vector()
    } else {
      array.map {
        case MetaValueElementObject(fields) =>
          metaSuggestionValueToIR(
              womType = womType,
              name = fields.get("name"),
              value = fields.get("value"),
              project = fields.get("project"),
              path = fields.get("path")
          )
        case rawElement: MetaValueElement =>
          metaSuggestionValueToIR(womType = womType, value = Some(rawElement))
        case _ =>
          throw new Exception(
              "Suggestions array must contain only raw values or annotated (hash) values"
          )
      }
    }
  }

  private def metaSuggestionValueToIR(womType: WomType,
                                      value: Option[MetaValueElement],
                                      name: Option[MetaValueElement] = None,
                                      project: Option[MetaValueElement] = None,
                                      path: Option[MetaValueElement] = None): IR.SuggestionRepr = {
    (womType, value) match {
      case (WomStringType, Some(MetaValueElementString(str))) =>
        IR.SuggestionReprString(value = str)
      case (WomIntegerType, Some(MetaValueElementInteger(i))) =>
        IR.SuggestionReprInteger(value = i)
      case (WomFloatType, Some(MetaValueElementFloat(f))) =>
        IR.SuggestionReprFloat(value = f)
      case (WomBooleanType, Some(MetaValueElementBoolean(b))) =>
        IR.SuggestionReprBoolean(value = b)
      case (WomSingleFileType, Some(MetaValueElementString(file))) =>
        createSuggestionFileIR(Some(file), name, project, path)
      case (WomSingleFileType, None) =>
        val s = createSuggestionFileIR(None, name, project, path)
        if (s.project.isEmpty || s.path.isEmpty) {
          throw new Exception(
              "If 'value' is not defined for a file-type suggestion, then both 'project' and 'path' "
                + "must be defined"
          )
        }
        s
      case _ =>
        throw new Exception(
            "Suggestion keyword is only valid for primitive- and file-type parameters, and types "
              + "must match between parameter and suggestions"
        )
    }
  }

  private def createSuggestionFileIR(file: Option[String],
                                     name: Option[MetaValueElement],
                                     project: Option[MetaValueElement],
                                     path: Option[MetaValueElement]): IR.SuggestionReprFile = {
    val nameStr: Option[String] = name match {
      case Some(MetaValueElementString(str)) => Some(str)
      case _                                 => None
    }
    val projectStr: Option[String] = project match {
      case Some(MetaValueElementString(str)) => Some(str)
      case _                                 => None
    }
    val pathStr: Option[String] = path match {
      case Some(MetaValueElementString(str)) => Some(str)
      case _                                 => None
    }
    IR.SuggestionReprFile(file, nameStr, projectStr, pathStr)
  }

  private def metaConstraintToIR(constraint: MetaValueElement): IR.ConstraintRepr = {
    constraint match {
      case MetaValueElementObject(obj: Map[String, MetaValueElement]) =>
        if (obj.size != 1) {
          throw new Exception("Constraint hash must have exactly one 'and' or 'or' key")
        }
        obj.head match {
          case (IR.PARAM_META_CONSTRAINT_AND, MetaValueElementArray(array)) =>
            IR.ConstraintReprOper(ConstraintOper.AND, array.map(metaConstraintToIR))
          case (IR.PARAM_META_CONSTRAINT_OR, MetaValueElementArray(array)) =>
            IR.ConstraintReprOper(ConstraintOper.OR, array.map(metaConstraintToIR))
          case _ =>
            throw new Exception(
                "Constraint must have key 'and' or 'or' and an array value"
            )
        }
      case MetaValueElementString(s) => IR.ConstraintReprString(s)
      case _                         => throw new Exception("'dx_type' constraints must be either strings or hashes")
    }
  }

  private def metaDefaultToIR(value: MetaValueElement, womType: WomType): IR.DefaultRepr = {
    (womType, value) match {
      case (WomStringType, MetaValueElementString(str)) =>
        IR.DefaultReprString(value = str)
      case (WomIntegerType, MetaValueElementInteger(i)) =>
        IR.DefaultReprInteger(value = i)
      case (WomFloatType, MetaValueElementFloat(f)) =>
        IR.DefaultReprFloat(value = f)
      case (WomBooleanType, MetaValueElementBoolean(b)) =>
        IR.DefaultReprBoolean(value = b)
      case (WomSingleFileType, MetaValueElementString(file)) =>
        IR.DefaultReprFile(value = file)
      case (womArrayType: WomArrayType, MetaValueElementArray(array)) =>
        def helper(wt: WomType)(mv: MetaValueElement) = metaDefaultToIR(mv, wt)
        IR.DefaultReprArray(array.map(helper(womArrayType.memberType)))
      case _ =>
        throw new Exception(
            "Default keyword is only valid for primitive-, file-, and array-type parameters, and "
              + "types must match between parameter and default"
        )
    }
  }

  private def unwrapWomArrayType(womType: WomType): WomType = {
    var wt = womType
    while (wt.isInstanceOf[WomArrayType]) {
      wt = wt.asInstanceOf[WomArrayType].memberType
    }
    wt
  }

  // Extract the parameter_meta info from the WOM structure
  // The parameter's WomType is passed in since some parameter metadata values are required to
  // have the same type as the parameter.
  private def unwrapParamMeta(paramMeta: Option[MetaValueElement],
                              womType: WomType): Option[Vector[IR.IOAttr]] = {
    paramMeta match {
      case None => None
      // If the parameter metadata is a string, treat it as help
      case Some(MetaValueElementString(text)) => Some(Vector(IR.IOAttrHelp(text)))
      case Some(MetaValueElementObject(obj)) => {
        // Whether to use 'description' in place of help
        val noHelp = !obj.contains(IR.PARAM_META_HELP)
        // Use flatmap to get the parameter metadata keys if they exist
        Some(obj.flatMap {
          case (IR.PARAM_META_GROUP, MetaValueElementString(text)) => Some(IR.IOAttrGroup(text))
          case (IR.PARAM_META_HELP, MetaValueElementString(text))  => Some(IR.IOAttrHelp(text))
          // Use 'description' in place of 'help' if the former is present and the latter is not
          case (IR.PARAM_META_DESCRIPTION, MetaValueElementString(text)) if noHelp =>
            Some(IR.IOAttrHelp(text))
          case (IR.PARAM_META_LABEL, MetaValueElementString(text)) => Some(IR.IOAttrLabel(text))
          // Try to parse the patterns key
          // First see if it's an array
          case (IR.PARAM_META_PATTERNS, MetaValueElementArray(array)) =>
            Some(IR.IOAttrPatterns(IR.PatternsReprArray(metaStringArrayToVec(array))))
          // See if it's an object, and if it is, parse out the optional key, class, and tag keys
          // Note all three are optional
          case (IR.PARAM_META_PATTERNS, MetaValueElementObject(obj)) =>
            Some(metaPatternsObjToIR(obj))
          // Try to parse the choices key, which will be an array of either values or objects
          case (IR.PARAM_META_CHOICES, MetaValueElementArray(array)) =>
            val wt = unwrapWomArrayType(womType)
            Some(IR.IOAttrChoices(metaChoicesArrayToIR(array, wt)))
          case (IR.PARAM_META_SUGGESTIONS, MetaValueElementArray(array)) =>
            val wt = unwrapWomArrayType(womType)
            Some(IR.IOAttrSuggestions(metaSuggestionsArrayToIR(array, wt)))
          case (IR.PARAM_META_TYPE, dx_type: MetaValueElement) =>
            val wt = unwrapWomArrayType(womType)
            wt match {
              case WomSingleFileType => Some(IR.IOAttrType(metaConstraintToIR(dx_type)))
              case _                 => throw new Exception("'dx_type' can only be specified for File parameters")
            }
          case (IR.PARAM_META_DEFAULT, default: MetaValueElement) =>
            Some(IR.IOAttrDefault(metaDefaultToIR(default, womType)))
          case _ => None
        }.toVector)
      }
      case _ => None // TODO: or throw exception?
    }
  }

  // Process a docker image, if there is one
  def triageDockerImage(dockerExpr: Option[WomExpression]): IR.DockerImage = {
    dockerExpr match {
      case None =>
        IR.DockerImageNone
      case Some(expr) if WomValueAnalysis.isExpressionConst(WomStringType, expr) =>
        val wdlConst = WomValueAnalysis.evalConst(WomStringType, expr)
        wdlConst match {
          case WomString(url) if url.startsWith(Utils.DX_URL_PREFIX) =>
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

  private def unwrapMetaValueElementString(element: MetaValueElement): String = {
    element match {
      case MetaValueElementString(text) => text
      case _                            => throw new Exception(s"Expected MetaValueElement, got ${element}")
    }
  }

  private def unwrapMetaValueElementStringArray(element: MetaValueElement): Vector[String] = {
    element match {
      case MetaValueElementArray(array) => array.map(unwrapMetaValueElementString)
      case _                            => throw new Exception(s"Expected MetaValueElementArray, got ${element}")
    }
  }

  private def unwrapTaskMeta(meta: Map[String, MetaValueElement]): Vector[IR.AppAttr] = {
    meta.flatMap {
      case (IR.META_TITLE, MetaValueElementString(text))       => Some(IR.AppAttrTitle(text))
      case (IR.META_DESCRIPTION, MetaValueElementString(text)) => Some(IR.AppAttrDescription(text))
      case (IR.META_SUMMARY, MetaValueElementString(text))     => Some(IR.AppAttrSummary(text))
      case (IR.META_DEVELOPER_NOTES, MetaValueElementString(text)) =>
        Some(IR.AppAttrDeveloperNotes(text))
      case (IR.META_VERSION, MetaValueElementString(text)) => Some(IR.AppAttrVersion(text))
      case (IR.META_DETAILS, MetaValueElementObject(fields)) =>
        val change_log: Option[IR.ChangesRepr] = fields.get("change_log") match {
          case None                         => None
          case MetaValueElementString(text) => Some(IR.ChangesReprString(text))
          case MetaValueElementArray(eltArray) =>
            Some(IR.ChangesReprList(eltArray.map {
              case MetaValueElementObject(fields) =>
                IR.VersionChanges(fields("version"),
                                  unwrapMetaValueElementStringArray(fields("changes")))
              case other =>
                throw new Exception(s"Unexpected value for 'change_log' element: ${other}")
            }))
          case other => throw new Exception(s"Unexpected value for 'change_log': ${other}")
        }
        Some(
            IR.AppAttrDetails(
                fields.get("contact_email").map(unwrapMetaValueElementString),
                fields.get("upstream_version").map(unwrapMetaValueElementString),
                fields.get("upstream_author").map(unwrapMetaValueElementString),
                fields.get("upstream_url").map(unwrapMetaValueElementString),
                fields.get("upstream_licenses").map(unwrapMetaValueElementStringArray),
                change_log
            )
        )
      case (IR.META_OPEN_SOURCE, MetaValueElementBoolean(b)) => Some(IR.AppAttrBoolean(b))
      case _                                                 => None
    }.toVector
  }

  // Compile a WDL task into an applet.
  //
  // Note: check if a task is a real WDL task, or if it is a wrapper for a
  // native applet.
  def apply(task: CallableTaskDefinition, taskSourceCode: String): IR.Applet = {
    Utils.trace(verbose.on, s"Compiling task ${task.name}")

    // create dx:applet input definitions. Note, some "inputs" are
    // actually expressions.
    val inputs: Vector[CVar] = task.inputs.flatMap {
      case RequiredInputDefinition(iName, womType, _, paramMeta) => {
        // This is a task "input" parameter declaration of the form:
        //     Int y
        val attr = unwrapParamMeta(paramMeta, womType)
        Some(CVar(iName.value, womType, None, attr))
      }

      case OverridableInputDefinitionWithDefault(iName, womType, defaultExpr, _, paramMeta) =>
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
            val attr = unwrapParamMeta(paramMeta, womType)
            Some(CVar(iName.value, womType, Some(value), attr))
        }

      // An input whose value should always be calculated from the default, and is
      // not allowed to be overridden.
      case FixedInputDefinitionWithDefault(iName, womType, defaultExpr, _, _) =>
        None

      case OptionalInputDefinition(iName, WomOptionalType(womType), _, paramMeta) =>
        val attr = unwrapParamMeta(paramMeta, womType)
        Some(CVar(iName.value, WomOptionalType(womType), None, attr))
    }.toVector

    // create dx:applet outputs
    val outputs: Vector[CVar] = task.outputs.map {
      case OutputDefinition(id, womType, expr) =>
        val defaultValue = WomValueAnalysis.ifConstEval(womType, expr) match {
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
      (task.meta.get("type"), task.meta.get("id")) match {
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

    // Parse any task metadata (other than 'type' and 'id', which are handled above)
    val appAttr = unwrapTaskMeta(task.meta)

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
              Some(appAttr))
  }
}
