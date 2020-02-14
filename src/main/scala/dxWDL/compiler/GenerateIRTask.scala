package dxWDL.compiler

import scala.reflect.ClassTag

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

  // Convert a WOM 
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

  // A choices array may contain either raw values or annotated values, which are hashes with
  // required 'value' key and optional 'name' key. The DNAnexus API allows raw and annoted values 
  // to be mixed, but dxWDL will impose the limitation that all elements of the choices array must 
  // be of the same type. Each value must be of the same type as the parameter, unless the 
  // parameter is an array, in which case choice values must be of the same type as the array's 
  // contained type. For now, we only allow choices for primitive- and file-type parameters, 
  // because there could be ambiguity (e.g. if a choice has a 'value' key, should we treat it as a 
  // raw map value or as an annotated value?).
  // 
  // choices: [true, false]
  // OR
  // choices: [{'name': 'yes', 'value': true}, {'name': 'no', 'value': false}]
  private def metaChoicesArrayToIR(array: Vector[MetaValueElement], 
                                   womType: WomType): Option[IR.IOAttrChoices] = womType match {
    case WomStringType | WomFileType =>
      metaChoicesTypedArrayToIR[String, MetaValueElementString](array)
    case WomIntegerType =>
      metaChoicesTypedArrayToIR[Int, MetaValueElementInteger](array)
    case WomFloatType =>
      metaChoicesTypedArrayToIR[Float, MetaValueElementFloat](array)
    case WomBooleanType =>
      metaChoicesTypedArrayToIR[Boolean, MetaValueElementBoolean](array)
    case _ =>
      // TODO: log or exception?
      // "dxWDL only allows 'choices' in parameter_meta for parameters that are of
      // primitive or file types"
      None
  }

  private def metaChoicesTypedArrayToIR[T, MT](array: Vector[MT])(implicit tag: ClassTag[MT]):     
                                               Option[IR.IOAttrChoices] =
    if (array.isEmpty) {
      Some(IR.IOAttrChoices(IR.ChoicesReprValArray[T]([])))
    } else {
      array.head match {
        case MetaValueElementObject =>
          Some(IR.IOAttrChoices(IR.ChoicesReprObjArray[T](array.map {
            case element: MetaValueElementObject =>
              if (!element.value.contains("value")) {
                throw Exception("Annotated choice must have a 'value' key")
              } else {
                IR.ChoicesReprObj[T](
                  name = element.value.getOrElse("name", None), 
                  value = element.value["value"].value
                )                  
              }
            case _ => throw Exception(
              "Choices array must contain only values or only annotated values"
            )
          })))
        case MT =>
          Some(IR.IOAttrChoices(IR.ChoicesReprValArray[T](array.map {
            case element: MT => element.value
            case _ => throw Exception(
              "Choices array must contain only values or only annotated values"
            )
          })))
        case _ =>
          // TODO: log or exception?
          // "Mismatch between parameter type {womType} and choice type 
          // {array.head.getClass}"
          None
      }
    }

  // Extract the parameter_meta info from the WOM structure
  // The parameter's WomType is passed in since some parameter metadata values are required to 
  // have the same type as the parameter.
  private def unwrapParamMeta(
      paramMeta: Option[MetaValueElement], womType: WomType
  ): Option[Vector[IR.IOAttr]] = paramMeta match {
    case None => None
    case Some(MetaValueElementObject(obj)) => {
      // Use flatmap to get the parameter metadata keys if they exist
      Some(obj.flatMap {
        case (IR.PARAM_META_GROUP, MetaValueElementString(text)) => Some(IR.IOAttrGroup(text))
        case (IR.PARAM_META_HELP, MetaValueElementString(text)) => Some(IR.IOAttrHelp(text))
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
          wt = womType
          while wt.isInstanceOf[WomArrayType] {
            wt = wt.memberType
          }
          metaChoicesArrayToIR(array, wt)
        case _ => None
      }.toVector)
    }
    case _ => None // TODO: or throw exception?
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
        val attr = unwrapParamMeta(paramMeta, womType)
        Some(CVar(iName.value, womType, None, attr))
      }

      case OverridableInputDefinitionWithDefault(iName, womType, defaultExpr, _, paramMeta) =>
        WomValueAnalysis.ifConstEval(womType, defaultExpr) match {
          case None =>
            // This is a task "input" of the form:
            //    Int y = x + 3
            // We consider it an expression, and not an input. The
            // runtime system will evaluate it.
            None
          case Some(value) =>
            val attr = unwrapParamMeta(paramMeta, womType)
            Some(CVar(iName.value, womType, Some(value), attr))
        }

      // An input whose value should always be calculated from the default, and is
      // not allowed to be overridden.
      case FixedInputDefinitionWithDefault(iName, womType, defaultExpr, _, _) =>
        None

      case OptionalInputDefinition(iName, WomOptionalType(womType), _, _) =>
        Some(CVar(iName.value, WomOptionalType(womType), None))
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
    IR.Applet(task.name, inputs, outputs, instanceType, dockerFinal, kind, selfContainedSourceCode)
  }
}
