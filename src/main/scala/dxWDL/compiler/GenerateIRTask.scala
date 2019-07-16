package dxWDL.compiler

import cats.data.Validated.{Invalid, Valid}
import common.validation.ErrorOr.ErrorOr
import wom.callable.CallableTaskDefinition
import wom.callable.Callable._
import wom.callable.MetaValueElement._
import wom.types._
import wom.values._

import dxWDL.base._
import dxWDL.dx._
import dxWDL.util._
import IR.CVar

case class GenerateIRTask(verbose: Verbose,
                          typeAliases: Map[String, WomType],
                          language: Language.Value) {
    val verbose2 : Boolean = verbose.containsKey("GenerateIR")

    private class DynamicInstanceTypesException private(ex: Exception) extends RuntimeException(ex) {
        def this() = this(new RuntimeException("Runtime instance type calculation required"))
    }

    // Figure out which instance to use.
    //
    // Extract three fields from the task:
    // RAM, disk space, and number of cores. These are WDL expressions
    // that, in the general case, could be calculated only at runtime.
    // At compile time, constants expressions are handled. Some can
    // only be evaluated at runtime.
    private def calcInstanceType(task: CallableTaskDefinition) : IR.InstanceType = {
        def evalAttr(task: CallableTaskDefinition, attrName: String) : Option[WomValue] = {
            task.runtimeAttributes.attributes.get(attrName) match {
                case None => None
                case Some(expr) =>
                    val result: ErrorOr[WomValue] =
                        expr.evaluateValue(Map.empty[String, WomValue], wom.expression.NoIoFunctionSet)
                    result match {
                        case Invalid(_) => throw new DynamicInstanceTypesException()
                        case Valid(x: WomValue) => Some(x)
                    }
            }
        }

        try {
            val dxInstaceType = evalAttr(task, Extras.DX_INSTANCE_TYPE_ATTR)
            val memory = evalAttr(task, "memory")
            val diskSpace = evalAttr(task, "disks")
            val cores = evalAttr(task, "cpu")
            val iTypeDesc = InstanceTypeDB.parse(dxInstaceType, memory, diskSpace, cores)
            IR.InstanceTypeConst(iTypeDesc.dxInstanceType,
                                 iTypeDesc.memoryMB,
                                 iTypeDesc.diskGB,
                                 iTypeDesc.cpu)
        } catch {
            case e : DynamicInstanceTypesException =>
                // The generated code will need to calculate the instance type at runtime
                IR.InstanceTypeRuntime
        }
    }

    // Compile a WDL task into an applet.
    //
    // Note: check if a task is a real WDL task, or if it is a wrapper for a
    // native applet.
    def apply(task : CallableTaskDefinition,
              taskSourceCode: String) : IR.Applet = {
        Utils.trace(verbose.on, s"Compiling task ${task.name}")

        // create dx:applet input definitions. Note, some "inputs" are
        // actually expressions.
        val inputs: Vector[CVar] = task.inputs.flatMap{
            case RequiredInputDefinition(iName, womType, _, _) =>
                Some(CVar(iName.value, womType, None))

            case OverridableInputDefinitionWithDefault(iName, womType, defaultExpr, _, _) =>
                WomValueAnalysis.ifConstEval(womType, defaultExpr) match {
                    case None =>
                        // This is a task "input" of the form:
                        //    Int y = x + 3
                        // We consider it an expression, and not an input. The
                        // runtime system will evaluate it.
                        None
                    case Some(value) =>
                        Some(CVar(iName.value, womType, Some(value)))
                }

            // An input whose value should always be calculated from the default, and is
            // not allowed to be overridden.
            case FixedInputDefinitionWithDefault(iName, womType, defaultExpr, _, _) =>
                None

            case OptionalInputDefinition(iName, WomOptionalType(womType), _, _) =>
                Some(CVar(iName.value, WomOptionalType(womType), None))
        }.toVector

        // create dx:applet outputs
        val outputs : Vector[CVar] = task.outputs.map{
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
                case (_,_) =>
                    // a WDL task
                    IR.AppletKindTask(task)
            }



        // Figure out if we need to use docker
        val docker = task.runtimeAttributes.attributes.get("docker") match {
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
        // The docker container is on the platform, we need to remove
        // the dxURLs in the runtime section, to avoid a runtime
        // lookup. For example:
        //
        //   dx://dxWDL_playground:/glnexus_internal  ->   dx://project-xxxx:record-yyyy
        val taskCleanedSourceCode = docker match {
            case IR.DockerImageDxFile(orgURL, dxFile) =>
                val dxURL = DxUtils.dxDataObjectToURL(dxFile)
                taskSourceCode.replaceAll(orgURL, dxURL)
            case _ => taskSourceCode
        }
        val WdlCodeSnippet(selfContainedSourceCode) =
            WdlCodeGen(verbose, typeAliases, language).standAloneTask(taskCleanedSourceCode)

        IR.Applet(task.name, inputs, outputs, instanceType, docker, kind, selfContainedSourceCode)
    }
}
