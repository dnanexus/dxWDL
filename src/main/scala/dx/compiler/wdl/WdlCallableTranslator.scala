package dx.compiler.wdl

import dx.api.DxApi
import dx.compiler.IR
import dx.compiler.ir.{Callable, ReorgAttributes, RuntimeAttributes, Type, Value}
import dx.core.languages.wdl.InstanceTypes
import wdlTools.eval.{Eval, EvalPaths, WdlValues, RuntimeAttributes => WdlRuntimeAttributes}
import wdlTools.types.{TypedAbstractSyntax => TAT}
import wdlTools.util.{FileSourceResolver, Logger}

case class WdlCallableTranslator(wdlBundle: WdlBundle,
                                 typeAliases: Map[String, Type],
                                 locked: Boolean,
                                 defaultRuntimeAttrs: RuntimeAttributes,
                                 reorgAttrs: ReorgAttributes,
                                 dxApi: DxApi = DxApi.get,
                                 fileResolver: FileSourceResolver = FileSourceResolver.get,
                                 logger: Logger = Logger.get) {

  private lazy val evaluator: Eval = Eval(EvalPaths.empty, wdlBundle.version, fileResolver, logger)

  private case class WdlTaskTranslator(task: TAT.Task) {
    private lazy val taskRuntimeAttrs: WdlRuntimeAttributes =
      WdlRuntimeAttributes.fromTask(task, evaluator)

    private def calcInstanceType(task: TAT.Task): IR.InstanceType = {
      def evalRuntimeAttr(attrName: String): Option[Value] = {
        taskRuntimeAttrs.getValue(attrName) match {
          case None =>
            // Check the overall defaults, there might be a setting over there
            defaultRuntimeAttrs.value.get(attrName)
          case Some(value) =>
            Utils.wdlToIRValue(value)
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
        val iTypeDesc = InstanceTypes.parse(dxInstanceType, memory, diskSpace, cores, gpu)
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

    def apply: Callable = {
      logger.trace(s"Translating task ${task.name}")
    }
  }

  private case class WdlWorkflowTranslator(wf: TAT.Workflow) {
    // Only the toplevel workflow may be unlocked. This happens
    // only if the user specifically compiles it as "unlocked".
    private lazy val isLocked: Boolean = {
      wdlBundle.primaryCallable match {
        case Some(wf2: TAT.Workflow) =>
          wf.name != wf2.name || locked
        case _ =>
          true
      }
    }

    def apply: Vector[Callable] = {}
  }

  def translateCallable(callable: TAT.Callable): Vector[Callable] = {
    callable match {
      case task: TAT.Task =>
        val taskTranslator = WdlTaskTranslator(task)
        Vector(taskTranslator.apply)
      case wf: TAT.Workflow =>
        val taskTranslator = WdlWorkflowTranslator(wf)
        taskTranslator.apply
    }
  }
}
