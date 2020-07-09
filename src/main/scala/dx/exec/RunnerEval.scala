package dx.exec

import dx.AppInternalException
import dx.api.InstanceTypeDB
import dx.compiler.WdlRuntimeAttrs
import dx.core.languages.wdl.InstanceTypes
import wdlTools.eval.{Eval, WdlValues, Context => EvalContext}
import wdlTools.exec.DockerUtils
import wdlTools.types.{WdlTypes, TypedAbstractSyntax => TAT}
import wdlTools.util.{Logger, TraceLevel}

case class RunnerEval(task: TAT.Task,
                      inputs: RunnerEnv,
                      defaultRuntimeAttrs: Option[WdlRuntimeAttrs],
                      logger: Logger,
                      evaluator: Eval) {
  private lazy val evalContext = EvalContext.createFromEnv(inputs.env)

  // Evaluate the runtime expressions, and figure out which instance type
  // this task requires.
  //
  // Do not download the files, if there are any. We may be
  // calculating the instance type in the workflow runner, outside
  // the task.
  def calcInstanceType(instanceTypeDb: InstanceTypeDB): String = {
    logger.traceLimited("calcInstanceType", minLevel = TraceLevel.VVerbose)

    val runtimeAttrs: Map[String, TAT.Expr] = task.runtime match {
      case None                             => Map.empty
      case Some(TAT.RuntimeSection(kvs, _)) => kvs
    }

    def evalAttr(attrName: String): Option[WdlValues.V] = {
      runtimeAttrs.get(attrName) match {
        case None =>
          // try the defaults
          defaultRuntimeAttrs.flatMap(_.value.get(attrName))
        case Some(expr) =>
          Some(
              evaluator.applyExprAndCoerce(expr, WdlTypes.T_String, evalContext)
          )
      }
    }

    val dxInstanceType = evalAttr("dx_instance_type")
    val memory = evalAttr("memory")
    val diskSpace = evalAttr("disks")
    val cores = evalAttr("cpu")
    val gpu = evalAttr("gpu")
    val iTypeRaw = InstanceTypes.parse(dxInstanceType, memory, diskSpace, cores, gpu)
    val iType = instanceTypeDb.apply(iTypeRaw)
    logger.traceLimited(
        s"""|calcInstanceType memory=${memory} disk=${diskSpace}
            |cores=${cores} instancetype=${iType}""".stripMargin
          .replaceAll("\n", " ")
    )
    iType
  }

  lazy val command: String = {
    evaluator.applyCommand(task.command, EvalContext.createFromEnv(inputs.env))
  }

  // Figure out if a docker image is specified. If so, return it as a string.
  lazy val dockerImage: Option[String] = {
    val attributes: Map[String, TAT.Expr] = task.runtime match {
      case None                             => Map.empty
      case Some(TAT.RuntimeSection(kvs, _)) => kvs
    }
    val dImg: Option[WdlValues.V] = attributes.get("docker") match {
      case None =>
        defaultRuntimeAttrs match {
          case None      => None
          case Some(dra) => dra.value.get("docker")
        }
      case Some(expr) =>
        val value =
          evaluator.applyExprAndCoerce(expr,
                                       WdlTypes.T_String,
                                       EvalContext.createFromEnv(inputs.env))
        Some(value)
    }
    dImg match {
      case None => None
      case Some(WdlValues.V_String(nameOrUrl)) =>
        val dockerUtils = DockerUtils(evaluator.opts, evaluator.evalCfg)
        Some(dockerUtils.getImage(nameOrUrl, task.runtime.map(_.loc).getOrElse(task.loc)))
      case Some(other) =>
        throw new AppInternalException(s"docker is not a string expression ${other}")
    }
  }
}

object RunnerEval {
  def apply(task: TAT.Task,
            inputs: Map[TAT.InputDefinition, WdlValues.V],
            defaultRuntimeAttrs: Option[WdlRuntimeAttrs],
            logger: Logger,
            evaluator: Eval): RunnerEval = {
    val env = RunnerInputs(inputs, task, evaluator)
    RunnerEval(task, env, defaultRuntimeAttrs, logger, evaluator)
  }
}
