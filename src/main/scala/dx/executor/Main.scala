package dx.executor

import java.nio.file.{InvalidPathException, Paths}

import dx.core.io.DxWorkerPaths
import dx.core.CliUtils._
import wdlTools.util.Enum

object Main {
  private val CommonOptions: InternalOptions = Map(
      "streamAllFiles" -> FlagOptionSpec.default
  )

  object ExecutorKind extends Enum {
    type ExecutorKind = Value
    val Task, Workflow = Value
  }

  private[executor] def dispatchCommand(args: Vector[String]): Termination = {
    if (args.size < 3) {
      return BadUsageTermination()
    }
    val kind = ExecutorKind.withNameIgnoreCase(args(0))
    val action = args(1).replaceAll("_", "")
    val rootDir =
      try {
        Paths.get(args(2))
      } catch {
        case _: InvalidPathException =>
          return BadUsageTermination(s"${args(1)} is not a valid root directory")
      }
    val options =
      try {
        parseCommandLine(args.drop(3), CommonOptions)
      } catch {
        case e: OptionParseException =>
          return BadUsageTermination("Error parsing command line options", Some(e))
      }
    initLogger(options)
    try {
      val jobMeta = WorkerJobMeta(DxWorkerPaths(rootDir))
      kind match {
        case ExecutorKind.Task =>
          val taskAction =
            try {
              TaskAction.withNameIgnoreCase(action)
            } catch {
              case _: NoSuchElementException =>
                return BadUsageTermination(s"Unknown action ${action}")
            }
          val streamAllFiles = options.getFlag("streamAllFiles")
          val taskExecutor = TaskExecutor(jobMeta, streamAllFiles)
          val successMessage = taskExecutor.apply(taskAction)
          Success(successMessage)
        case ExecutorKind.Workflow =>
          val workflowAction =
            try {
              WorkflowAction.withNameIgnoreCase(action)
            } catch {
              case _: NoSuchElementException =>
                return BadUsageTermination(s"Unknown action ${args(0)}")
            }
          val executor = WorkflowExecutor(jobMeta)
          val (_, successMessage) = executor.apply(workflowAction)
          Success(successMessage)
        case _ =>
          BadUsageTermination()
      }
    } catch {
      case e: Throwable =>
        Failure(s"failure running ${action}", Some(e))
    }
  }

  private val usageMessage =
    s"""|java -jar dxWDL.jar <task|workflow> <action> <rootdir> [options]
        |
        |Options:
        |    -streamAllFiles        Mount all files with dxfuse, do not use the download agent
        |    -traceLevel [0,1,2]    How much debug information to write to the
        |                           job log at runtime. Zero means write the minimum,
        |                           one is the default, and two is for internal debugging.
        |    -quiet                 Do not print warnings or informational outputs
        |    -verbose               Print detailed progress reports
        |    -verboseKey <module>   Detailed information for a specific module
        |    -logFile <path>        File to use for logging output; defaults to stderr
        |""".stripMargin

  def main(args: Vector[String]): Unit = {
    terminate(dispatchCommand(args), usageMessage)
  }
}
