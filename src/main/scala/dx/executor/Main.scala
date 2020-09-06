package dx.executor

import java.nio.file.{InvalidPathException, Paths}

import dx.core.util.MainUtils._

object Main {
  private val CommonOptions: InternalOptions = Map(
      "streamAllFiles" -> FlagOptionSpec.Default
  )

  private[executor] def dispatchCommand(args: Vector[String]): Termination = {
    if (args.size < 3) {
      return BadUsageTermination()
    }
    val kind = args(0)
    val action = args(1).replaceAll("_", "")
    val homeDir =
      try {
        Paths.get(args(2))
      } catch {
        case _: InvalidPathException =>
          return BadUsageTermination(s"${args(1)} is not a valid home directory")
      }
    val options =
      try {
        parseCommandLine(args.drop(3), CommonOptions)
      } catch {
        case e: OptionParseException =>
          return BadUsageTermination("Error parsing command line options", Some(e))
      }
    val logger = initLogger(options)
    kind match {
      case "task" =>
        val taskAction =
          try {
            TaskAction.withNameIgnoreCase(action)
          } catch {
            case _: NoSuchElementException =>
              return BadUsageTermination(s"Unknown action ${action}")
          }
        val streamAllFiles = options.getFlag("streamAllFiles")
        try {
          val taskExecutor = TaskExecutor(homeDir, streamAllFiles, logger = logger)
          val successMessage = taskExecutor.apply(taskAction)
          Success(successMessage)
        } catch {
          case e: Throwable =>
            Failure(s"failure running ${action}", Some(e))
        }
      case "frag" =>
        val wfAction =
          try {
            WorkflowAction.withNameIgnoreCase(action)
          } catch {
            case _: NoSuchElementException =>
              return BadUsageTermination(s"Unknown action ${args(0)}")
          }
        try {
          val wfMeta = WorkflowMeta(homeDir)
          val executor = WorkflowExecutor(wfMeta)
          val successMessage = executor.apply(wfAction)
          Success(successMessage)
        } catch {
          case e: Throwable =>
            JobMeta.writeError(homeDir, e)
            Failure(s"failure running ${action}", Some(e))
        }
      case _ =>
        BadUsageTermination()
    }
  }

  private val usageMessage =
    s"""|java -jar dxWDL.jar internal <action> <homedir> [options]
        |
        |Options:
        |    -traceLevel [0,1,2] How much debug information to write to the
        |                        job log at runtime. Zero means write the minimum,
        |                        one is the default, and two is for internal debugging.
        |    -streamAllFiles     Mount all files with dxfuse, do not use the download agent
        |""".stripMargin

  def main(args: Vector[String]): Unit = {
    terminate(dispatchCommand(args), usageMessage)
  }
}
