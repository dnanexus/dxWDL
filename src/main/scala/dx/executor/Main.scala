package dx.executor

import java.nio.file.{InvalidPathException, Paths}

import dx.core.io.DxWorkerPaths
import dx.core.util.MainUtils._

object Main {
  private val CommonOptions: InternalOptions = Map(
      "streamAllFiles" -> FlagOptionSpec.Default
  )

  private[executor] def dispatchCommand(args: Vector[String]): Termination = {
    if (args.size < 2) {
      return BadUsageTermination()
    }
    val action =
      try {
        ExecutorAction.withNameIgnoreCase(args(0).replaceAll("_", ""))
      } catch {
        case _: NoSuchElementException =>
          return BadUsageTermination(s"Unknown action ${args(0)}")
      }
    val homeDir =
      try {
        Paths.get(args(1))
      } catch {
        case _: InvalidPathException =>
          return BadUsageTermination(s"${args(1)} is not a valid home directory")
      }
    val options =
      try {
        parseCommandLine(args, CommonOptions)
      } catch {
        case e: OptionParseException =>
          return BadUsageTermination("Error parsing command line options", Some(e))
      }
    val logger = initLogger(options)
    val streamAllFiles = options.getFlag("streamAllFiles")

    try {
      // parse the job meta files (inputs, outputs, etc)
      val jobMeta = JobMeta(homeDir)
      // Setup the standard paths used for applets. These are used at runtime, not at compile time.
      // On the cloud instance running the job, the user is "dnanexus", and the home directory is
      // "/home/dnanexus".
      val workerPaths = DxWorkerPaths(streamAllFiles, logger)
      // TODO: swap this out for a parallelized version
      val fileUploader = SerialFileUploader()
      val executor = Executor(jobMeta, workerPaths, fileUploader)
      val successMessage = executor.apply(action)
      Success(successMessage)
    } catch {
      case e: Throwable =>
        JobMeta.writeError(homeDir, e)
        Failure(s"failure running ${action}", Some(e))
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

  def main(args: Seq[String]): Unit = {
    terminate(dispatchCommand(args.toVector), usageMessage)
  }
}
