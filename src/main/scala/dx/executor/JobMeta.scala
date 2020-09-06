package dx.executor

import java.nio.file.Path

import dx.{AppException, AppInternalException}
import dx.api.{DxApi, DxExecutable, DxJobDescribe, Field}
import dx.core.io.DxWorkerPaths
import spray.json._
import wdlTools.util.{FileUtils, JsUtils, Logger, TraceLevel}

abstract class JobMeta(homeDir: Path = DxWorkerPaths.HomeDir,
                       dxApi: DxApi = DxApi.get,
                       logger: Logger = Logger.get) {

  private val inputPath = homeDir.resolve(JobMeta.inputFile)
  private val outputPath = homeDir.resolve(JobMeta.outputFile)
  private val infoPath = homeDir.resolve(JobMeta.infoFile)

  lazy val jsInputs: Map[String, JsValue] =
    FileUtils.readFileContent(inputPath).parseJson.asJsObject.fields

  protected lazy val info: Map[String, JsValue] =
    FileUtils.readFileContent(infoPath).parseJson.asJsObject.fields

  lazy val jobDesc: DxJobDescribe = DxApi.get.currentJob.describe(
      Set(Field.Executable, Field.Details, Field.InstanceType)
  )

  lazy val executable: DxExecutable = info.get("executable") match {
    case None =>
      dxApi.logger.trace(
          "executable field not found locally, performing an API call.",
          minLevel = TraceLevel.None
      )
      jobDesc.executable.get
    case Some(JsString(x)) if x.startsWith("app-") =>
      dxApi.app(x)
    case Some(JsString(x)) if x.startsWith("applet-") =>
      dxApi.applet(x)
    case Some(other) =>
      throw new Exception(s"Malformed executable field ${other} in job info")
  }

  lazy val executableDetails: Map[String, JsValue] =
    executable.describe(Set(Field.Details)).details.get.asJsObject.fields

  def error(e: Throwable): Unit = {
    JobMeta.writeError(homeDir, e)
  }
}

object JobMeta {
  val inputFile = "job_input.json"
  val outputFile = "job_output.json"
  val errorFile = "job_error.json"
  val infoFile = "dnanexus-job.json"

  /**
    * Report an error, since this is called from a bash script, we can't simply
    * raise an exception. Instead, we write the error to a standard JSON file.
    * @param e the exception
    */
  def writeError(homeDir: Path, e: Throwable): Unit = {
    val jobErrorPath = homeDir.resolve(errorFile)
    val errType = e match {
      case _: AppException         => "AppError"
      case _: AppInternalException => "AppInternalError"
      case _: Throwable            => "AppInternalError"
    }
    // We are limited in what characters can be written to json, so we
    // provide a short description for json.
    //
    // Note: we sanitize this string, to be absolutely sure that
    // it does not contain problematic JSON characters.
    val errMsg = JsObject(
        "error" -> JsObject(
            "type" -> JsString(errType),
            "message" -> JsUtils.sanitizedString(e.getMessage)
        )
    ).prettyPrint
    FileUtils.writeFileContent(jobErrorPath, errMsg)
  }
}
