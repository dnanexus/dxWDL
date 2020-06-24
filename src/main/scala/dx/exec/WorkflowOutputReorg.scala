package dx.exec

import com.dnanexus.DXAPI
import com.fasterxml.jackson.databind.JsonNode
import dx.api._
import dx.core.io.DxPathConfig
import dx.core.languages.wdl.DxFileAccessProtocol
import dx.core.util.SysUtils
import dx.util.{JsUtils, Logger}
import spray.json.{JsBoolean, JsObject, JsValue}
import wdlTools.types.WdlTypes

case class WorkflowOutputReorg(wf: TAT.Workflow,
                               document: TAT.Document,
                               typeAliases: Map[String, WdlTypes.T],
                               dxPathConfig: DxPathConfig,
                               dxIoFunctions: DxFileAccessProtocol,
                               runtimeDebugLevel: Int) {
  private val verbose = runtimeDebugLevel >= 1

  // Efficiently get the names of many files. We
  // don't want to do a `describe` each one of them, instead,
  // we do a bulk-describe.
  //
  // In other words, this code is an efficient replacement for:
  // files.map(_.describe().getName())
  private def bulkGetFilenames(files: Seq[DxFile]): Vector[String] = {
    val info: Map[DxFile, DxFileDescribe] = DxFile.bulkDescribe(files.toVector)
    info.values.map(_.name).toVector
  }

  // find all output files from the analysis
  //
  // We want to find all inputs and outputs with little platform overhead.
  // Here, we use a describe API call on the analysis.
  //
  // The output files could include some of the inputs, and we need
  // to filter those out. One way, is to check file creation dates,
  // however, that would require a describe API call per output
  // file. Instead, we find all the output files that do not also
  // appear in the input.
  private def analysisFileOutputs(dxAnalysis: DxAnalysis): Vector[DxFile] = {
    val req = JsObject(
        "fields" -> JsObject("input" -> JsBoolean(true), "output" -> JsBoolean(true))
    )
    val rep = DXAPI.analysisDescribe(dxAnalysis.id, req, classOf[JsonNode])
    val repJs: JsValue = JsUtils.jsValueOfJsonNode(rep)
    val outputs = repJs.asJsObject.fields.get("output") match {
      case None    => throw new Exception("Failed to get analysis outputs")
      case Some(x) => x
    }
    val inputs = repJs.asJsObject.fields.get("input") match {
      case None    => throw new Exception("Failed to get analysis inputs")
      case Some(x) => x
    }

    val fileOutputs: Set[DxFile] = DxUtils.findDxFiles(outputs).toSet
    val fileInputs: Set[DxFile] = DxUtils.findDxFiles(inputs).toSet
    val realOutputs: Set[DxFile] = fileOutputs -- fileInputs
    Logger.appletLog(verbose, s"analysis has ${fileOutputs.size} output files")
    Logger.appletLog(verbose, s"analysis has ${fileInputs.size} input files")
    Logger.appletLog(verbose, s"analysis has ${realOutputs.size} real outputs")

    Logger.appletLog(verbose, "Checking timestamps")
    if (realOutputs.size > MAX_NUM_FILES_MOVE_LIMIT) {
      Logger.appletLog(
          verbose,
          s"WARNING: Large number of outputs (${realOutputs.size}), not moving objects"
      )
      return Vector.empty
    }
    val realFreshOutputs: Map[DxFile, DxFileDescribe] =
      DxFile.bulkDescribe(realOutputs.toVector)

    // Retain only files that were created AFTER the analysis started
    val anlCreateTs: java.util.Date = dxAnalysis.describe().getCreationDate
    val outputFiles: Vector[DxFile] = realFreshOutputs.flatMap {
      case (dxFile, desc) =>
        val creationDate = new java.util.Date(desc.created)
        if (creationDate.compareTo(anlCreateTs) >= 0)
          Some(dxFile)
        else
          None
    }.toVector
    Logger.appletLog(verbose, s"analysis has ${outputFiles.length} verified output files")

    outputFiles
  }

  // Move all intermediate results to a sub-folder
  def moveIntermediateResultFiles(exportFiles: Vector[DxFile]): Unit = {
    val dxEnv = DxUtils.dxEnv
    val dxProject = DxProject(dxEnv.getProjectContext)
    val dxProjDesc = dxProject.describe()
    val dxAnalysis = DxJob(dxEnv.getJob).describe().analysis.get
    val outFolder = dxAnalysis.describe().folder
    val intermFolder = outFolder + "/" + SysUtils.INTERMEDIATE_RESULTS_FOLDER
    Logger.appletLog(verbose, s"proj=${dxProjDesc.name} outFolder=${outFolder}")

    // find all analysis output files
    val analysisFiles: Vector[DxFile] = analysisFileOutputs(dxAnalysis)
    if (analysisFiles.isEmpty)
      return

    val exportIds: Set[String] = exportFiles.map(_.id).toSet
    val exportNames: Seq[String] = bulkGetFilenames(exportFiles)
    Logger.appletLog(verbose, s"exportFiles=${exportNames}")

    // Figure out which of the files should be kept
    val intermediateFiles = analysisFiles.filter(x => !(exportIds contains x.id))
    val iNames: Seq[String] = bulkGetFilenames(intermediateFiles)
    Logger.appletLog(verbose, s"intermediate files=${iNames}")
    if (intermediateFiles.isEmpty)
      return

    // Move all non exported results to the subdir. Do this in
    // a single API call, to improve performance.
    val folderContents: FolderContents = dxProject.listFolder(outFolder)
    Logger.appletLog(verbose, s"subfolders=${folderContents.subFolders}")
    if (!(folderContents.subFolders contains intermFolder)) {
      Logger.appletLog(verbose, s"Creating intermediate results sub-folder ${intermFolder}")
      dxProject.newFolder(intermFolder, parents = true)
    } else {
      Logger.appletLog(verbose, s"Intermediate results sub-folder ${intermFolder} already exists")
    }
    dxProject.moveObjects(intermediateFiles, intermFolder)
  }

  def apply(wfOutputFiles: Vector[DxFile]): Map[String, JsValue] = {
    Logger.appletLog(verbose, s"dxWDL version: ${getVersion}")

    // Reorganize directory structure
    moveIntermediateResultFiles(wfOutputFiles)

    // empty results
    Map.empty[String, JsValue]
  }
}
