package dx.exec

import com.dnanexus.DXAPI
import com.fasterxml.jackson.databind.JsonNode
import dx.api._
import dx.exec
import dx.util.{JsUtils, getVersion}
import spray.json.{JsBoolean, JsObject, JsValue}

case class WorkflowOutputReorg(dxApi: DxApi) {
  // Efficiently get the names of many files. We
  // don't want to do a `describe` each one of them, instead,
  // we do a bulk-describe.
  //
  // In other words, this code is an efficient replacement for:
  // files.map(_.describe().getName())
  private def bulkGetFilenames(files: Seq[DxFile]): Vector[String] = {
    val info: Map[DxFile, DxFileDescribe] = dxApi.fileBulkDescribe(files.toVector)
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

    val fileOutputs: Set[DxFile] = dxApi.findFiles(outputs).toSet
    val fileInputs: Set[DxFile] = dxApi.findFiles(inputs).toSet
    val realOutputs: Set[DxFile] = fileOutputs -- fileInputs
    dxApi.logger.traceLimited(s"analysis has ${fileOutputs.size} output files")
    dxApi.logger.traceLimited(s"analysis has ${fileInputs.size} input files")
    dxApi.logger.traceLimited(s"analysis has ${realOutputs.size} real outputs")

    dxApi.logger.traceLimited("Checking timestamps")
    if (realOutputs.size > exec.MAX_NUM_FILES_MOVE_LIMIT) {
      dxApi.logger.traceLimited(
          s"WARNING: Large number of outputs (${realOutputs.size}), not moving objects"
      )
      return Vector.empty
    }
    val realFreshOutputs: Map[DxFile, DxFileDescribe] =
      dxApi.fileBulkDescribe(realOutputs.toVector)

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
    dxApi.logger.traceLimited(s"analysis has ${outputFiles.length} verified output files")

    outputFiles
  }

  // Move all intermediate results to a sub-folder
  def moveIntermediateResultFiles(exportFiles: Vector[DxFile]): Unit = {
    val dxProject = dxApi.currentProject
    val dxProjDesc = dxProject.describe()
    val dxAnalysis = dxApi.currentJob.describe().analysis.get
    val outFolder = dxAnalysis.describe().folder
    val intermFolder = outFolder + "/" + exec.INTERMEDIATE_RESULTS_FOLDER
    dxApi.logger.traceLimited(s"proj=${dxProjDesc.name} outFolder=${outFolder}")

    // find all analysis output files
    val analysisFiles: Vector[DxFile] = analysisFileOutputs(dxAnalysis)
    if (analysisFiles.isEmpty)
      return

    val exportIds: Set[String] = exportFiles.map(_.id).toSet
    val exportNames: Seq[String] = bulkGetFilenames(exportFiles)
    dxApi.logger.traceLimited(s"exportFiles=${exportNames}")

    // Figure out which of the files should be kept
    val intermediateFiles = analysisFiles.filter(x => !(exportIds contains x.id))
    val iNames: Seq[String] = bulkGetFilenames(intermediateFiles)
    dxApi.logger.traceLimited(s"intermediate files=${iNames}")
    if (intermediateFiles.isEmpty)
      return

    // Move all non exported results to the subdir. Do this in
    // a single API call, to improve performance.
    val folderContents: FolderContents = dxProject.listFolder(outFolder)
    dxApi.logger.traceLimited(s"subfolders=${folderContents.subFolders}")
    if (!(folderContents.subFolders contains intermFolder)) {
      dxApi.logger.traceLimited(s"Creating intermediate results sub-folder ${intermFolder}")
      dxProject.newFolder(intermFolder, parents = true)
    } else {
      dxApi.logger.traceLimited(s"Intermediate results sub-folder ${intermFolder} already exists")
    }
    dxProject.moveObjects(intermediateFiles, intermFolder)
  }

  def apply(wfOutputFiles: Vector[DxFile]): Map[String, JsValue] = {
    dxApi.logger.traceLimited(s"dxWDL version: ${getVersion}")

    // Reorganize directory structure
    moveIntermediateResultFiles(wfOutputFiles)

    // empty results
    Map.empty[String, JsValue]
  }
}
