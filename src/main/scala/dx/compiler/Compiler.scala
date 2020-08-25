package dx.compiler

import java.nio.file.{Files, Path}

import com.typesafe.config.{Config, ConfigFactory}
import dx.api.{DxApi, DxFile, DxPath, DxProject, DxRecord, DxUtils, Field, InstanceTypeDbQuery}
import dx.compiler.ir.{DockerRegistry, Extras}
import dx.core.io.DxPathConfig
import dx.core.ir.Bundle
import dx.core.languages.wdl.ParameterLinkSerde
import spray.json.{JsObject, JsString, JsValue}
import wdlTools.util.{FileSourceResolver, FileUtils, JsUtils, Logger}

import scala.jdk.CollectionConverters._

case class CompilerResults(primaryCallable: Option[ExecRecord], execDict: Map[String, ExecRecord]) {
  def executableIds: Vector[String] = {
    primaryCallable match {
      case None      => execDict.values.map(_.dxExec.getId).toVector
      case Some(obj) => Vector(obj.dxExec.id)
    }
  }
}

/**
  * Compile IR to native applets and workflows.
  * @param extras extra configuration
  * @param runtimePathConfig path configuration on the runtime environment
  * @param runtimeTraceLevel trace level to use at runtime
  * @param includeAsset whether to package the runtime asset with generated applications
  * @param archive whether to archive existing applications
  * @param force whether to overwrite existing applications
  * @param leaveWorkflowsOpen whether to leave generated workflows in the open state
  * @param locked whether to generate locked workflows
  * @param projectWideReuse whether to allow project-wide reuse of applications
  * @param fileResolver the FileSourceResolver
  * @param dxApi the DxApi
  * @param logger the Logge
  */
case class Compiler(extras: Option[Extras],
                    runtimePathConfig: DxPathConfig,
                    runtimeTraceLevel: Int,
                    includeAsset: Boolean,
                    archive: Boolean,
                    force: Boolean,
                    leaveWorkflowsOpen: Boolean,
                    locked: Boolean,
                    projectWideReuse: Boolean,
                    fileResolver: FileSourceResolver = FileSourceResolver.get,
                    dxApi: DxApi = DxApi.get,
                    logger: Logger = Logger.get) {
  // logger for extra trace info
  private val logger2: Logger = dxApi.logger.withTraceIfContainsKey("Native")
  // query interface for fetching instance types by user/org
  private lazy val instanceTypeDbQuery: InstanceTypeDbQuery = InstanceTypeDbQuery(dxApi)
  // create a temp dir where applications will be compiled, and make sure it's
  // deleted on shutdown
  private lazy val appCompileDirPath: Path = {
    val p = Files.createTempDirectory("dxWDL_Compile")
    sys.addShutdownHook({
      FileUtils.deleteRecursive(p)
    })
    p
  }
  // Are we setting up a private docker registry?
  private val dockerRegistryInfo: Option[DockerRegistry] = extras match {
    case None => None
    case Some(extras) =>
      extras.dockerRegistry match {
        case None    => None
        case Some(x) => Some(x)
      }
  }

  private def getDxWdlRuntimeRecord(project: DxProject): Option[DxRecord] = {
    // get billTo and region from the project, then find the runtime asset
    // in the current region.
    val projectRegion = project.describe(Set(Field.Region)).region match {
      case Some(s) => s
      case None    => throw new Exception(s"Cannot get region for project ${project}")
    }
    // Find the runtime dxWDL asset with the correct version. Look inside the
    // project configured for this region. The regions live in dxWDL.conf
    val config = ConfigFactory.load(DxWdlRuntimeConfigFile)
    val regionToProjectOption: Vector[Config] =
      config.getConfigList("dxWDL.region2project").asScala.toVector
    val regionToProjectConf: Map[String, String] = regionToProjectOption.map { pair =>
      val region = pair.getString("region")
      val project = pair.getString("path")
      region -> project
    }.toMap
    // The mapping from region to project name is list of (region, proj-name) pairs.
    // Get the project for this region.
    val destination = regionToProjectConf.get(projectRegion) match {
      case None       => throw new Exception(s"Region ${projectRegion} is currently unsupported")
      case Some(dest) => dest
    }
    val destRegexp = "(?:(.*):)?(.+)".r
    val (regionalProjectName, folder) = destination match {
      case destRegexp(null, project)   => (project, "/")
      case destRegexp(project, folder) => (project, folder)
      case _ =>
        throw new Exception(s"Bad syntax for destination ${destination}")
    }
    val regionalProject = dxApi.resolveProject(regionalProjectName)
    val assetUri = s"${DxPath.DxUriPrefix}${regionalProject.getId}:${folder}/${DxWdlAsset}"
    logger.trace(s"Looking for asset id at ${assetUri}")
    val dxAsset = dxApi.resolveOnePath(assetUri, Some(regionalProject)) match {
      case dxFile: DxRecord => dxFile
      case other =>
        throw new Exception(s"Found dx object of wrong type ${other} at ${assetUri}")
    }
    // We need the dxWDL runtime library cloned into this project, so it will
    // be available to all subjobs we run.
    dxApi.cloneAsset(dxAsset, project, DxWdlAsset, regionalProject)
    Some(dxAsset)
  }

  private def getRuntimeAsset(record: DxRecord): JsValue = {
    // Extract the archive from the details field
    val desc = record.describe(Set(Field.Details))
    val dxLink =
      try {
        JsUtils.get(desc.details.get, Some("archiveFileId"))
      } catch {
        case _: Throwable =>
          throw new Exception(
              s"record does not have an archive field ${desc.details}"
          )
      }
    val dxFile = DxFile.fromJsValue(dxApi, dxLink)
    JsObject(
        "name" -> JsString(dxFile.describe().name),
        "id" -> JsObject(DxUtils.DxLinkKey -> JsString(dxFile.id))
    )
  }

  /**
    * Compile the IR bundle to a native applet or workflow.
    * @param bundle the IR bundle
    * @param project the destination project
    * @param folder the destination folder
    */
  def apply(bundle: Bundle, project: DxProject, folder: String): CompilerResults = {
    val wdlVarLinksConverter =
      ParameterLinkSerde(dxApi, fileResolver, dxFileDescCache, bundle2.typeAliases)
    // database of available instance types for the user/org that owns the project
    val instanceTypeDb = instanceTypeDbQuery.query(project)
    // directory of the currently existing applets - we don't want to build them
    // if we don't have to.
    val dataObjDir = DxDataObjectDirectory(bundle, project, folder, projectWideReuse, dxApi, logger)
    val runtimeAssetRecord: Option[DxRecord] = if (includeAsset) {
      getDxWdlRuntimeRecord(project)
    } else {
      None
    }
    val runtimeAsset = runtimeAssetRecord.map(getRuntimeAsset)
  }
}
