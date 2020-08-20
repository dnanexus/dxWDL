package dx.compiler

import com.typesafe.config.{Config, ConfigFactory}
import dx.api.{DxApi, DxPath, DxProject, DxRecord, Field, InstanceTypeDbQuery}
import dx.compiler.ExecTreeFormat.ExecTreeFormat
import dx.compiler.Main.CompilerFlag
import dx.compiler.ir.{Bundle, Extras}
import dx.core.io.DxPathConfig
import dx.core.languages.wdl.WdlVarLinksConverter
import spray.json.JsValue
import wdlTools.util.{Enum, FileSourceResolver, Logger}

import scala.jdk.CollectionConverters._

// Tree printer types for the execTree option
object ExecTreeFormat extends Enum {
  type ExecTreeFormat = Value
  val Json, Pretty = Value
}

case class Compiler(includeAsset: Boolean,
                    extras: Option[Extras[_]],
                    execTree: Option[ExecTreeFormat],
                    runtimeTraceLevel: Int,
                    archive: Boolean,
                    force: Boolean,
                    leaveWorkflowsOpen: Boolean,
                    locked: Boolean,
                    projectWideReuse: Boolean,
                    runtimePathConfig: DxPathConfig,
                    fileResolver: FileSourceResolver = FileSourceResolver.get,
                    dxApi: DxApi = DxApi.get,
                    logger: Logger = Logger.get) {

  // The mapping from region to project name is list of (region, proj-name) pairs.
  // Get the project for this region.
  private def getProjectWithRuntimeLibrary(region2project: Map[String, String],
                                           region: String): (String, String) = {
    val destination = region2project.get(region) match {
      case None       => throw new Exception(s"Region ${region} is currently unsupported")
      case Some(dest) => dest
    }

    // The destionation is something like:
    val parts = destination.split(":")
    if (parts.length == 1) {
      (parts(0), "/")
    } else if (parts.length == 2) {
      (parts(0), parts(1))
    } else {
      throw new Exception(s"Bad syntax for destination ${destination}")
    }
  }

  // the regions live in dxWDL.conf
  def getRegions: Map[String, String] = {
    val config = ConfigFactory.load(DxWdlRuntimeConfigFile)
    val l: Vector[Config] = config.getConfigList("dxWDL.region2project").asScala.toVector
    val region2project: Map[String, String] = l.map { pair =>
      val r = pair.getString("region")
      val projName = pair.getString("path")
      r -> projName
    }.toMap
    region2project
  }

  // Find the runtime dxWDL asset with the correct version. Look inside the
  // project configured for this region.
  private def getAssetId(region: String): String = {
    val region2project = getRegions
    val (projNameRt, folder) = getProjectWithRuntimeLibrary(region2project, region)
    val dxProjRt = dxApi.resolveProject(projNameRt)
    logger.trace(s"Looking for asset-id in ${projNameRt}:/${folder}")

    val assetDxPath = s"${DxPath.DxUriPrefix}${dxProjRt.getId}:${folder}/${DxWdlAsset}"
    val dxObj = dxApi.resolveOnePath(assetDxPath, Some(dxProjRt))
    if (!dxObj.isInstanceOf[DxRecord])
      throw new Exception(s"Found dx object of wrong type ${dxObj} at ${assetDxPath}")
    dxObj.getId
  }

  // We need the dxWDL runtime library cloned into this project, so it will
  // be available to all subjobs we run.
  private def cloneRtLibraryToProject(region: String,
                                      dxWDLrtId: String,
                                      dxProject: DxProject): Unit = {
    val region2project = getRegions
    val (projNameRt, _) = getProjectWithRuntimeLibrary(region2project, region)
    val dxProjRt = dxApi.resolveProject(projNameRt)
    dxApi.cloneAsset(dxApi.record(dxWDLrtId), dxProject, DxWdlAsset, dxProjRt)
  }

  // Backend compiler pass
  private def compileNative(
      bundle: IR.Bundle,
      folder: String,
      dxProject: DxProject,
      runtimePathConfig: DxPathConfig,
      wdlVarLinksConverter: WdlVarLinksConverter
  ): Native.Results = {
    val dxWDLrtId: Option[String] = compileMode match {
      case CompilerFlag.IR =>
        throw new Exception("Invalid value IR for compilation mode")
      case CompilerFlag.NativeWithoutRuntimeAsset =>
        // Testing mode, we don't need the runtime library to check native
        // compilation.
        None
      case CompilerFlag.All =>
        // get billTo and region from the project, then find the runtime asset
        // in the current region.
        val region = dxProject.describe(Set(Field.Region)).region match {
          case Some(s) => s
          case None    => throw new Exception(s"Cannot get region for project ${dxProject}")
        }
        val lrtId = getAssetId(region)
        cloneRtLibraryToProject(region, lrtId, dxProject)
        Some(lrtId)
    }
    // get list of available instance types
    val instanceTypeDB = InstanceTypeDbQuery(dxApi).query(dxProject)

    // Efficiently build a directory of the currently existing applets.
    // We don't want to build them if we don't have to.
    val dxObjDir = DxObjectDirectory(bundle, dxProject, folder, projectWideReuse, dxApi)

    // Generate dx:applets and dx:workflow from the IR
    Native(
        dxWDLrtId,
        folder,
        dxProject,
        dxObjDir,
        instanceTypeDB,
        runtimePathConfig,
        wdlVarLinksConverter,
        bundle.typeAliases,
        extras,
        runtimeTraceLevel,
        leaveWorkflowsOpen,
        force,
        archive,
        locked,
        dxApi
    ).apply(bundle)
  }

  // Compile up to native dx applets and workflows
  def apply(bundle: Bundle,
            folder: String,
            dxProject: DxProject): (String, Option[Either[String, JsValue]]) = {

    // Up to this point, compilation does not require
    // the dx:project. This allows unit testing without
    // being logged in to the platform. For the native
    // pass the dx:project is required to establish
    // (1) the instance price list and database
    // (2) the output location of applets and workflows
    val wdlVarLinksConverter =
      WdlVarLinksConverter(dxApi, fileResolver, dxFileDescCache, bundle2.typeAliases)
    val cResults =
      compileNative(bundle2, folder, dxProject, runtimePathConfig, wdlVarLinksConverter)
    cResults.primaryCallable match {
      case None =>
        val ids = cResults.execDict.map { case (_, r) => r.dxExec.getId }.mkString(",")
        (ids, None)
      case Some(wf) if execTree.isDefined =>
        cResults.primaryCallable match {
          case None =>
            (wf.dxExec.getId, None)
          case Some(primary) =>
            val tree = new ExecTree(cResults.execDict)
            val treeRepr = execTree.get match { // Safe get because we check isDefined above
              case PrettyTreePrinter =>
                Left(
                    ExecTree.generateTreeFromJson(tree.apply(primary).asJsObject)
                )
              case JsonTreePrinter => Right(tree.apply(primary)) // Convert to string
            }
            (wf.dxExec.getId, Some(treeRepr))
        }
      case Some(wf) =>
        (wf.dxExec.getId, None)
    }
  }
}
