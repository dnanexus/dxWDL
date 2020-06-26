package dx.compiler

import java.nio.file.{Path, Paths}

import com.typesafe.config.{Config, ConfigFactory}
import dx.api.{
  DxApi,
  DxFile,
  DxFileDescribe,
  DxPath,
  DxProject,
  DxRecord,
  Field,
  InstanceTypeDbQuery
}
import dx.core.io.DxPathConfig
import dx.core.languages.wdl.ParseSource
import dx.core.util.SysUtils
import spray.json.JsValue
import wdlTools.types.{TypedAbstractSyntax => TAT}

import scala.jdk.CollectionConverters._

object CompilerFlag extends Enumeration {
  type CompilerFlag = Value
  val All, IR, NativeWithoutRuntimeAsset = Value
}

// Tree printer types for the execTree option
sealed trait TreePrinter
case object JsonTreePrinter extends TreePrinter
case object PrettyTreePrinter extends TreePrinter

// Packing of all compiler flags in an easy to digest format
case class CompilerOptions(archive: Boolean,
                           compileMode: CompilerFlag.Value,
                           defaults: Option[Path],
                           extras: Option[Extras],
                           fatalValidationWarnings: Boolean,
                           force: Boolean,
                           importDirs: List[Path],
                           inputs: List[Path],
                           leaveWorkflowsOpen: Boolean,
                           locked: Boolean,
                           projectWideReuse: Boolean,
                           reorg: Boolean,
                           streamAllFiles: Boolean,
                           execTree: Option[TreePrinter],
                           runtimeTraceLevel: Int,
                           dxApi: DxApi)

case class Top(cOpt: CompilerOptions) {
  private val dxApi = cOpt.dxApi

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
    val config = ConfigFactory.load(DX_WDL_RUNTIME_CONF_FILE)
    val l: List[Config] = config.getConfigList("dxWDL.region2project").asScala.toList
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
    dxApi.logger.trace(s"Looking for asset-id in ${projNameRt}:/${folder}")

    val assetDxPath = s"${DxPath.DX_URL_PREFIX}${dxProjRt.getId}:${folder}/${DX_WDL_ASSET}"
    val dxObj = dxApi.resolveOnePath(assetDxPath, dxProjRt)
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
    dxApi.cloneAsset(dxApi.record(dxWDLrtId), dxProject, DX_WDL_ASSET, dxProjRt)
  }

  // Backend compiler pass
  private def compileNative(
      bundle: IR.Bundle,
      folder: String,
      dxProject: DxProject,
      runtimePathConfig: DxPathConfig,
      fileInfoDir: Map[String, (DxFile, DxFileDescribe)]
  ): Native.Results = {
    val dxWDLrtId: Option[String] = cOpt.compileMode match {
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
    val dxObjDir = DxObjectDirectory(bundle, dxProject, folder, cOpt.projectWideReuse, dxApi)

    // Generate dx:applets and dx:workflow from the IR
    Native(
        dxWDLrtId,
        folder,
        dxProject,
        dxObjDir,
        instanceTypeDB,
        runtimePathConfig,
        fileInfoDir,
        bundle.typeAliases,
        cOpt.extras,
        cOpt.runtimeTraceLevel,
        cOpt.leaveWorkflowsOpen,
        cOpt.force,
        cOpt.archive,
        cOpt.locked,
        cOpt.dxApi
    ).apply(bundle)
  }

  // check the declarations in [graph], and make sure they
  // do not contain the reserved '___' substring.
  private def checkDeclarations(varNames: Vector[String]): Unit = {
    for (varName <- varNames)
      if (varName contains "___")
        throw new Exception(s"Variable ${varName} is using the reserved substring ___")
  }

  // check that streaming annotations are only done for files.
  private def validate(callable: TAT.Callable): Unit = {
    callable match {
      case wf: TAT.Workflow =>
        if (wf.parameterMeta.isDefined) {
          dxApi.logger.warning("dxWDL workflows ignore their parameter meta section")
        }
        checkDeclarations(wf.inputs.map(_.name))
        checkDeclarations(wf.outputs.map(_.name))
        val allDeclarations: Vector[TAT.Declaration] = wf.body.collect {
          case d: TAT.Declaration => d
        }
        checkDeclarations(allDeclarations.map(_.name))

      case task: TAT.Task =>
        checkDeclarations(task.inputs.map(_.name))
        checkDeclarations(task.outputs.map(_.name))
    }
  }

  // Scan the JSON inputs files for dx:files, and batch describe them. This
  // reduces the number of API calls.
  private def bulkFileDescribe(
      bundle: IR.Bundle,
      dxProject: DxProject
  ): (Map[String, DxFile], Map[String, (DxFile, DxFileDescribe)]) = {
    val defResults: InputFileScanResults = cOpt.defaults match {
      case None => InputFileScanResults(Map.empty, Vector.empty)
      case Some(path) =>
        InputFileScan(bundle, dxProject, dxApi).apply(path)
    }

    val allResults: InputFileScanResults = cOpt.inputs.foldLeft(defResults) {
      case (accu: InputFileScanResults, inputFilePath) =>
        val res = InputFileScan(bundle, dxProject, dxApi).apply(inputFilePath)
        InputFileScanResults(accu.path2file ++ res.path2file, accu.dxFiles ++ res.dxFiles)
    }

    val allDescribe = dxApi.fileBulkDescribe(allResults.dxFiles)
    val allFiles: Map[String, (DxFile, DxFileDescribe)] = allDescribe.map {
      case (f: DxFile, desc) => f.id -> (f, desc)
      case _                 => throw new Exception("has to be all files")
    }
    (allResults.path2file, allFiles)
  }

  private def wdlToIR(source: Path): IR.Bundle = {
    val (language, everythingBundle, allSources, adjunctFiles) =
      ParseSource(dxApi.logger).apply(source, cOpt.importDirs)

    // validate
    everythingBundle.allCallables.foreach { case (_, c) => validate(c) }
    everythingBundle.primaryCallable match {
      case None    => ()
      case Some(x) => validate(x)
    }

    // Compile the WDL workflow into an Intermediate
    // Representation (IR)
    val defaultRuntimeAttrs = cOpt.extras match {
      case None     => WdlRuntimeAttrs(Map.empty)
      case Some(ex) => ex.defaultRuntimeAttributes
    }

    val reorgApp: Either[Boolean, ReorgAttrs] = cOpt.extras match {

      case None => Left(cOpt.reorg)
      case Some(ex) =>
        ex.customReorgAttributes match {
          case None       => Left(cOpt.reorg)
          case Some(cOrg) => Right(cOrg)
        }

    }

    // TODO: load default hints from attrs
    val defaultHintAttrs = WdlHintAttrs(Map.empty)
    GenerateIR(dxApi, defaultRuntimeAttrs, defaultHintAttrs)
      .apply(everythingBundle, allSources, language, cOpt.locked, reorgApp, adjunctFiles)
  }

  // Compile IR only
  private def handleInputFiles(bundle: IR.Bundle,
                               path2file: Map[String, DxFile],
                               fileInfoDir: Map[String, (DxFile, DxFileDescribe)]): IR.Bundle = {
    val bundle2: IR.Bundle = cOpt.defaults match {
      case None => bundle
      case Some(path) =>
        InputFile(fileInfoDir, path2file, bundle.typeAliases, dxApi)
          .embedDefaults(bundle, path)
    }

    // generate dx inputs from the Cromwell-style input specification.
    cOpt.inputs.foreach { path =>
      val dxInputs = InputFile(fileInfoDir, path2file, bundle.typeAliases, dxApi)
        .dxFromCromwell(bundle2, path)
      // write back out as xxxx.dx.json
      val filename = SysUtils.replaceFileSuffix(path, ".dx.json")
      val parent = path.getParent
      val dxInputFile =
        if (parent != null) parent.resolve(filename)
        else Paths.get(filename)
      SysUtils.writeFileContent(dxInputFile, dxInputs.prettyPrint)
      dxApi.logger.trace(s"Wrote dx JSON input file ${dxInputFile}")
    }
    bundle2
  }

  // compile and generate intermediate code only
  def applyOnlyIR(source: Path, dxProject: DxProject): IR.Bundle = {
    // generate IR
    val bundle: IR.Bundle = wdlToIR(source)

    // lookup platform files in bulk
    val (path2dxFile, fileInfoDir) = bulkFileDescribe(bundle, dxProject)

    // handle changes resulting from setting defaults, and
    // generate DNAx input files.
    handleInputFiles(bundle, path2dxFile, fileInfoDir)
  }

  // Compile up to native dx applets and workflows
  def apply(source: Path,
            folder: String,
            dxProject: DxProject,
            runtimePathConfig: DxPathConfig,
            execTree: Option[TreePrinter]): (String, Option[Either[String, JsValue]]) = {
    val bundle: IR.Bundle = wdlToIR(source)

    // lookup platform files in bulk
    val (path2dxFile, fileInfoDir) = bulkFileDescribe(bundle, dxProject)

    // generate IR
    val bundle2: IR.Bundle = handleInputFiles(bundle, path2dxFile, fileInfoDir)

    // Up to this point, compilation does not require
    // the dx:project. This allows unit testing without
    // being logged in to the platform. For the native
    // pass the dx:project is required to establish
    // (1) the instance price list and database
    // (2) the output location of applets and workflows
    val cResults = compileNative(bundle2, folder, dxProject, runtimePathConfig, fileInfoDir)
    cResults.primaryCallable match {
      case None =>
        val ids = cResults.execDict.map { case (_, r) => r.dxExec.getId }.mkString(",")
        (ids, None)
      case Some(wf) if execTree.isDefined =>
        cResults.primaryCallable match {
          case None =>
            (wf.dxExec.getId, None)
          case Some(primary) =>
            val tree = new Tree(cResults.execDict)
            val treeRepr = execTree.get match { // Safe get because we check isDefined above
              case PrettyTreePrinter =>
                Left(
                    Tree.generateTreeFromJson(tree.apply(primary).asJsObject)
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
