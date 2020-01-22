// Interface to the compilation tool chain. The methods here are the only ones
// that should be called to perform compilation.

package dxWDL.compiler

import java.nio.file.{Path, Paths}
import spray.json._

import wom.callable._
import wom.executable.WomBundle
import wom.graph.expression._

import dxWDL.base._
import dxWDL.base.Utils.{DX_URL_PREFIX, DX_WDL_ASSET}
import dxWDL.dx._
import dxWDL.util._

case class Top(cOpt: CompilerOptions) {
  val verbose = cOpt.verbose

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

  // Find the runtime dxWDL asset with the correct version. Look inside the
  // project configured for this region.
  private def getAssetId(region: String): String = {
    val region2project = Utils.getRegions()
    val (projNameRt, folder) = getProjectWithRuntimeLibrary(region2project, region)
    val dxProjRt = DxPath.resolveProject(projNameRt)
    Utils.trace(verbose.on, s"Looking for asset-id in ${projNameRt}:/${folder}")

    val assetDxPath = s"${DX_URL_PREFIX}${dxProjRt.getId}:${folder}/${DX_WDL_ASSET}"
    val dxObj = DxPath.resolveOnePath(assetDxPath, dxProjRt)
    if (!dxObj.isInstanceOf[DxRecord])
      throw new Exception(s"Found dx object of wrong type ${dxObj} at ${assetDxPath}")
    dxObj.getId
  }

  // We need the dxWDL runtime library cloned into this project, so it will
  // be available to all subjobs we run.
  private def cloneRtLibraryToProject(region: String,
                                      dxWDLrtId: String,
                                      dxProject: DxProject): Unit = {
    val region2project = Utils.getRegions()
    val (projNameRt, folder) = getProjectWithRuntimeLibrary(region2project, region)
    val dxProjRt = DxPath.resolveProject(projNameRt)
    DxUtils.cloneAsset(DxRecord.getInstance(dxWDLrtId), dxProject, DX_WDL_ASSET, dxProjRt, verbose)
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
        val (billTo, region) = DxUtils.projectDescribeExtraInfo(dxProject)
        val lrtId = getAssetId(region)
        cloneRtLibraryToProject(region, lrtId, dxProject)
        Some(lrtId)
    }
    // get list of available instance types
    val instanceTypeDB = InstanceTypeDB.query(dxProject, verbose)

    // Efficiently build a directory of the currently existing applets.
    // We don't want to build them if we don't have to.
    val dxObjDir = DxObjectDirectory(bundle, dxProject, folder, cOpt.projectWideReuse, verbose)

    // Generate dx:applets and dx:workflow from the IR
    new Native(dxWDLrtId,
               folder,
               dxProject,
               dxObjDir,
               instanceTypeDB,
               runtimePathConfig,
               fileInfoDir,
               bundle.typeAliases,
               cOpt.extras,
               cOpt.runtimeDebugLevel,
               cOpt.leaveWorkflowsOpen,
               cOpt.force,
               cOpt.archive,
               cOpt.locked,
               cOpt.verbose).apply(bundle)
  }

  // check the declarations in [graph], and make sure they
  // do not contain the reserved '___' substring.
  private def checkDeclarations(varNames: Seq[String]): Unit = {
    for (varName <- varNames)
      if (varName contains "___")
        throw new Exception(s"Variable ${varName} is using the reserved substring ___")
  }

  // check that streaming annotations are only done for files.
  private def validate(callable: Callable): Unit = {
    callable match {
      case wf: WorkflowDefinition =>
        if (wf.parameterMeta.size > 0)
          Utils.warning(verbose, "dxWDL workflows ignore their parameter meta section")
        val g = wf.innerGraph
        checkDeclarations(g.inputNodes.map(_.localName).toSeq)
        checkDeclarations(g.outputNodes.map(_.localName).toSeq)
        val allDeclarations = g.allNodes.filter(_.isInstanceOf[ExposedExpressionNode])
        checkDeclarations(allDeclarations.map(_.identifier.localName.value).toSeq)

      case task: CallableTaskDefinition =>
        checkDeclarations(task.inputs.map(_.name).toSeq)
        checkDeclarations(task.outputs.map(_.name).toSeq)

      case other =>
        throw new Exception(s"Unexpected object ${other}")
    }
  }

  // Check the uniqueness of tasks, Workflows, and Types
  // merge everything into one bundle.
  private def mergeIntoOneBundle(mainBundle: WomBundle,
                                 subBundles: Vector[WomBundle]): WomBundle = {
    var allCallables = mainBundle.allCallables
    var allTypeAliases = mainBundle.typeAliases

    subBundles.foreach { subBund =>
      subBund.allCallables.foreach {
        case (key, callable) =>
          allCallables.get(key) match {
            case None =>
              allCallables = allCallables + (key -> callable)

            // The comparision is done with "toString", because otherwise two
            // identical definitions are somehow, through the magic of Scala,
            // unequal.
            case Some(existing) if (existing.toString != callable.toString) =>
              Utils.error(s"""|${key} appears with two different callable definitions
                              |1)
                              |${callable}
                              |
                              |2)
                              |${existing}
                              |""".stripMargin)
              throw new Exception(s"${key} appears twice, with two different definitions")
            case _ => ()
          }
      }
      subBund.typeAliases.foreach {
        case (key, definition) =>
          allTypeAliases.get(key) match {
            case None =>
              allTypeAliases = allTypeAliases + (key -> definition)
            case Some(existing) if (existing != definition) =>
              Utils.error(s"""|${key} appears twice, with two different definitions
                              |1)
                              |${definition}
                              |
                              |2)
                              |${existing}
                              |""".stripMargin)
              throw new Exception(s"${key} type alias appears twice")
            case _ => ()
          }
      }
    }

    // Merge all the bundles together
    WomBundle(mainBundle.primaryCallable, allCallables, allTypeAliases, Set.empty)
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
        InputFileScan(bundle, dxProject, verbose).apply(path)
    }

    val allResults: InputFileScanResults = cOpt.inputs.foldLeft(defResults) {
      case (accu: InputFileScanResults, inputFilePath) =>
        val res = InputFileScan(bundle, dxProject, verbose).apply(inputFilePath)
        InputFileScanResults(accu.path2file ++ res.path2file, accu.dxFiles ++ res.dxFiles)
    }

    val allDescribe = DxFile.bulkDescribe(allResults.dxFiles)
    val allFiles: Map[String, (DxFile, DxFileDescribe)] = allDescribe.map {
      case (f: DxFile, desc) => f.id -> (f, desc)
      case _                 => throw new Exception("has to be all files")
    }.toMap
    (allResults.path2file, allFiles)
  }

  private def womToIR(source: Path): IR.Bundle = {
    val (language, womBundle, allSources, subBundles) =
      ParseWomSourceFile(verbose.on).apply(source, cOpt.importDirs)

    // Check that each workflow/task appears just one
    val everythingBundle: WomBundle = mergeIntoOneBundle(womBundle, subBundles)

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

    new GenerateIR(cOpt.verbose, defaultRuntimeAttrs)
      .apply(everythingBundle, allSources, language, cOpt.locked, reorgApp)
  }

  // Compile IR only
  private def handleInputFiles(bundle: IR.Bundle,
                               path2file: Map[String, DxFile],
                               fileInfoDir: Map[String, (DxFile, DxFileDescribe)]): IR.Bundle = {
    val bundle2: IR.Bundle = cOpt.defaults match {
      case None => bundle
      case Some(path) =>
        InputFile(fileInfoDir, path2file, bundle.typeAliases, cOpt.verbose)
          .embedDefaults(bundle, path)
    }

    // generate dx inputs from the Cromwell-style input specification.
    cOpt.inputs.foreach { path =>
      val dxInputs = InputFile(fileInfoDir, path2file, bundle.typeAliases, cOpt.verbose)
        .dxFromCromwell(bundle2, path)
      // write back out as xxxx.dx.json
      val filename = Utils.replaceFileSuffix(path, ".dx.json")
      val parent = path.getParent
      val dxInputFile =
        if (parent != null) parent.resolve(filename)
        else Paths.get(filename)
      Utils.writeFileContent(dxInputFile, dxInputs.prettyPrint)
      Utils.trace(cOpt.verbose.on, s"Wrote dx JSON input file ${dxInputFile}")
    }
    bundle2
  }

  // compile and generate intermediate code only
  def applyOnlyIR(source: Path, dxProject: DxProject): IR.Bundle = {
    // generate IR
    val bundle: IR.Bundle = womToIR(source)

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
            runtimePathConfig: DxPathConfig): (String, Option[JsValue]) = {
    val bundle: IR.Bundle = womToIR(source)

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
      case Some(wf) =>
        val wfId = wf.dxExec.getId
        val tree = new Tree(cResults.execDict).apply(wf)
        (wfId, Some(tree))
    }
  }
}
