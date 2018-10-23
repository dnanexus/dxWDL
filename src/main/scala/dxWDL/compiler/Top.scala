// Interface to the compilation tool chain. The methods here are the only ones
// that should be called to perform compilation.

package dxWDL.compiler

import com.dnanexus.{DXDataObject, DXProject, DXRecord, DXSearch}
import common.Checked
import common.validation.Validation._
import dxWDL.util._
import dxWDL.util.Utils.DX_WDL_ASSET
import cromwell.core.path.{DefaultPathBuilder, Path}
import cromwell.languages.util.ImportResolver._
import java.nio.file.{Files, Paths}
import languages.cwl.CwlV1_0LanguageFactory
import languages.wdl.draft2.WdlDraft2LanguageFactory
import languages.wdl.draft3.WdlDraft3LanguageFactory
import scala.collection.JavaConverters._
import scala.util.Try
import wom.executable.WomBundle

object Top {

    // The mapping from region to project name is list of (region, proj-name) pairs.
    // Get the project for this region.
    private def getProjectWithRuntimeLibrary(region2project: Map[String, String],
                                             region: String) : (String, String) = {
        val destination = region2project.get(region) match {
            case None => throw new Exception(s"Region ${region} is currently unsupported")
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
    private def getAssetId(region: String,
                           verbose: Verbose) : String = {
        val region2project = Utils.getRegions()
        val (projNameRt, folder)  = getProjectWithRuntimeLibrary(region2project, region)
        val dxProjRt = DxPath.lookupProject(projNameRt)
        Utils.trace(verbose.on, s"Looking for asset-id in ${projNameRt}:/${folder}")

        val found:List[DXDataObject] =
            DXSearch.findDataObjects()
                .nameMatchesExactly(DX_WDL_ASSET)
                .inFolder(dxProjRt, folder)
                .execute().asList().asScala.toList
        if (found.length == 0)
            throw new Exception(s"Could not find runtime asset at ${dxProjRt.getId}:${folder}/${DX_WDL_ASSET}")
        if (found.length > 1)
            throw new Exception(s"Found more than one dx:object named ${DX_WDL_ASSET} in path ${dxProjRt.getId}:${folder}")

        found(0).getId
    }

    // We need the dxWDL runtime library cloned into this project, so it will
    // be available to all subjobs we run.
    private def cloneRtLibraryToProject(region: String,
                                        dxWDLrtId: String,
                                        dxProject: DXProject,
                                        verbose: Verbose) : Unit = {
        val region2project = Utils.getRegions()
        val (projNameRt, folder)  = getProjectWithRuntimeLibrary(region2project, region)
        val dxProjRt = DxPath.lookupProject(projNameRt)
        Utils.cloneAsset(DXRecord.getInstance(dxWDLrtId),
                         dxProject,
                         DX_WDL_ASSET,
                         dxProjRt,
                         verbose)
    }

    // Backend compiler pass
    private def compileNative(bundle: IR.Bundle,
                              folder: String,
                              dxProject: DXProject,
                              cOpt: CompilerOptions) : CompilationResults = {
        // get billTo and region from the project
        val (billTo, region) = Utils.projectDescribeExtraInfo(dxProject)
        val dxWDLrtId = getAssetId(region, cOpt.verbose)
        cloneRtLibraryToProject(region, dxWDLrtId, dxProject, cOpt.verbose)

        // get list of available instance types
        val instanceTypeDB = InstanceTypeDB.query(dxProject, cOpt.verbose)

        // Efficiently build a directory of the currently existing applets.
        // We don't want to build them if we don't have to.
        val dxObjDir = DxObjectDirectory(bundle, dxProject, folder, cOpt.projectWideReuse,
                                         cOpt.verbose)

        // Generate dx:applets and dx:workflow from the IR
        Native.apply(bundle,
                     dxWDLrtId, folder, dxProject, instanceTypeDB, dxObjDir,
                     cOpt.extras,
                     cOpt.runtimeDebugLevel,
                     cOpt.leaveWorkflowsOpen, cOpt.force, cOpt.archive, cOpt.locked, cOpt.verbose)
    }

    // Compile IR only
    def applyOnlyIR(sourcePath: String,
                    cOpt: CompilerOptions) : IR.Bundle = {
        val bundle = ParseWomSourceFile.apply(sourcePath)

        // Compile the WDL workflow into an Intermediate
        // Representation (IR)
        GenerateIR.apply(bundle, cOpt.verbose)
    }

    // Compile up to native dx applets and workflows
    def apply(sourcePath: String,
              folder: String,
              dxProject: DXProject,
              cOpt: CompilerOptions) : Option[String] = {
        val bundle: IR.Bundle = applyOnlyIR(sourcePath, cOpt)

        // Up to this point, compilation does not require
        // the dx:project. This allows unit testing without
        // being logged in to the platform. For the native
        // pass the dx:project is required to establish
        // (1) the instance price list and database
        // (2) the output location of applets and workflows
        val cResults = compileNative(bundle, folder, dxProject, cOpt)
        val execIds = cResults.entrypoint match {
            case None =>
                cResults.execDict.map{ case (_, dxExec) => dxExec.getId }.mkString(",")
            case Some(wf) =>
                wf.getId
        }
        Some(execIds)
    }
}
