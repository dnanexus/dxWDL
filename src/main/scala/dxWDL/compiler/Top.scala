// Interface to the compilation tool chain. The methods here are the only ones
// that should be called to perform compilation.

package dxWDL.compiler

import com.dnanexus.{DXDataObject, DXProject, DXRecord, DXSearch}
import dxWDL.util._
import dxWDL.util.Utils.DX_WDL_ASSET
import java.nio.file.{Path, Paths}
import scala.collection.JavaConverters._
import wom.callable._
import wom.types._
import wom.executable.WomBundle

case class Top(cOpt: CompilerOptions) {
    val verbose = cOpt.verbose

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
    private def getAssetId(region: String) : String = {
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
                                        dxProject: DXProject) : Unit = {
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
                              runtimePathConfig: DxPathConfig) : CompilationResults = {
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
                val (billTo, region) = Utils.projectDescribeExtraInfo(dxProject)
                val lrtId = getAssetId(region)
                cloneRtLibraryToProject(region, lrtId, dxProject)
                Some(lrtId)
        }
        // get list of available instance types
        val instanceTypeDB = InstanceTypeDB.query(dxProject, verbose)

        // Efficiently build a directory of the currently existing applets.
        // We don't want to build them if we don't have to.
        val dxObjDir = DxObjectDirectory(bundle, dxProject, folder, cOpt.projectWideReuse,
                                         verbose)

        // Generate dx:applets and dx:workflow from the IR
        new Native(dxWDLrtId, folder, dxProject,
                   dxObjDir,
                   instanceTypeDB, runtimePathConfig,
                   cOpt.extras,
                   cOpt.runtimeDebugLevel,
                   cOpt.leaveWorkflowsOpen,
                   cOpt.force, cOpt.archive, cOpt.locked, cOpt.verbose).apply(bundle)
    }


    // check that streaming annotations are only done for files.
    private def validate(callable: Callable) : Unit = {
        callable match {
            case wf: WorkflowDefinition =>
                if (wf.parameterMeta.size > 0)
                    Utils.warning(verbose, "dxWDL workflows ignore their parameter meta section")

            case task: CallableTaskDefinition =>
                task.inputs.foreach{
                    case iDef : Callable.InputDefinition =>
                        iDef.parameterMeta match {
                            case None => ()
                            case Some(MetaValueElement.MetaValueElementString(x)) if x == "stream"=>
                                if (iDef.womType != WomSingleFileType) {
                                    val msg =
                                        s"""|Only files that are task inputs can be declared streaming.
                                            |task = ${task.name}, input = ${iDef.name},
                                            |womType = ${iDef.womType}
                                            |""".stripMargin.replaceAll("\n", " ")
                                    if (cOpt.fatalValidationWarnings)
                                        throw new Exception(msg)
                                    else
                                        Utils.warning(verbose, msg)
                                }
                            case Some(other) => ()
                        }
                }

            case other =>
                throw new Exception(s"Unexpected object ${other}")
        }
    }

    // Check the uniqueness of tasks, Workflows, and Types
    // merge everything into one bundle.
    private def mergeIntoOneBundle(mainBundle: WomBundle,
                                   subBundles: Vector[WomBundle]) : WomBundle = {
        var allCallables = mainBundle.allCallables
        var allTypeAliases = mainBundle.typeAliases

        subBundles.foreach{ subBund =>
            subBund.allCallables.foreach{ case (key, callable) =>
                allCallables.get(key) match {
                    case None =>
                        allCallables = allCallables + (key -> callable)
                    case Some(existing) if (existing != callable) =>
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
            subBund.typeAliases.foreach { case (key, definition) =>
                allTypeAliases.get(key) match {
                    case None =>
                        allTypeAliases = allTypeAliases + (key -> definition)
                    case Some(existing) =>
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
        WomBundle(mainBundle.primaryCallable,
                  allCallables,
                  allTypeAliases)
    }

    // Compile IR only
    def applyOnlyIR(source: Path) : IR.Bundle = {
        val (language, womBundle, allSources, subBundles) = ParseWomSourceFile.apply(source)

        // Check that each workflow/task appears just one
        val everythingBundle : WomBundle = mergeIntoOneBundle(womBundle, subBundles)

        // validate
        everythingBundle.allCallables.foreach{ case (_, c) => validate(c) }
        everythingBundle.primaryCallable match {
            case None => ()
            case Some(x) => validate(x)
        }

        // Compile the WDL workflow into an Intermediate
        // Representation (IR)
        val bundle: IR.Bundle = new GenerateIR(cOpt.verbose).apply(everythingBundle, allSources, language,
                                                                   cOpt.locked, cOpt.reorg)
        val bundle2: IR.Bundle = cOpt.defaults match {
            case None => bundle
            case Some(path) => InputFile(cOpt.verbose).embedDefaults(bundle, path)
        }

        // generate dx inputs from the Cromwell-style input specification.
        cOpt.inputs.foreach{ path =>
            val dxInputs = InputFile(cOpt.verbose).dxFromCromwell(bundle2, path)
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

    // Compile up to native dx applets and workflows
    def apply(source: Path,
              folder: String,
              dxProject: DXProject,
              runtimePathConfig: DxPathConfig) : Option[String] = {
        val bundle: IR.Bundle = applyOnlyIR(source)

        // Up to this point, compilation does not require
        // the dx:project. This allows unit testing without
        // being logged in to the platform. For the native
        // pass the dx:project is required to establish
        // (1) the instance price list and database
        // (2) the output location of applets and workflows
        val cResults = compileNative(bundle, folder, dxProject, runtimePathConfig)
        val execIds = cResults.primaryCallable match {
            case None =>
                cResults.execDict.map{ case (_, dxExec) => dxExec.getId }.mkString(",")
            case Some(wf) =>
                wf.getId
        }
        Some(execIds)
    }
}
