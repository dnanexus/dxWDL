// Interface to the compilation tool chain. The methods here are the only ones
// that should be called to perform compilation.

package dxWDL.compiler

import com.dnanexus.{DXDataObject, DXProject, DXSearch}
import com.typesafe.config._
import dxWDL.{CompilerOptions, CompilationResults, DxPath, InstanceTypeDB, Utils, Verbose}
import dxWDL.Utils.DX_WDL_ASSET
import java.nio.file.{Files, Path, Paths}
import java.io.{FileWriter, PrintWriter}
import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import scala.io.Source
import scala.util.{Failure, Success}
import wdl.draft2.model.{WdlExpression, WdlNamespace, Draft2ImportResolver}

// Load files from the local filesystem
case class TopFileResolver(localWdlSourceFiles: Map[String, Path],
                           fileIndex: HashMap[String, String]) {
    def make : Draft2ImportResolver = { fullName =>
        localWdlSourceFiles.get(fullName) match {
            case None =>
                throw new Exception(s"Unable to find ${fullName}")
            case Some(path) =>
                val wdlCode = Utils.readFileContent(path)
                fileIndex(path.toString) = wdlCode
                wdlCode
        }
    }
}

// The [languages.wdl.draft2.WdlDraft2LanguageFactory] module has the implementation
// of the Cromwell http resolver.
//
// The full path is
//    CROMWELL/languageFactories/language-factory-core/src/main/scala/cromwell/languages
//    CROMWELL/languageFactories/wdl-draft2/src/main/scala/languages/wdl/draft2/WdlDraft2LanguageFactory.scala

// Download files from the web, and record the contents in a mapping.
case class TopHttpResolver(fileIndex: HashMap[String, String]) {
    def make : Draft2ImportResolver = { url =>
        fileIndex.get(url) match {
            case None =>
                val html = Source.fromURL(url)
                val wdlCode = html.mkString
                fileIndex(url) = wdlCode
                wdlCode
            case Some(wdlCode) =>
                // file has already been downloaded; we assume the contents
                // has not changed
                wdlCode
        }
    }
}


object Top {
    private def prettyPrintIR(wdlSourceFile : Path,
                              extraSuffix: Option[String],
                              irc: IR.Namespace,
                              verbose: Boolean) : Unit = {
        val suffix = extraSuffix match {
            case None => ".ir.yaml"
            case Some(x) => x + ".ir.yaml"
        }
        val trgName: String = Utils.replaceFileSuffix(wdlSourceFile, suffix)
        val trgPath = Utils.appCompileDirPath.resolve(trgName).toFile
        val yo = IR.yaml(irc)
        val humanReadable = IR.prettyPrint(yo)
        val fos = new FileWriter(trgPath)
        val pw = new PrintWriter(fos)
        pw.print(humanReadable)
        pw.flush()
        pw.close()
        Utils.trace(verbose, s"Wrote intermediate representation to ${trgPath.toString}")
    }

    private def findSourcesInImportDirs(wdlSourceFile: Path,
                                        imports: List[Path],
                                        verbose: Verbose) : Map[String, Path] = {
        Utils.trace(verbose.on, s"WDL import directories:  ${imports.toVector}")

        // Find all the WDL files under a directory.
        def getListOfFiles(dir: Path) : Map[String, Path] = {
            if (Files.exists(dir) && Files.isDirectory(dir)) {
                val files: List[Path] =
                    Files.list(dir).iterator().asScala
                        .filter(Files.isRegularFile(_))
                        .filter(_.toString.endsWith(".wdl"))
                        .toList
                files.map{ path =>  path.toFile.getName -> path}.toMap
            } else {
                Map.empty
            }
        }

        // Add the directory where the source file is in to the
        // search path
        def buildDirList : List[Path] = {
            val parent:Path = wdlSourceFile.getParent()
            val sourceDir =
                if (parent == null) {
                    // source file has no parent directory, use the
                    // current directory instead
                    Paths.get(System.getProperty("user.dir"))
                } else {
                    parent
                }
            sourceDir :: imports
        }

        val allWdlSourceFiles: Map[String, Path] =
            buildDirList.foldLeft(Map.empty[String,Path]) {
                case (accu, d) =>
                    accu ++ getListOfFiles(d)
            }

        //val wdlFileNames = "{" + allWdlSourceFiles.keys.mkString(", ") + "}"
        //Utils.trace(verbose.on, s"Files in search path=${wdlFileNames}")
        allWdlSourceFiles
    }

    private def compileNamespaceOpsTree(wdlSourceFile : Path,
                                        cOpt: CompilerOptions) : NamespaceOps.Tree = {
        val importedFiles = HashMap.empty[String, String]
        val localWdlSourceFiles : Map[String, Path] =
            findSourcesInImportDirs(wdlSourceFile, cOpt.importDirs, cOpt.verbose)
        val fileResolver = new TopFileResolver(localWdlSourceFiles, importedFiles)
        val httpResolver = new TopHttpResolver(importedFiles)
        val ns =
            WdlNamespace.loadUsingPath(wdlSourceFile,
                                       None,
                                       Some(List(fileResolver.make, httpResolver.make))) match {
                case Success(ns) => ns
                case Failure(f) =>
                    System.err.println("Error loading WDL source code")
                    throw f
            }

        // Make sure the namespace doesn't use names or substrings
        // that will give us problems.
        Validate.apply(ns, cOpt.fatalValidationWarnings, cOpt.verbose)

        Utils.trace(cOpt.verbose.on, s"imported files: ${importedFiles.keys}")
        val ctx: Context = Context.make(importedFiles.toMap, wdlSourceFile, cOpt.verbose)
        val defaultRuntimeAttributes = cOpt.extras match {
            case None => Map.empty[String, WdlExpression]
            case Some(xt) => xt.defaultRuntimeAttributes
        }
        val nsTree: NamespaceOps.Tree = NamespaceOps.load(ns, ctx, defaultRuntimeAttributes)
        val ctxHdrs = ctx.makeHeaders

        // Convert large sub-blocks to sub-workflows
        Decompose.apply(nsTree, wdlSourceFile, ctxHdrs, cOpt.verbose)
    }

    private def compileIR(wdlSourceFile : Path,
                          nsTree: NamespaceOps.Tree,
                          cOpt: CompilerOptions) : IR.Namespace = {
        // Compile the WDL workflow into an Intermediate
        // Representation (IR)
        val irNs = GenerateIR.apply(nsTree, cOpt.reorg, cOpt.locked, cOpt.verbose)
        val irNs2: IR.Namespace = cOpt.defaults match {
            case None => irNs
            case Some(path) => InputFile(cOpt.verbose).embedDefaults(irNs, path)
        }

        // Write out the intermediate representation
        prettyPrintIR(wdlSourceFile, None, irNs, cOpt.verbose.on)

        // generate dx inputs from the Cromwell-style input specification.
        cOpt.inputs.foreach{ path =>
            val dxInputs = InputFile(cOpt.verbose).dxFromCromwell(irNs2, path)
            // write back out as xxxx.dx.json
            val filename = Utils.replaceFileSuffix(path, ".dx.json")
            val parent = path.getParent
            val dxInputFile =
                if (parent != null) parent.resolve(filename)
                else Paths.get(filename)
            Utils.writeFileContent(dxInputFile, dxInputs.prettyPrint)
            Utils.trace(cOpt.verbose.on, s"Wrote dx JSON input file ${dxInputFile}")
        }
        irNs2
    }


    // The mapping from region to project name is list of (region, proj-name) pairs.
    // Get the project for this region.
    private def getProjectWithRuntimeLibrary(config: Config, region: String) : (String, String) = {
        val l: List[Config] = config.getConfigList("dxWDL.region2project").asScala.toList
        val region2project:Map[String, String] = l.map{ pair =>
            val r = pair.getString("region")
            val projName = pair.getString("path")
            r -> projName
        }.toMap

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
        val config = Utils.getConfig()
        val (projNameRt, folder)  = getProjectWithRuntimeLibrary(config, region)
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

    // Backend compiler pass
    private def compileNative(irNs: IR.Namespace,
                              folder: String,
                              dxProject: DXProject,
                              cOpt: CompilerOptions) : CompilationResults = {
        // get billTo and region from the project
        val (billTo, region) = Utils.projectDescribeExtraInfo(dxProject)
        val dxWDLrtId = getAssetId(region, cOpt.verbose)

        // get list of available instance types
        val instanceTypeDB = InstanceTypeDB.query(dxProject, cOpt.verbose)

        // Efficiently build a directory of the currently existing applets.
        // We don't want to build them if we don't have to.
        val dxObjDir = DxObjectDirectory(irNs, dxProject, folder, cOpt.projectWideReuse,
                                         cOpt.verbose)

        // Generate dx:applets and dx:workflow from the IR
        Native.apply(irNs,
                     dxWDLrtId, folder, dxProject, instanceTypeDB, dxObjDir,
                     cOpt.extras,
                     cOpt.runtimeDebugLevel,
                     cOpt.force, cOpt.archive, cOpt.locked, cOpt.verbose)
    }


    // Compile IR only
    def applyOnlyIR(wdlSourceFile: String,
                    cOpt: CompilerOptions) : IR.Namespace = {
        val path = Paths.get(wdlSourceFile)
        val nsTree = compileNamespaceOpsTree(path, cOpt)
        compileIR(path, nsTree, cOpt)
    }

    // Compile up to native dx applets and workflows
    def apply(wdlSourceFile: String,
              folder: String,
              dxProject: DXProject,
              cOpt: CompilerOptions) : Option[String] = {
        val path = Paths.get(wdlSourceFile)
        val nsTree = compileNamespaceOpsTree(path, cOpt)
        val irNs = compileIR(path, nsTree, cOpt)

        // Up to this point, compilation does not require
        // the dx:project. This allows unit testing without
        // being logged in to the platform. For the native
        // pass the dx:project is required to establish
        // (1) the instance price list and database
        // (2) the output location of applets and workflows
        val cResults = compileNative(irNs, folder, dxProject, cOpt)
        val execIds = cResults.entrypoint match {
            case None =>
                cResults.execDict.map{ case (_, dxExec) => dxExec.getId }.mkString(",")
            case Some(wf) =>
                wf.getId
        }
        Some(execIds)
    }
}
