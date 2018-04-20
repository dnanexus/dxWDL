package dxWDL.compiler

import com.dnanexus.{DXProject}
import com.typesafe.config._
import dxWDL.{CompilerOptions, CompilationResults, InstanceTypeDB, Utils, Verbose}
import java.nio.file.{Files, Path, Paths}
import java.io.{FileWriter, PrintWriter}
import scala.collection.JavaConverters._
import scala.util.{Failure, Success}
import wdl.{WdlNamespace, ImportResolver}

// Interface to the compilation tool chain. The methods here are the only ones
// that should be called to perform compilation.
object CompilerTop {

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

    private def embedDefaults(wf: IR.Workflow,
                              irNs: IR.Namespace,
                              path: Path,
                              cOpt: CompilerOptions) : IR.Namespace = {
        val allStageNames = wf.stages.map{ stg => stg.name }.toVector

        // embed the defaults into the IR
        val irNsEmb = InputFile(cOpt.verbose).embedDefaults(wf, irNs, path)

        // make sure the stage order hasn't changed
        val workflow1: IR.Workflow = irNsEmb.entrypoint.get
        val embedAllStageNames = workflow1.stages.map{ stg => stg.name }.toVector
        assert(allStageNames == embedAllStageNames)

        irNsEmb
    }

    private def findSourcesInImports(wdlSourceFile: Path,
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

        val wdlFileNames = "{" + allWdlSourceFiles.keys.mkString(", ") + "}"
        Utils.trace(verbose.on, s"Files in search path=${wdlFileNames}")
        allWdlSourceFiles
    }


    def makeResolver(allWdlSources: Map[String, String]) : ImportResolver = {
        filename => allWdlSources.get(filename) match {
            case None => throw new Exception(s"Unable to find ${filename}")
            case Some(content) => content
        }
    }

    private def compileNamespaceOpsTree(wdlSourceFile : Path,
                                        cOpt: CompilerOptions) : NamespaceOps.Tree = {
        val allWdlSourceFiles = findSourcesInImports(wdlSourceFile, cOpt.imports, cOpt.verbose)
        val allWdlSources: Map[String, String] = allWdlSourceFiles.map{
            case (name, path) => name -> Utils.readFileContent(path)
        }.toMap
        val resolver = makeResolver(allWdlSources)
        val ns =
            WdlNamespace.loadUsingPath(wdlSourceFile, None, Some(List(resolver))) match {
                case Success(ns) => ns
                case Failure(f) =>
                    System.err.println("Error loading WDL source code")
                    throw f
            }

        // Make sure the namespace doesn't use names or substrings
        // that will give us problems.
        Validate.apply(ns, cOpt.verbose)

        val nsTree: NamespaceOps.Tree = NamespaceOps.load(ns, allWdlSources, cOpt.verbose)
        val nsTreePruned = NamespaceOps.prune(nsTree, cOpt.verbose)
        nsTreePruned match {
            case node:NamespaceOps.TreeNode =>
                System.err.println(s"pruned sources = ${node.allWdlSources.keys}")
            case _ => ()
        }
        System.err.println(s"nsTreePruned sources=${nsTreePruned.getAllWdlSourceFiles}")

        // Simplify the original workflow, for example,
        // convert call arguments from expressions to variables.
        val nsTreeSimple = SimplifyExpr.apply(nsTreePruned, wdlSourceFile, cOpt.verbose)
        System.err.println(s"nsTreeSimple sources=${nsTreeSimple.getAllWdlSourceFiles}")

        // Convert large sub-blocks to sub-workflows
        DecomposeBlocks.apply(nsTreeSimple, wdlSourceFile, cOpt.verbose)
    }

    private def compileIR(wdlSourceFile : Path,
                          nsTree: NamespaceOps.Tree,
                          cOpt: CompilerOptions) : IR.Namespace = {
        // Compile the WDL workflow into an Intermediate
        // Representation (IR)
        val irNs = GenerateIR.apply(nsTree, cOpt.reorg, cOpt.locked, cOpt.verbose)
        val irNs2: IR.Namespace = (cOpt.defaults, irNs.entrypoint) match {
            case (Some(path), Some(wf)) =>
                embedDefaults(wf, irNs, path, cOpt)
            case (_,_) => irNs
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


    private def getAssetId(region: String) : String = {
        val config = ConfigFactory.load()

        // The asset ids is list of (region, asset-id) pairs
        val rawAssets: List[Config] = config.getConfigList("dxWDL.asset_ids").asScala.toList
        val assets:Map[String, String] = rawAssets.map{ pair =>
            val r = pair.getString("region")
            val assetId = pair.getString("asset")
            assert(assetId.startsWith("record"))
            r -> assetId
        }.toMap

        assets.get(region) match {
            case None => throw new Exception(s"Region ${region} is currently unsupported")
            case Some(assetId) => assetId
        }
    }

    // Backend compiler pass
    private def compileNative(irNs: IR.Namespace,
                              folder: String,
                              dxProject: DXProject,
                              cOpt: CompilerOptions) : CompilationResults = {
        // get billTo and region from the project
        val (billTo, region) = Utils.projectDescribeExtraInfo(dxProject)
        val dxWDLrtId = getAssetId(region)

        // get list of available instance types
        val instanceTypeDB = InstanceTypeDB.query(dxProject, cOpt.verbose)

        // Generate dx:applets and dx:workflow from the IR
        Native.apply(irNs,
                     dxWDLrtId, folder, dxProject, instanceTypeDB,
                     cOpt.force, cOpt.archive, cOpt.locked, cOpt.verbose)
    }


    // Compile IR only
    def applyOnlyIR(wdlSourceFile: String,
                    cOpt: CompilerOptions) : Unit = {
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
                cResults.applets.map{ case (_, apl) => apl.getId }.mkString(",")
            case Some(wf) =>
                wf.getId
        }
        Some(execIds)
    }
}
