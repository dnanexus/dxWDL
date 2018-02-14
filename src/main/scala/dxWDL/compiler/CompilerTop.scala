package dxWDL.compiler

import com.dnanexus.{DXProject}
import com.typesafe.config._
import dxWDL.{CompilerFlag, CompilerOptions, InstanceTypeDB, Utils}
import java.nio.file.{Path, Paths}
import java.io.{FileWriter, PrintWriter}
import scala.collection.JavaConverters._
import scala.util.{Failure, Success}
import wdl.WdlNamespace
import wom.core.WorkflowSource


// Interface to the compilation tool chain. The methods here are the only ones
// that should be called to perform compilation.
object CompilerTop {

    private def prettyPrintIR(wdlSourceFile : Path,
                              extraSuffix: Option[String],
                              irNs: IR.Namespace,
                              verbose: Boolean) : Unit = {
        val suffix = extraSuffix match {
            case None => ".ir.yaml"
            case Some(x) => x + ".ir.yaml"
        }
        val trgName: String = Utils.replaceFileSuffix(wdlSourceFile, suffix)
        val trgPath = Utils.appCompileDirPath.resolve(trgName).toFile
        val yo = IR.yaml(irNs)
        val humanReadable = IR.prettyPrint(yo)
        val fos = new FileWriter(trgPath)
        val pw = new PrintWriter(fos)
        pw.print(humanReadable)
        pw.flush()
        pw.close()
        Utils.trace(verbose, s"Wrote intermediate representation to ${trgPath.toString}")
    }

    private def embedDefaults(irNs: IR.Namespace,
                              irWf: IR.Workflow,
                              path: Path,
                              cOpt: CompilerOptions) : IR.Namespace = {
        val allStageNames = irWf.stages.map{ stg => stg.name }.toVector

        // embed the defaults into the IR
        val irNsEmb = InputFile(cOpt.verbose).embedDefaults(irNs, irWf, path)

        // make sure the stage order hasn't changed
        val embedAllStageNames = irNsEmb.workflow.get.stages.map{ stg => stg.name }.toVector
        assert(allStageNames == embedAllStageNames)
        irNsEmb
    }

    private def compileIR(wdlSourceFile : Path,
                          cOpt: CompilerOptions) : IR.Namespace = {
        // Resolving imports. Look for referenced files in the
        // source directory.
        def resolver(filename: String) : WorkflowSource = {
            var sourceDir:Path = wdlSourceFile.getParent()
            if (sourceDir == null) {
                // source file has no parent directory, use the
                // current directory instead
                sourceDir = Paths.get(System.getProperty("user.dir"))
            }
            val p:Path = sourceDir.resolve(filename)
            Utils.readFileContent(p)
        }
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

        val nsTree: NamespaceOps.Tree = NamespaceOps.load(ns, wdlSourceFile)

        // Simplify the original workflow, for example,
        // convert call arguments from expressions to variables.
        val nsTreeSimple = SimplifyExpr.apply(nsTree, wdlSourceFile, cOpt.verbose)

        // Reorganize the declarations, to minimize the number of
        // applets, stages, and jobs.
        val nsTreeReorg = ReorgDecl.apply(nsTreeSimple, wdlSourceFile, cOpt.verbose)

        // Compile the WDL workflow into an Intermediate
        // Representation (IR) For some reason, the pretty printer
        // mangles the outputs, which is why we pass the originals
        // unmodified.
        val irNs1 = GenerateIR.apply(nsTreeReorg, cOpt.reorg, cOpt.locked, cOpt.verbose)
        val irNs2: IR.Namespace = (cOpt.defaults, irNs1.workflow) match {
            case (Some(path), Some(irWf)) =>
                embedDefaults(irNs1, irWf, path, cOpt)
            case (_,_) => irNs1
        }

        // Write out the intermediate representation
        prettyPrintIR(wdlSourceFile, None, irNs2, cOpt.verbose.on)

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
                              cOpt: CompilerOptions) : String = {
        // get billTo and region from the project
        val (billTo, region) = Utils.projectDescribeExtraInfo(dxProject)
        val dxWDLrtId = getAssetId(region)

        // get list of available instance types
        val instanceTypeDB = InstanceTypeDB.query(dxProject, cOpt.verbose)

        // Generate dx:applets and dx:workflow from the IR
        val (wf, _) =
            Native(dxWDLrtId, folder, dxProject, instanceTypeDB,
                   cOpt.force, cOpt.archive, cOpt.locked, cOpt.verbose).apply(irNs)
        wf match {
            case Some(dxwfl) => dxwfl.getId
            case None => ""
        }
    }


    def apply(wdlSourceFile: String,
              folder: String,
              dxProject: DXProject,
              cOpt: CompilerOptions) : Option[String] = {
        val irNs = compileIR(Paths.get(wdlSourceFile), cOpt)
        cOpt.compileMode match {
            case CompilerFlag.IR =>
                None
            case CompilerFlag.Default =>
                // Up to this point, compilation does not require
                // the dx:project. This allows unit testing without
                // being logged in to the platform. For the native
                // pass the dx:project is required to establish
                // (1) the instance price list and database
                // (2) the output location of applets and workflows
                val dxc = compileNative(irNs, folder, dxProject, cOpt)
                Some(dxc)
        }
    }

}
