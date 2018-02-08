package dxWDL

import com.dnanexus.{DXProject}
import com.typesafe.config._
import java.io.{FileWriter, PrintWriter}
import java.nio.file.{Path, Paths}
import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import scala.util.{Failure, Success}
import spray.json._
import spray.json.JsString
import Utils.Verbose
import wdl.{WdlNamespace, WdlTask, WdlNamespaceWithWorkflow, WdlWorkflow}
import wom.core.WorkflowSource

object Main extends App {
    sealed trait Termination
    case class SuccessfulTermination(output: String) extends Termination
    case class UnsuccessfulTermination(output: String) extends Termination
    case class BadUsageTermination(info: String) extends Termination

    type OptionsMap = Map[String, List[String]]

    object Actions extends Enumeration {
        val Compile, Config, DXNI, Internal, Version  = Value
    }
    object InternalOp extends Enumeration {
        val Collect,
            Eval,
            MiniWorkflow,
            ScatterCollectSubjob,
            TaskEpilog, TaskProlog, TaskRelaunch,
            WorkflowOutputReorg = Value
    }
    object CompilerFlag extends Enumeration {
        val Default, IR = Value
    }

    case class BaseOptions(force: Boolean,
                           verbose: Verbose)

    // Packing of all compiler flags in an easy to digest
    // format
    case class CompilerOptions(archive: Boolean,
                               compileMode: CompilerFlag.Value,
                               defaults: Option[Path],
                               locked: Boolean,
                               inputs: List[Path],
                               reorg: Boolean)

    // Packing of all compiler flags in an easy to digest
    // format
    case class DxniOptions(outputFile: Option[Path],
                           recursive: Boolean)

    private def normKey(s: String) : String= {
        s.replaceAll("_", "").toUpperCase
    }

    // load configuration information
    private def getVersion() : String = {
        val config = ConfigFactory.load()
        val version = config.getString("dxWDL.version")
        version
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

    // Split arguments into sub-lists, one per each option.
    // For example:
    //    --sort relaxed --reorg --compile-mode IR
    // =>
    //    [[--sort, relaxed], [--reorg], [--compile-mode, IR]]
    //
    def splitCmdLine(arglist: List[String]) : List[List[String]] = {
        def isKeyword(word: String) : Boolean = word.startsWith("-")

        val keywordAndOptions: List[List[String]] = arglist.foldLeft(List.empty[List[String]]) {
            case (head :: tail, word) if (isKeyword(word)) =>
                List(word) :: head :: tail
            case ((head :: tail), word) =>
                (word :: head) :: tail
            case (head, word) if (isKeyword(word)) =>
                List(word) :: head
            case (Nil, word) if (isKeyword(word)) => List(List(word))
            case (Nil, word) if (!isKeyword(word)) =>
                throw new Exception("Keyword must precede options")
        }
        keywordAndOptions.map(_.reverse).reverse
    }

    // parse extra command line arguments
    def parseCmdlineOptions(arglist: List[String]) : OptionsMap = {
        def normKeyword(word: String) : String = {
            // normalize a keyword, remove leading dashes, and convert
            // letters to lowercase.
            //
            // "--Archive" -> "archive"
            word.replaceAll("-", "").toLowerCase
        }
        def checkNumberOfArguments(keyword: String,
                                   expectedNumArgs:Int,
                                   subargs:List[String]) : Unit = {
            if (expectedNumArgs != subargs.length)
                throw new Exception(s"""|Wrong number of arguments for ${keyword}.
                                        |Expected ${expectedNumArgs}, input is
                                        |${subargs}"""
                                        .stripMargin.replaceAll("\n", " "))
        }
        val cmdLineOpts = splitCmdLine(arglist)
        val options = HashMap.empty[String, List[String]]
        cmdLineOpts.foreach {
            case Nil => throw new Exception("sanity: empty command line option")
            case keyOrg :: subargs =>
                val keyword = normKeyword(keyOrg)
                val (nKeyword, value) = keyword match {
                    case "archive" =>
                        checkNumberOfArguments(keyword, 0, subargs)
                        (keyword, "")
                    case "compilemode" =>
                        checkNumberOfArguments(keyword, 1, subargs)
                        (keyword, subargs.head)
                    case "defaults" =>
                        checkNumberOfArguments(keyword, 1, subargs)
                        (keyword, subargs.head)
                    case "destination" =>
                        checkNumberOfArguments(keyword, 1, subargs)
                        (keyword, subargs.head)
                    case "folder" =>
                        checkNumberOfArguments(keyword, 1, subargs)
                        (keyword, subargs.head)
                    case ("force"|"f"|"overwrite") =>
                        checkNumberOfArguments(keyword, 0, subargs)
                        ("force", "")
                    case "help" =>
                        checkNumberOfArguments(keyword, 0, subargs)
                        (keyword, "")
                    case "inputs" =>
                        checkNumberOfArguments(keyword, 1, subargs)
                        (keyword, subargs.head)
                    case "locked" =>
                        checkNumberOfArguments(keyword, 0, subargs)
                        (keyword, "")
                    case ("o"|"output"|"outputfile") =>
                        checkNumberOfArguments(keyword, 1, subargs)
                        ("outputFile", subargs.head)
                    case "project" =>
                        checkNumberOfArguments(keyword, 1, subargs)
                        (keyword, subargs.head)
                    case ("q"|"quiet") =>
                        checkNumberOfArguments(keyword, 0, subargs)
                        ("quiet", "")
                    case ("r"|"recursive") =>
                        checkNumberOfArguments(keyword, 0, subargs)
                        ("recursive", "")
                    case "reorg" =>
                        checkNumberOfArguments(keyword, 0, subargs)
                        (keyword, "")
                    case "sort" =>
                        val retval =
                            if (subargs.isEmpty) "normal"
                            else if (subargs.head == "relaxed") "relaxed"
                            else throw new Exception(s"Unknown sort option ${subargs.head}")
                        (keyword, retval)
                    case "verbose" =>
                        val retval =
                            if (subargs.isEmpty) ""
                            else if (subargs.length == 1) subargs.head
                            else throw new Exception("Too many arguments to verbose flag")
                        (keyword, retval)
                    case _ =>
                        throw new IllegalArgumentException(s"Unregonized keyword ${keyword}")
                }
                options.get(nKeyword) match {
                    case None =>
                        // first time
                        options(nKeyword) = List(value)
                    case Some(x) if (nKeyword == "verbose") =>
                        // append to the already existing verbose flags
                        options(nKeyword) = value :: x
                    case Some(x) if (nKeyword == "inputs") =>
                        options(nKeyword) = value :: x
                    case Some(x) =>
                        options(nKeyword) = List(value)
                }
        }
        options.toMap
    }

    // Report an error, since this is called from a bash script, we
    // can't simply raise an exception. Instead, we write the error to
    // a standard JSON file.
    def writeJobError(jobErrorPath : Path, e: Throwable) : Unit = {
        val errType = e match {
            case _ : AppException => "AppError"
            case _ : AppInternalException => "AppInternalError"
            case _ : Throwable => "AppInternalError"
        }
        // We are limited in what characters can be written to json, so we
        // provide a short description for json.
        //
        // Note: we sanitize this string, to be absolutely sure that
        // it does not contain problematic JSON characters.
        val errMsg = JsObject(
            "error" -> JsObject(
                "type" -> JsString(errType),
                "message" -> JsString(Utils.sanitize(e.getMessage))
            )
        ).prettyPrint
        Utils.writeFileContent(jobErrorPath, errMsg)

        // Write out a full stack trace to standard error.
        System.err.println(Utils.exceptionToString(e))
    }


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

    // Get the project name.
    //
    // We use dxpy here, because there is some subtle difference
    // between dxjava and dxpy.  When opening sessions on two
    // terminals, and selecting two projects, dxpy will correctly
    // provide pwd, dxjava only returns the name stored in
    // ~/.dnanexus_config.DX_PROJECT_CONTEXT_NAME.
    private def getDxPwd(): (String, String) = {
        try {
            // version with dxjava
            //val dxEnv = com.dnanexus.DXEnvironment.create()
            //dxEnv.getProjectContext()
            val (path, _) = Utils.execCommand("dx pwd", None)
            val vec = path.trim.split(":")
            val (projName, folder) = vec.length match {
                case 1 => (vec(0), "/")
                case 2 => (vec(0), vec(1))
                case _ => throw new Exception(s"Invalid path syntex <${path}>")
            }
            (projName, folder)
        } catch {
            case e : Throwable =>
                throw new Exception("Could not execute 'dx pwd', please check that dx is in your path")
        }
    }

    private def baseOptions(options: OptionsMap) : BaseOptions = {
        val verboseKeys: Set[String] = options.get("verbose") match {
            case None => Set.empty
            case Some(modulesToTrace) => modulesToTrace.toSet
        }
        val verbose = Verbose(options contains "verbose",
                              options contains "quiet",
                              verboseKeys)
        BaseOptions(options contains "force",
                    verbose)
    }

    private def pathOptions(verbose: Verbose,
                            options: OptionsMap) : (DXProject, String) = {
        var folderOpt:Option[String] = options.get("folder") match {
            case None => None
            case Some(List(f)) => Some(f)
            case _ => throw new Exception("sanity")
        }
        var projectOpt:Option[String] = options.get("project") match {
            case None => None
            case Some(List(p)) => Some(p)
            case _ => throw new Exception("sanity")
        }

        // There are three possible syntaxes:
        //    project-id:/folder
        //    project-id:
        //    /folder
        options.get("destination") match {
            case None => ()
            case Some(List(d)) if d contains ":" =>
                val vec = d.split(":")
                vec.length match {
                    case 1 if (d.endsWith(":")) =>
                        projectOpt = Some(vec(0))
                    case 2 =>
                        projectOpt = Some(vec(0))
                        folderOpt = Some(vec(1))
                    case _ => throw new Exception(s"Invalid path syntex <${d}>")
                }
            case Some(List(d)) if d.startsWith("/") =>
                folderOpt = Some(d)
            case Some(other) => throw new Exception(s"Invalid path syntex <${other}>")
        }

        // Use the current dx path, if nothing else was
        // specified
        val (projectRaw, folderRaw) = (projectOpt, folderOpt) match {
            case (None, None) => getDxPwd()
            case (None, Some(d)) =>
                val (crntProj, _) = getDxPwd()
                (crntProj, d)
            case (Some(p), None) => (p, "/")
            case (Some(p), Some(d)) =>(p, d)
        }

        if (folderRaw.isEmpty)
            throw new Exception(s"Cannot specify empty folder")
        if (!folderRaw.startsWith("/"))
            throw new Exception(s"Folder must start with '/'")
        val dxFolder = folderRaw
        val dxProject = DxPath.lookupProject(projectRaw)
        val projName = dxProject.describe.getName
        Utils.trace(verbose.on,
                    s"""|project name: <${projName}>
                        |project ID: <${dxProject.getId}>
                        |folder: <${dxFolder}>""".stripMargin)
        (dxProject, dxFolder)
    }

    // Get basic information about the dx environment, and process
    // the compiler flags
    private def compilerOptions(options: OptionsMap, bOpt: BaseOptions) : CompilerOptions = {
        val compileMode: CompilerFlag.Value = options.get("compilemode") match {
            case None => CompilerFlag.Default
            case Some(List(x)) if (x.toLowerCase == "ir") => CompilerFlag.IR
            case Some(other) => throw new Exception(s"unrecognized compiler flag ${other}")
        }
        val defaults: Option[Path] = options.get("defaults") match {
            case None => None
            case Some(List(p)) => Some(Paths.get(p))
            case _ => throw new Exception("defaults specified twice")
        }
        val inputs: List[Path] = options.get("inputs") match {
            case None => List.empty
            case Some(pl) => pl.map(p => Paths.get(p))
        }
        CompilerOptions(options contains "archive",
                        compileMode,
                        defaults,
                        options contains "locked",
                        inputs,
                        options contains "reorg")
    }

    private def dnxiOptions(options: OptionsMap) : DxniOptions = {
        val outputFile: Option[Path] = options.get("outputFile") match {
            case None => None
            case Some(List(p)) => Some(Paths.get(p))
            case _ => throw new Exception("only one output file can be specified")
        }
        DxniOptions(outputFile,
                    options contains "recursive")
    }


    private def embedDefaults(irNs: IR.Namespace,
                              irWf: IR.Workflow,
                              path: Path,
                              bOpt: BaseOptions) : IR.Namespace = {
        val allStageNames = irWf.stages.map{ stg => stg.name }.toVector

        // embed the defaults into the IR
        val irNsEmb = InputFile(bOpt.verbose).embedDefaults(irNs, irWf, path)

        // make sure the stage order hasn't changed
        val embedAllStageNames = irNsEmb.workflow.get.stages.map{ stg => stg.name }.toVector
        assert(allStageNames == embedAllStageNames)
        irNsEmb
    }

    private def compileIR(wdlSourceFile : Path,
                          cOpt: CompilerOptions,
                          bOpt: BaseOptions) : IR.Namespace = {
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
        val orgNs =
            WdlNamespace.loadUsingPath(wdlSourceFile, None, Some(List(resolver))) match {
                case Success(ns) => ns
                case Failure(f) =>
                    System.err.println("Error loading WDL source code")
                    throw f
            }

        // Perform check for cycles in the workflow
        // Assuming the source file is xxx.wdl, the new name will
        // be xxx.sorted.wdl.
        //CompilerTopologicalSort.check(orgNs, bOpt.verbose)

        // Simplify the original workflow, for example,
        // convert call arguments from expressions to variables.
        val nsExpr = compiler.CompilerSimplifyExpr.apply(orgNs, wdlSourceFile, bOpt.verbose)

        // Reorganize the declarations, to minimize the number of
        // applets, stages, and jobs.
        val ns = compiler.CompilerReorgDecl(nsExpr, bOpt.verbose).apply(wdlSourceFile)

        // Compile the WDL workflow into an Intermediate
        // Representation (IR) For some reason, the pretty printer
        // mangles the outputs, which is why we pass the originals
        // unmodified.
        val irNs1 = compiler.CompilerIR.apply(ns, cOpt.reorg, cOpt.locked, bOpt.verbose)
        val irNs2: IR.Namespace = (cOpt.defaults, irNs1.workflow) match {
            case (Some(path), Some(irWf)) =>
                embedDefaults(irNs1, irWf, path, bOpt)
            case (_,_) => irNs1
        }

        // Write out the intermediate representation
        prettyPrintIR(wdlSourceFile, None, irNs2, bOpt.verbose.on)

        // generate dx inputs from the Cromwell-style input specification.
        cOpt.inputs.foreach{ path =>
            val dxInputs = InputFile(bOpt.verbose).dxFromCromwell(irNs2, path)
            // write back out as xxxx.dx.json
            val filename = Utils.replaceFileSuffix(path, ".dx.json")
            val parent = path.getParent
            val dxInputFile =
                if (parent != null) parent.resolve(filename)
                else Paths.get(filename)
            Utils.writeFileContent(dxInputFile, dxInputs.prettyPrint)
            Utils.trace(bOpt.verbose.on, s"Wrote dx JSON input file ${dxInputFile}")
        }
        irNs2
    }


    // Backend compiler pass
    private def compileNative(irNs: IR.Namespace,
                              cOpt: CompilerOptions,
                              bOpt: BaseOptions,
                              folder: String,
                              dxProject: DXProject) : String = {
        // get billTo and region from the project
        val (billTo, region) = Utils.projectDescribeExtraInfo(dxProject)
        val dxWDLrtId = getAssetId(region)

        // get list of available instance types
        val instanceTypeDB = InstanceTypeDB.query(dxProject, bOpt.verbose)

        // Generate dx:applets and dx:workflow from the IR
        val (wf, _) =
            compiler.CompilerNative(dxWDLrtId, folder, dxProject, instanceTypeDB,
                                    bOpt.force, cOpt.archive, cOpt.locked, bOpt.verbose).apply(irNs)
        wf match {
            case Some(dxwfl) => dxwfl.getId
            case None => ""
        }
    }

    def compile(args: Seq[String]): Termination = {
        val wdlSourceFile = args.head
        val options =
            try {
                parseCmdlineOptions(args.tail.toList)
            } catch {
                case e : Throwable =>
                    return BadUsageTermination(Utils.exceptionToString(e))
            }
        if (options contains "help")
            return BadUsageTermination("")
        val (bOpt, cOpt): (BaseOptions, CompilerOptions) =
            try {
                val bOpt = baseOptions(options)
                val cOpt = compilerOptions(options, bOpt)
                (bOpt, cOpt)
            } catch {
                case e: Throwable =>
                    return BadUsageTermination(Utils.exceptionToString(e))
            }
        try {
            val irNs = compileIR(Paths.get(wdlSourceFile), cOpt, bOpt)
            cOpt.compileMode match {
                case CompilerFlag.Default =>
                    // Up to this point, compilation does not require
                    // the dx:project. This allows unit testing without
                    // being logged in to the platform. For the native
                    // pass the dx:project is required to establish
                    // (1) the instance price list and database
                    // (2) the output location of applets and workflows
                    val (dxProject, folder) = pathOptions(bOpt.verbose, options)
                    val dxc = compileNative(irNs, cOpt, bOpt, folder, dxProject)
                    SuccessfulTermination(dxc)
                case CompilerFlag.IR =>
                    SuccessfulTermination("")
            }
        } catch {
            case e : Throwable =>
                return UnsuccessfulTermination(Utils.exceptionToString(e))
        }
    }

    def dxni(args: Seq[String]): Termination = {
        val options =
            try {
                parseCmdlineOptions(args.toList)
            } catch {
                case e : Throwable =>
                    return BadUsageTermination(Utils.exceptionToString(e))
            }
        if (options contains "help")
            return BadUsageTermination("")
        val (bOpt, dOpt, dxProject, folder) =
            try {
                val bOpt = baseOptions(options)
                val dnxiOpt = dnxiOptions(options)
                val (dxProject, folder) = pathOptions(bOpt.verbose, options)
                (bOpt, dnxiOpt, dxProject, folder)
            } catch {
                case e: Throwable =>
                    return BadUsageTermination(Utils.exceptionToString(e))
            }
        val output = dOpt.outputFile match {
            case None => throw new Exception("Output file not specified")
            case Some(x) => x
        }

        // Validate the folder. It would have been nicer to be able
        // to check if a folder exists, instead of validating by
        // listing its contents, which could be very large.
        try {
            dxProject.listFolder(folder)
        } catch {
            case e : Throwable =>
                return UnsuccessfulTermination(s"Folder ${folder} is invalid")
        }

        try {
            compiler.DxNI.apply(dxProject, folder, output, dOpt.recursive, bOpt.force, bOpt.verbose)
            SuccessfulTermination("")
        } catch {
            case e : Throwable =>
                return UnsuccessfulTermination(Utils.exceptionToString(e))
        }
    }


    // Extract the only task from a namespace
    def taskOfNamespace(ns: WdlNamespace) : WdlTask = {
        val numTasks = ns.tasks.length
        if (numTasks != 1)
            throw new Exception(s"WDL file contains ${numTasks} tasks, instead of 1")
        ns.tasks.head
    }

    def workflowOfNamespace(ns: WdlNamespace): WdlWorkflow = {
        ns match {
            case nswf: WdlNamespaceWithWorkflow => nswf.workflow
            case _ => throw new Exception("WDL file contains no workflow")
        }
    }

    private def isTaskOp(op: InternalOp.Value) : Boolean = {
        op match {
            case InternalOp.TaskEpilog | InternalOp.TaskProlog | InternalOp.TaskRelaunch =>
                true
            case _ => false
        }
    }

    private def appletAction(op: InternalOp.Value, args : Seq[String]): Termination = {
        if (args.length != 2)
            return BadUsageTermination("All applet actions take a WDL file, and a home directory")
        val wdlDefPath = args(0)
        val homeDir = Paths.get(args(1))
        val (jobInputPath, jobOutputPath, jobErrorPath, jobInfoPath) =
            Utils.jobFilesOfHomeDir(homeDir)
        val ns = WdlNamespace.loadUsingPath(Paths.get(wdlDefPath), None, None).get
        val cef = new CompilerErrorFormatter(ns.terminalMap)
        try {
            // Figure out input/output types
            val (inputSpec, outputSpec) = Utils.loadExecInfo

            // Parse the inputs, do not download files from the platform,
            // they will be passed as links.
            val inputLines : String = Utils.readFileContent(jobInputPath)
            val orgInputs = inputLines.parseJson

            val outputFields: Map[String, JsValue] =
                if (isTaskOp(op)) {
                    // Running tasks
                    val task = taskOfNamespace(ns)
                    val inputs = WdlVarLinks.loadJobInputsAsLinks(inputLines, inputSpec, Some(task))
                    op match {
                        case InternalOp.TaskEpilog =>
                            val runner = RunnerTask(task, cef)
                            runner.epilog(inputSpec, outputSpec, inputs)
                        case InternalOp.TaskProlog =>
                            val runner = RunnerTask(task, cef)
                            runner.prolog(inputSpec, outputSpec, inputs)
                        case InternalOp.TaskRelaunch =>
                            val runner = RunnerTask(task, cef)
                            runner.relaunch(inputSpec, outputSpec, inputs)
                    }
                } else {
                    val inputs = WdlVarLinks.loadJobInputsAsLinks(inputLines, inputSpec, None)
                    val wf = workflowOfNamespace(ns)
                    op match {
                        case InternalOp.Collect =>
                            RunnerCollect.apply(wf , inputSpec, outputSpec, inputs)
                        case InternalOp.Eval =>
                            RunnerEval.apply(wf, inputSpec, outputSpec, inputs)
                        case InternalOp.MiniWorkflow =>
                            RunnerMiniWorkflow.apply(wf,
                                                     inputSpec, outputSpec, inputs, orgInputs, false)
                        case InternalOp.ScatterCollectSubjob =>
                            RunnerMiniWorkflow.apply(wf,
                                                     inputSpec, outputSpec, inputs, orgInputs, true)
                        case InternalOp.WorkflowOutputReorg =>
                            RunnerWorkflowOutputReorg.apply(wf, inputSpec, outputSpec, inputs)
                    }
                }

            // write outputs, ignore null values, these could occur for optional
            // values that were not specified.
            val json = JsObject(outputFields.filter{
                                    case (_,jsValue) => jsValue != null && jsValue != JsNull
                                })
            val ast_pp = json.prettyPrint
            Utils.writeFileContent(jobOutputPath, ast_pp)
            System.err.println(s"Wrote outputs ${ast_pp}")

            SuccessfulTermination(s"success ${op}")
        } catch {
            case e : Throwable =>
                writeJobError(jobErrorPath, e)
                UnsuccessfulTermination(s"failure running ${op}")
        }
    }

    def internalOp(args : Seq[String]) : Termination = {
        val intOp = InternalOp.values find (x => normKey(x.toString) == normKey(args.head))
        intOp match {
            case None =>
                UnsuccessfulTermination(s"unknown internal action ${args.head}")
            case Some(x) =>  appletAction(x, args.tail)
        }
    }

    def dispatchCommand(args: Seq[String]): Termination = {
        if (args.isEmpty)
            return BadUsageTermination("")
        val action = Actions.values find (x => normKey(x.toString) == normKey(args.head))
        action match {
            case None => BadUsageTermination("")
            case Some(x) => x match {
                case Actions.Compile => compile(args.tail)
                case Actions.Config => SuccessfulTermination(ConfigFactory.load().toString)
                case Actions.DXNI => dxni(args.tail)
                case Actions.Internal => internalOp(args.tail)
                case Actions.Version => SuccessfulTermination(getVersion())
            }
        }
    }

    val usageMessage =
        s"""|java -jar dxWDL.jar <action> <parameters> [options]
            |
            |Actions:
            |  compile <WDL file>
            |    Compile a wdl file into a dnanexus workflow.
            |    Optionally, specify a destination path on the
            |    platform. If a WDL inputs files is specified, a dx JSON
            |    inputs file is generated from it.
            |    options
            |      -archive              Archive older versions of applets
            |      -compileMode <string> Compilation mode, a debugging flag
            |      -defaults <string>    Path to Cromwell formatted default values file
            |      -destination <string> Output path on the platform for workflow
            |      -inputs <string>      Path to Cromwell formatted input file
            |      -locked               Create a locked-down workflow (experimental)
            |      -reorg                Reorganize workflow output files
            |
            |  dxni
            |    Dx Native call Interface. Create stubs for calling dx
            |    applets, and store them as WDL tasks in a local file. Allows
            |    calling existing platform applets without modification.
            |    options:
            |      -o <string>           Destination file for WDL task definitions
            |      -r | recursive        Recursive search
            |
            |Common options
            |    -destination           Full platform path (project:/folder)
            |    -f | force             Delete existing applets/workflows
            |    -folder <string>       Platform folder
            |    -project <string>      Platform project
            |    -quiet                 Do not print warnings or informational outputs
            |    -verbose [flag]        Print detailed progress reports
            |""".stripMargin

    val termination = dispatchCommand(args)

    termination match {
        case SuccessfulTermination(s) => println(s)
        case BadUsageTermination(s) if (s == "") =>
            Console.err.println(usageMessage)
            System.exit(1)
        case BadUsageTermination(s) =>
            Console.err.println(s)
            System.exit(1)
        case UnsuccessfulTermination(s) =>
            Console.err.println(s)
            System.exit(1)
    }
}
