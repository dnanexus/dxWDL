package dxWDL

import com.dnanexus.{DXProject, DXWorkflow}
import com.typesafe.config._
import java.io.{FileWriter, PrintWriter}
import java.nio.file.{Path, Paths}
import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import scala.util.{Failure, Success}
import spray.json._
import spray.json.JsString
import Utils.{TopoMode, Verbose}
import wdl4s.wdl.{ImportResolver, WdlNamespace, WdlTask,
    WdlNamespaceWithWorkflow, WdlWorkflow, WorkflowOutput, WorkflowSource}

object Main extends App {
    sealed trait Termination
    case class SuccessfulTermination(output: String) extends Termination
    case class UnsuccessfulTermination(output: String) extends Termination
    case class BadUsageTermination(info: String) extends Termination

    type OptionsMap = Map[String, List[String]]

    object Actions extends Enumeration {
        val Compile, Config, DXCI, Internal, Version  = Value
    }
    object InternalOp extends Enumeration {
        val Eval, MiniWorkflow,
            TaskEpilog, TaskProlog, TaskRelaunch,
            WorkflowOutputs, WorkflowOutputsAndReorg = Value
    }
    object CompilerFlag extends Enumeration {
        val Default, IR = Value
    }

    // Compiler state.
    // Packs common arguments passed between methods.
    case class State(ns: WdlNamespace,
                     outputs: Option[Seq[WorkflowOutput]],
                     wdlSourceFile: Path,
                     resolver: ImportResolver,
                     verbose: Verbose)

    // Packing of all compiler flags in an easy to digest
    // format
    case class CompileOptions(appletTimeout: Option[Int],
                              archive: Boolean,
                              billTo: String,
                              compileMode: CompilerFlag.Value,
                              defaults: Option[Path],
                              dxProject: DXProject,
                              dxWDLrtId: String,
                              folder: String,
                              force: Boolean,
                              inputs: Option[Path],
                              outputFile: Option[Path],
                              region: String,
                              reorg: Boolean,
                              sortMode: TopoMode.Value,
                              verbose: Verbose)

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
        //System.err.println(s"allAssets = $assets")

        assets.get(region) match {
            case None => throw new Exception(s"Region ${region} is currently unsupported")
            case Some(assetId) => assetId
        }
    }

    def outputs(ns: WdlNamespace) : Option[Seq[WorkflowOutput]] = {
        ns match {
            case nswf: WdlNamespaceWithWorkflow =>
                val wf = nswf.workflow
                if (wf.hasEmptyOutputSection) None
                else Some(wf.outputs)
            case _ => None
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
                    case ("destination"|"folder") =>
                        checkNumberOfArguments(keyword, 1, subargs)
                        ("destination", subargs.head)
                    case ("force"|"f"|"overwrite") =>
                        checkNumberOfArguments(keyword, 0, subargs)
                        ("force", "")
                    case "help" =>
                        checkNumberOfArguments(keyword, 0, subargs)
                        (keyword, "")
                    case "inputs" =>
                        checkNumberOfArguments(keyword, 1, subargs)
                        (keyword, subargs.head)
                    case ("o"|"output"|"outputfile") =>
                        checkNumberOfArguments(keyword, 1, subargs)
                        ("outputFile", subargs.head)
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


    def prettyPrintIR(wdlSourceFile : Path,
                      irNs: IR.Namespace,
                      verbose: Boolean) : Unit = {
        val trgName: String = Utils.replaceFileSuffix(wdlSourceFile, ".ir.yaml")
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

    // Add a suffix to a filename, before the regular suffix. For example:
    //  xxx.wdl -> xxx.sorted.wdl
    private def addFilenameSuffix(src: Path, secondSuffix: String) : String = {
        val fName = src.toFile().getName()
        val index = fName.lastIndexOf('.')
        if (index == -1) {
            fName + secondSuffix
        } else {
            val prefix = fName.substring(0, index)
            val suffix = fName.substring(index)
            prefix + secondSuffix + suffix
        }
    }

    // Assuming the source file is xxx.wdl, the new name will
    // be xxx.SUFFIX.wdl.
    private def writeToFile(wdlSourceFile: Path, suffix: String, lines: String) : Unit = {
        val trgName: String = addFilenameSuffix(wdlSourceFile, suffix)
        val simpleWdl = Utils.appCompileDirPath.resolve(trgName).toFile
        val fos = new FileWriter(simpleWdl)
        val pw = new PrintWriter(fos)
        pw.println(lines)
        pw.flush()
        pw.close()
        System.err.println(s"Wrote WDL to ${simpleWdl.toString}")
    }

    // Convert a namespace to string representation and apply WDL
    // parser again.  This fixes the ASTs, as well as any other
    // imperfections in our WDL rewriting technology.
    //
    // Note: by keeping the namespace in memory, instead of writing to
    // a temporary file on disk, we keep the resolver valid.
    def washNamespace(rewrittenNs: WdlNamespace,
                      suffix: String,
                      cState: State) : WdlNamespace = {
        val lines: String = WdlPrettyPrinter(true, cState.outputs)
            .apply(rewrittenNs, 0)
            .mkString("\n")
        val cleanNs = WdlNamespace.loadUsingSource(
            lines, None, Some(List(cState.resolver))
        ).get
        if (cState.verbose.on)
            writeToFile(cState.wdlSourceFile, "." + suffix, lines)
        cleanNs
    }

    // Get basic information about the dx environment, and process
    // the compiler flags
    def compilerOptions(options: OptionsMap) : CompileOptions = {
        val verboseKeys: Set[String] = options.get("verbose") match {
            case None => Set.empty
            case Some(modulesToTrace) => modulesToTrace.toSet
        }
        val verbose = Verbose(options contains "verbose", verboseKeys)

        // There are three possible syntaxes:
        //    project-id:/folder
        //    project-id:
        //    /folder
        val (project, folder) = options.get("destination") match {
            case None => (None, "/")
            case Some(List(d)) if d contains ":" =>
                val vec = d.split(":")
                vec.length match {
                    case 1 if (d.endsWith(":")) =>
                        (Some(vec(0)), "/")
                    case 2 => (Some(vec(0)), vec(1))
                    case _ => throw new Exception(s"Invalid path syntex <${d}>")
                }
            case Some(List(d)) if d.startsWith("/") =>
                (None, d)
            case Some(other) => throw new Exception(s"Invalid path syntex <${other}>")
        }
        if (folder.isEmpty)
            throw new Exception(s"destination cannot specify empty folder")

        val dxProject : DXProject = project match {
            case None => Utils.getCurrentProject
            case Some(p) => Utils.lookupProject(p)
        }

        val projName = dxProject.describe.getName
        Utils.trace(verbose.on, s"project: ${projName}")

        // get billTo and region from the project
        val (billTo, region) = Utils.projectDescribeExtraInfo(dxProject)

        val dxWDLrtId = getAssetId(region)
        val compileMode: CompilerFlag.Value = options.get("compilemode") match {
            case None => CompilerFlag.Default
            case Some(List(x)) if (x.toLowerCase == "ir") => CompilerFlag.IR
            case Some(other) => throw new Exception(s"unrecognized compiler flag ${other}")
        }
        val sortMode = options.get("sort") match {
            case None => TopoMode.Check
            case Some(List("normal")) => TopoMode.Sort
            case Some(List("relaxed")) => TopoMode.SortRelaxed
            case _ => throw new Exception("Sanity: bad sort mode")
        }
        val appletTimeout =
            if (options contains "noAppletTimeout") {
                None
            } else {
                // default timeout
                Some(Utils.DEFAULT_APPLET_TIMEOUT)
            }
        val defaults: Option[Path] = options.get("defaults") match {
            case None => None
            case Some(List(p)) => Some(Paths.get(p))
            case _ => throw new Exception("defaults specified twice")
        }
        val inputs: Option[Path] = options.get("inputs") match {
            case None => None
            case Some(List(p)) => Some(Paths.get(p))
            case _ => throw new Exception("inputs specified twice")
        }
        val outputFile: Option[Path] = options.get("outputFile") match {
            case None => None
            case Some(List(p)) => Some(Paths.get(p))
            case _ => throw new Exception("only one output file can be specified")
        }
        CompileOptions(appletTimeout,
                       options contains "archive",
                       billTo,
                       compileMode,
                       defaults,
                       dxProject,
                       dxWDLrtId,
                       folder,
                       options contains "force",
                       inputs,
                       outputFile,
                       region,
                       options contains "reorg",
                       sortMode,
                       verbose)
    }

    def compileBody(wdlSourceFile : Path, cOpt: CompileOptions) : String = {
        // get list of available instance types
        val instanceTypeDB = InstanceTypeDB.query(cOpt.dxProject)

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
        val cState = State(orgNs, outputs(orgNs), wdlSourceFile, resolver, cOpt.verbose)

        // Topologically sort the WDL file so no forward references exist in
        // subsequent steps. Create new file to hold the result.
        //
        // Additionally perform check for cycles in the workflow
        // Assuming the source file is xxx.wdl, the new name will
        // be xxx.sorted.wdl.
        val nsSorted1 = CompilerTopologicalSort.apply(orgNs, cOpt.sortMode, cOpt.verbose)
        val nsSorted = washNamespace(nsSorted1, "sorted", cState)

        // Simplify the original workflow, for example,
        // convert call arguments from expressions to variables.
        val nsExpr1 = CompilerSimplifyExpr.apply(nsSorted, cOpt.verbose)
        val nsExpr = washNamespace(nsExpr1, "simplified", cState)

        // Reorganize the declarations, to minimize the number of
        // applets, stages, and jobs.
        val ns1 = CompilerReorgDecl(nsExpr, cOpt.verbose).apply
        val ns = washNamespace(ns1, "reorg", cState)

        // Compile the WDL workflow into an Intermediate
        // Representation (IR) For some reason, the pretty printer
        // mangles the outputs, which is why we pass the originals
        // unmodified.
        val cef = new CompilerErrorFormatter(ns.terminalMap)
        var irNs = CompilerIR(cState.outputs, cOpt.folder, instanceTypeDB, cef,
                              cOpt.reorg, cOpt.verbose).apply(ns)

        irNs = cOpt.defaults match {
            case Some(path) =>
                // embed the defaults into the IR
                InputFile(cOpt.verbose).embedDefaults(irNs, path)
            case _ => irNs
        }

        // Write out the intermediate representation
        prettyPrintIR(wdlSourceFile, irNs, cOpt.verbose.on)

        // generate dx inputs from the Cromwell-style input specification.
        (cOpt.inputs, irNs.workflow) match {
            case (Some(path), Some(irwf)) =>
                val dxInputs = InputFile(cOpt.verbose).dxFromCromwell(irNs, irwf, path)
                // write back out as xxxx.dx.json
                val filename = Utils.replaceFileSuffix(path, ".dx.json")
                val dxInputFile = path.getParent().resolve(filename)
                Utils.writeFileContent(dxInputFile, dxInputs.prettyPrint)
                Utils.trace(cOpt.verbose.on, s"Wrote dx JSON input file ${dxInputFile}")
            case _ => ()
        }

        // Backend compiler pass
        val wf:Option[DXWorkflow] = cOpt.compileMode match {
            case CompilerFlag.Default =>
                // Generate dx:applets and dx:workflow from the IR
                val (wf, _) =
                    CompilerNative(cOpt.dxWDLrtId, cOpt.dxProject, instanceTypeDB,
                                   cOpt.folder, cef,
                                   cOpt.appletTimeout,
                                   cOpt.force, cOpt.archive, cOpt.verbose).apply(irNs)
                wf
            case CompilerFlag.IR =>
                None
        }

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
        val cOpt:CompileOptions = compilerOptions(options)
        try {
            val dxc = compileBody(Paths.get(wdlSourceFile), cOpt)
            SuccessfulTermination(dxc)
        } catch {
            case e : Throwable =>
                return UnsuccessfulTermination(Utils.exceptionToString(e))
        }
    }

    def dxci(args: Seq[String]): Termination = {
        val options =
            try {
                parseCmdlineOptions(args.toList)
            } catch {
                case e : Throwable =>
                    return BadUsageTermination(Utils.exceptionToString(e))
            }
        if (options contains "help")
            return BadUsageTermination("")
        val cOpt:CompileOptions = compilerOptions(options)
        val output = cOpt.outputFile match {
            case None => throw new Exception("Output file not specified")
            case Some(x) => x
        }
        try {
            DxCI.apply(cOpt.dxProject, cOpt.folder, output, cOpt.force, cOpt.verbose)
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
            op match {
                case InternalOp.Eval =>
                    RunnerEval.apply(workflowOfNamespace(ns),
                                     jobInputPath, jobOutputPath, jobInfoPath)
                case InternalOp.MiniWorkflow =>
                    RunnerMiniWorkflow.apply(workflowOfNamespace(ns),
                                             jobInputPath, jobOutputPath, jobInfoPath)
                case InternalOp.TaskEpilog =>
                    val runner = RunnerTask(taskOfNamespace(ns), cef)
                    runner.epilog(jobInputPath, jobOutputPath, jobInfoPath)
                case InternalOp.TaskProlog =>
                    val runner = RunnerTask(taskOfNamespace(ns), cef)
                    runner.prolog(jobInputPath, jobOutputPath, jobInfoPath)
                case InternalOp.TaskRelaunch =>
                    val runner = RunnerTask(taskOfNamespace(ns), cef)
                    runner.relaunch(jobInputPath, jobOutputPath, jobInfoPath)
                case InternalOp.WorkflowOutputs =>
                    RunnerWorkflowOutputs.apply(workflowOfNamespace(ns),
                                                jobInputPath, jobOutputPath, jobInfoPath, false)
                case InternalOp.WorkflowOutputsAndReorg =>
                    RunnerWorkflowOutputs.apply(workflowOfNamespace(ns),
                                                jobInputPath, jobOutputPath, jobInfoPath, true)
            }
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
                case Actions.DXCI => dxci(args.tail)
                case Actions.Internal => internalOp(args.tail)
                case Actions.Version => SuccessfulTermination(getVersion())
            }
        }
    }

    val usageMessage =
        s"""|java -jar dxWDL.jar <action> <parameters> [options]
            |
            |Actions:
            |
            |compile <WDL file>
            |  Compile a wdl file into a dnanexus workflow.
            |  Optionally, specify a destination path on the
            |  platform. If a WDL inputs files is specified, a dx JSON
            |  inputs file is generated from it.
            |  options:
            |    -archive              Archive older versions of applets
            |    -compileMode <string> Compilation mode, a debugging flag
            |    -defaults <string>    Path to Cromwell formatted default values file
            |    -destination <string> Output folder on the platform for workflow
            |    -force                Delete existing applets/workflows
            |    -inputs <string>      Path to Cromwell formatted input file
            |    -noAppletTimeout      By default, applets cannot run more than ${Utils.DEFAULT_APPLET_TIMEOUT} hours.
            |                          Remove this limitation.
            |    -reorg                Reorganize workflow output files
            |    -sort [string]        Sort call graph, to avoid forward references
            |    -verbose [flag]       Print detailed progress reports
            |
            |config
            |  Print the configuration parameters
            |
            |dxci
            |  Foreign Function Interface. Create stubs for calling dx
            |  applets, and store them as WDL tasks in a local file. Allows
            |  calling existing platform applets without modification.
            |  options:
            |    -folder <string>      Platform folder to search for applets
            |    -o <string>           Destination file for WDL task definitions
            |
            |internal <sub command>
            |  Various internal commands
            |
            |version
            |  Report the current version
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
