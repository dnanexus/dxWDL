package dxWDL

import com.dnanexus.{DXProject}
import com.typesafe.config._
import dxWDL.compiler.IR
import java.nio.file.{Path, Paths}
import org.apache.log4j.{Level, Logger}
import scala.collection.mutable.HashMap
import spray.json._
import spray.json.JsString
import wdl.draft2.model.{WdlNamespace, WdlTask, WdlNamespaceWithWorkflow}


object Main extends App {
    sealed trait Termination
    case class SuccessfulTermination(output: String) extends Termination
    case class SuccessfulTerminationIR(ir: IR.Namespace) extends Termination
    case class UnsuccessfulTermination(output: String) extends Termination
    case class BadUsageTermination(info: String) extends Termination

    type OptionsMap = Map[String, List[String]]

    object Actions extends Enumeration {
        val Compile, Config, DXNI, Internal, Version, Wom  = Value
    }
    object InternalOp extends Enumeration {
        val Collect,
            WfFragment,
            TaskCheckInstanceType, TaskEpilog, TaskProlog, TaskRelaunch,
            WorkflowOutputReorg = Value
    }

    case class DxniOptions(apps: Boolean,
                           force: Boolean,
                           outputFile: Option[Path],
                           recursive: Boolean,
                           verbose: Verbose)

    private def normKey(s: String) : String= {
        s.replaceAll("_", "").toUpperCase
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
        def keywordValueIsList = Set("inputs", "imports", "verbose")
        def normKeyword(word: String) : String = {
            // normalize a keyword, remove leading dashes
            // letters to lowercase.
            //
            // "--Archive" -> "archive"
            // "--archive-only -> "archiveonly"
            word.replaceAll("-", "")
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
                    case "apps" =>
                        checkNumberOfArguments(keyword, 0, subargs)
                        (keyword, "")
                    case "archive" =>
                        checkNumberOfArguments(keyword, 0, subargs)
                        (keyword, "")
                    case "compileMode" =>
                        checkNumberOfArguments(keyword, 1, subargs)
                        (keyword, subargs.head)
                    case "defaults" =>
                        checkNumberOfArguments(keyword, 1, subargs)
                        (keyword, subargs.head)
                    case "destination" =>
                        checkNumberOfArguments(keyword, 1, subargs)
                        (keyword, subargs.head)
                    case "destination_unicode" =>
                        checkNumberOfArguments(keyword, 1, subargs)
                        (keyword, subargs.head)
                    case "extras" =>
                        checkNumberOfArguments(keyword, 1, subargs)
                        (keyword, subargs.head)
                    case "fatalValidationWarnings" =>
                        checkNumberOfArguments(keyword, 0, subargs)
                        (keyword, "")
                    case "folder" =>
                        checkNumberOfArguments(keyword, 1, subargs)
                        (keyword, subargs.head)
                    case ("force"|"f"|"overwrite") =>
                        checkNumberOfArguments(keyword, 0, subargs)
                        ("force", "")
                    case "help" =>
                        checkNumberOfArguments(keyword, 0, subargs)
                        (keyword, "")
                    case ("input" | "inputs") =>
                        checkNumberOfArguments(keyword, 1, subargs)
                        (keyword, subargs.head)
                    case ("imports"|"p") =>
                        checkNumberOfArguments(keyword, 1, subargs)
                        (keyword, subargs.head)
                    case "locked" =>
                        checkNumberOfArguments(keyword, 0, subargs)
                        (keyword, "")
                    case ("o"|"output"|"outputFile") =>
                        checkNumberOfArguments(keyword, 1, subargs)
                        ("outputFile", subargs.head)
                    case "project" =>
                        checkNumberOfArguments(keyword, 1, subargs)
                        (keyword, subargs.head)
                    case "projectWideReuse" =>
                        checkNumberOfArguments(keyword, 0, subargs)
                        (keyword, "")
                    case ("q"|"quiet") =>
                        checkNumberOfArguments(keyword, 0, subargs)
                        ("quiet", "")
                    case ("r"|"recursive") =>
                        checkNumberOfArguments(keyword, 0, subargs)
                        ("recursive", "")
                    case "reorg" =>
                        checkNumberOfArguments(keyword, 0, subargs)
                        (keyword, "")
                    case "runtimeDebugLevel" =>
                        checkNumberOfArguments(keyword, 1, subargs)
                        (keyword, subargs.head)
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
                    case Some(x) if (keywordValueIsList contains nKeyword) =>
                        // append to the already existing verbose flags
                        options(nKeyword) = value :: x
                    case Some(x) =>
                        // overwrite the previous flag value
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


    private def pathOptions(options: OptionsMap,
                            verbose: Verbose) : (DXProject, String) = {
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
        var destinationOpt : Option[String] = options.get("destination") match {
            case None => None
            case Some(List(d)) => Some(d)
            case Some(other) => throw new Exception(s"Invalid path syntex <${other}>")
        }
        options.get("destination_unicode") match {
            case None => None
            case Some(List(d)) =>
                destinationOpt = Some(Utils.unicodeFromHex(d))
            case Some(other) => throw new Exception(s"Invalid path syntex <${other}>")
        }

        // There are three possible syntaxes:
        //    project-id:/folder
        //    project-id:
        //    /folder
        destinationOpt match {
            case None => ()
            case Some(d) if d contains ":" =>
                val vec = d.split(":")
                vec.length match {
                    case 1 if (d.endsWith(":")) =>
                        projectOpt = Some(vec(0))
                    case 2 =>
                        projectOpt = Some(vec(0))
                        folderOpt = Some(vec(1))
                    case _ => throw new Exception(s"Invalid path syntex <${d}>")
                }
            case Some(d) if d.startsWith("/") =>
                folderOpt = Some(d)
            case Some(other) => throw new Exception(s"Invalid path syntex <${other}>")
        }

        // Use the current dx path, if nothing else was
        // specified
        val (projectRaw, folderRaw) = (projectOpt, folderOpt) match {
            case (None, _) => throw new Exception("project is unspecified")
            case (Some(p), None) => (p, "/")
            case (Some(p), Some(d)) =>(p, d)
        }

        if (folderRaw.isEmpty)
            throw new Exception(s"Cannot specify empty folder")
        if (!folderRaw.startsWith("/"))
            throw new Exception(s"Folder must start with '/'")
        val dxFolder = folderRaw
        val dxProject = DxPath.lookupProject(projectRaw)
        Utils.trace(verbose.on,
                    s"""|project ID: ${dxProject.getId}
                        |folder: ${dxFolder}""".stripMargin)
        (dxProject, dxFolder)
    }

    private def parseRuntimeDebugLevel(numberStr: String) : Int = {
        val rtDebugLvl =
            try {
                numberStr.toInt
            } catch {
                case e : java.lang.NumberFormatException =>
                    throw new Exception(s"""|the runtimeDebugLevel flag takes an integer input,
                                            |${numberStr} is not of type int"""
                                            .stripMargin.replaceAll("\n", " "))
            }
        if (rtDebugLvl < 0 || rtDebugLvl > 2)
            throw new Exception(s"""|the runtimeDebugLevel flag must be one of {0, 1, 2}.
                                    |Value ${rtDebugLvl} is out of bounds."""
                                    .stripMargin.replaceAll("\n", " "))
        rtDebugLvl
    }

    // Get basic information about the dx environment, and process
    // the compiler flags
    private def compilerOptions(options: OptionsMap) : CompilerOptions = {
        // First: get the verbosity mode. It is used almost everywhere.
        val verboseKeys: Set[String] = options.get("verbose") match {
            case None => Set.empty
            case Some(modulesToTrace) => modulesToTrace.toSet
        }
        val verbose = Verbose(options contains "verbose",
                              options contains "quiet",
                              verboseKeys)

        val compileMode: CompilerFlag.Value = options.get("compileMode") match {
            case None => CompilerFlag.Default
            case Some(List(x)) if (x.toLowerCase == "ir") => CompilerFlag.IR
            case Some(other) => throw new Exception(s"unrecognized compiler flag ${other}")
        }
        val defaults: Option[Path] = options.get("defaults") match {
            case None => None
            case Some(List(p)) => Some(Paths.get(p))
            case _ => throw new Exception("defaults specified twice")
        }
        val extras = options.get("extras") match {
            case None => None
            case Some(List(p)) =>
                val contents = Utils.readFileContent(Paths.get(p))
                Some(Extras.parse(contents.parseJson, verbose))
            case _ => throw new Exception("extras specified twice")
        }
        val inputs: List[Path] = options.get("inputs") match {
            case None => List.empty
            case Some(pl) => pl.map(p => Paths.get(p))
        }
        val imports: List[Path] = options.get("imports") match {
            case None => List.empty
            case Some(pl) => pl.map(p => Paths.get(p))
        }
        val runtimeDebugLevel:Option[Int] = options.get("runtimeDebugLevel") match {
            case None => None
            case Some(List(numberStr)) => Some(parseRuntimeDebugLevel(numberStr))
            case _ => throw new Exception("debug level specified twice")
        }
        CompilerOptions(options contains "archive",
                        compileMode,
                        defaults,
                        extras,
                        options contains "fatalValidationWarnings",
                        options contains "force",
                        imports,
                        inputs,
                        options contains "locked",
                        options contains "projectWideReuse",
                        options contains "reorg",
                        runtimeDebugLevel,
                        verbose)
    }

    private def dxniOptions(options: OptionsMap) : DxniOptions = {
        val outputFile: Option[Path] = options.get("outputFile") match {
            case None => None
            case Some(List(p)) => Some(Paths.get(p))
            case _ => throw new Exception("only one output file can be specified")
        }
        val verboseKeys: Set[String] = options.get("verbose") match {
            case None => Set.empty
            case Some(modulesToTrace) => modulesToTrace.toSet
        }
        val verbose = Verbose(options contains "verbose",
                              options contains "quiet",
                              verboseKeys)
        DxniOptions(options contains "apps",
                    options contains "force",
                    outputFile,
                    options contains "recursive",
                    verbose)
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
        try {
            val cOpt = compilerOptions(options)
            cOpt.compileMode match {
                case CompilerFlag.IR =>
                    val ir: IR.Namespace = compiler.Top.applyOnlyIR(wdlSourceFile, cOpt)
                    return SuccessfulTerminationIR(ir)

                case CompilerFlag.Default =>
                    val (dxProject, folder) = pathOptions(options, cOpt.verbose)
                    val retval = compiler.Top.apply(wdlSourceFile, folder, dxProject, cOpt)
                    val desc = retval.getOrElse("")
                    return SuccessfulTermination(desc)
            }
        } catch {
            case e : NamespaceValidationException =>
                return UnsuccessfulTermination(
                    "Namespace validation error\n\n" +
                    e.getMessage)
            case e : Throwable =>
                return UnsuccessfulTermination(Utils.exceptionToString(e))
        }
    }

    def dxniApplets(options: OptionsMap,
                    dOpt: DxniOptions,
                    outputFile: Path): Termination = {
        val (dxProject, folder) =
            try {
                pathOptions(options, dOpt.verbose)
            } catch {
                case e: Throwable =>
                    return BadUsageTermination(Utils.exceptionToString(e))
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
            compiler.DxNI.apply(dxProject, folder, outputFile, dOpt.recursive, dOpt.force, dOpt.verbose)
            SuccessfulTermination("")
        } catch {
            case e : Throwable =>
                return UnsuccessfulTermination(Utils.exceptionToString(e))
        }
    }

    def dxniApps(options: OptionsMap,
                 dOpt: DxniOptions,
                 outputFile: Path): Termination = {
        try {
            compiler.DxNI.applyApps(outputFile, dOpt.force, dOpt.verbose)
            SuccessfulTermination("")
        } catch {
            case e : Throwable =>
                return UnsuccessfulTermination(Utils.exceptionToString(e))
        }
    }

    def dxni(args: Seq[String]): Termination = {
        try {
            val options = parseCmdlineOptions(args.toList)
            if (options contains "help")
                return BadUsageTermination("")

            val dOpt = dxniOptions(options)
            val output = dOpt.outputFile match {
                case None => throw new Exception("Output file not specified")
                case Some(x) => x
            }

            if (dOpt.apps)
                dxniApps(options, dOpt, output)
            else
                dxniApplets(options, dOpt, output)
        } catch {
            case e: Throwable =>
                return BadUsageTermination(Utils.exceptionToString(e))
        }
    }

    def wom(args: Seq[String]): Termination = {
        try {
            val options = parseCmdlineOptions(args.toList)
            if (options contains "help")
                return BadUsageTermination("")

            val wdlSourceFile = args.head
            val bundle = Wom.getBundle(wdlSourceFile)
            SuccessfulTermination(bundle.toString)
        } catch {
            case e: Throwable =>
                return BadUsageTermination(Utils.exceptionToString(e))
        }
    }


    // Extract the only task from a namespace
    def taskOfNamespace(ns: WdlNamespace) : WdlTask = {
        val numTasks = ns.tasks.length
        if (numTasks != 1)
            throw new Exception(s"WDL file contains ${numTasks} tasks, instead of 1")
        ns.tasks.head
    }

    private def isTaskOp(op: InternalOp.Value) : Boolean = {
        op match {
            case InternalOp.TaskCheckInstanceType |
                    InternalOp.TaskEpilog |
                    InternalOp.TaskProlog |
                    InternalOp.TaskRelaunch => true
            case _ => false
        }
    }

    private def appletAction(op: InternalOp.Value,
                             wdlDefPath: Path,
                             jobInputPath: Path,
                             jobOutputPath: Path,
                             rtDebugLvl: Int): Termination = {
        val ns = WdlNamespace.loadUsingPath(wdlDefPath, None, None).get
        val cef = new CompilerErrorFormatter(wdlDefPath.toString, ns.terminalMap)

        // Figure out input/output types
        val (inputSpec, outputSpec) = Utils.loadExecInfo

        // Parse the inputs, do not download files from the platform,
        // they will be passed as links.
        val inputLines : String = Utils.readFileContent(jobInputPath)
        val orgInputs = inputLines.parseJson

        // Figure out the available instance types, and their prices,
        // by reading the file
        val dbRaw = Utils.readFileContent(Paths.get("/" + Utils.INSTANCE_TYPE_DB_FILENAME))
        val instanceTypeDB =dbRaw.parseJson.convertTo[InstanceTypeDB]

        if (op == InternalOp.TaskCheckInstanceType) {
            // special operation to check if this task is on the right instance type
            val task = taskOfNamespace(ns)
            val inputs = WdlVarLinks.loadJobInputsAsLinks(inputLines, inputSpec, Some(task))
            val r = runner.Task(task, instanceTypeDB, cef, rtDebugLvl)
            val correctInstanceType:Boolean = r.checkInstanceType(inputSpec, outputSpec, inputs)
            SuccessfulTermination(correctInstanceType.toString)
        } else {
            val outputFields: Map[String, JsValue] =
                if (isTaskOp(op)) {
                    // Running tasks
                    val task = taskOfNamespace(ns)
                    val inputs = WdlVarLinks.loadJobInputsAsLinks(inputLines, inputSpec, Some(task))
                    op match {
                        case InternalOp.TaskEpilog =>
                            val r = runner.Task(task, instanceTypeDB, cef, rtDebugLvl)
                            r.epilog(inputSpec, outputSpec, inputs)
                        case InternalOp.TaskProlog =>
                            val r = runner.Task(task, instanceTypeDB, cef, rtDebugLvl)
                            r.prolog(inputSpec, outputSpec, inputs)
                        case InternalOp.TaskRelaunch =>
                            val r = runner.Task(task, instanceTypeDB, cef, rtDebugLvl)
                            r.relaunch(inputSpec, outputSpec, inputs)
                    }
                } else {
                    val inputs = WdlVarLinks.loadJobInputsAsLinks(inputLines, inputSpec, None)
                    val nswf = ns.asInstanceOf[WdlNamespaceWithWorkflow]
                    op match {
                        case InternalOp.Collect =>
                            runner.WfFragment.apply(nswf,
                                                    instanceTypeDB,
                                                    inputSpec, outputSpec, inputs, orgInputs,
                                                    RunnerWfFragmentMode.Collect, rtDebugLvl)
                        case InternalOp.WfFragment =>
                            runner.WfFragment.apply(nswf,
                                                    instanceTypeDB,
                                                    inputSpec, outputSpec, inputs, orgInputs,
                                                    RunnerWfFragmentMode.Launch, rtDebugLvl)
                        case InternalOp.WorkflowOutputReorg =>
                            runner.WorkflowOutputReorg(true).apply(nswf, inputSpec, outputSpec, inputs)
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
        }
    }

    def internalOp(args : Seq[String]) : Termination = {
        val op = InternalOp.values find (x => normKey(x.toString) == normKey(args.head))
        op match {
            case None =>
                UnsuccessfulTermination(s"unknown internal action ${args.head}")
            case Some(x) if (args.length == 4) =>
                val wdlDefPath = Paths.get(args(1))
                val homeDir = Paths.get(args(2))
                val rtDebugLvl = parseRuntimeDebugLevel(args(3))
                val (jobInputPath, jobOutputPath, jobErrorPath, _) =
                    Utils.jobFilesOfHomeDir(homeDir)
                try {
                    appletAction(x, wdlDefPath, jobInputPath, jobOutputPath, rtDebugLvl)
                } catch {
                    case e : Throwable =>
                        writeJobError(jobErrorPath, e)
                        UnsuccessfulTermination(s"failure running ${op}")
                }
            case Some(_) =>
                BadUsageTermination(s"All applet actions take a WDL file, and a home directory (${args})")
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
                case Actions.Version => SuccessfulTermination(Utils.getVersion())
                case Actions.Wom => wom(args.tail)
            }
        }
    }

    // Silence the log4j package
    Logger.getRootLogger().removeAllAppenders()
    Logger.getRootLogger().setLevel(Level.OFF);

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
            |      -archive               Archive older versions of applets
            |      -compileMode <string>  Compilation mode, a debugging flag
            |      -defaults <string>     File with Cromwell formatted default values (JSON)
            |      -destination <string>  Output path on the platform for workflow
            |      -destination_unicode <string>  destination in unicode encoded as hexadecimal
            |      -extras <string>       JSON formatted file with extra options, for example
            |                             default runtime options for tasks.
            |      -inputs <string>       File with Cromwell formatted inputs
            |      -locked                Create a locked-down workflow
            |      -p | -imports <string> Directory to search for imported WDL files
            |      -projectWideReuse      Look for existing applets/workflows in the entire project
            |                             before generating new ones. The normal search scope is the
            |                             target folder only.
            |      -reorg                 Reorganize workflow output files
            |      -runtimeDebugLevel [0,1,2] How much debug information to write to the
            |                             job log at runtime. Zero means write the minimum,
            |                             one is the default, and two is for internal debugging.
            |
            |  dxni
            |    Dx Native call Interface. Create stubs for calling dx
            |    executables (apps/applets/workflows), and store them as WDL
            |    tasks in a local file. Allows calling existing platform executables
            |    without modification. Default is to look for applets.
            |    options:
            |      -apps                  Search only for global apps.
            |      -o <string>            Destination file for WDL task definitions
            |      -r | recursive         Recursive search
            |
            |  wom
            |    Experimental. Compile into the WOM model.
            |
            |Common options
            |    -destination             Full platform path (project:/folder)
            |    -f | force               Delete existing applets/workflows
            |    -folder <string>         Platform folder
            |    -project <string>        Platform project
            |    -quiet                   Do not print warnings or informational outputs
            |    -verbose [flag]          Print detailed progress reports
            |""".stripMargin

    val termination = dispatchCommand(args)

    termination match {
        case SuccessfulTermination(s) => println(s)
        case SuccessfulTerminationIR(s) => println("Intermediate representation")
        case BadUsageTermination(s) if (s == "") =>
            Console.err.println(usageMessage)
            System.exit(1)
        case BadUsageTermination(s) =>
            Utils.error(s)
            System.exit(1)
        case UnsuccessfulTermination(s) =>
            Utils.error(s)
            System.exit(1)
    }
}
