package dxWDL

import com.dnanexus.{DXProject}
import com.typesafe.config._
import dxWDL.compiler.IR
import java.nio.file.{Path, Paths}
import scala.collection.mutable.HashMap
import spray.json._
import spray.json.JsString
import wdl.draft2.model.{WdlExpression, WdlNamespace, WdlTask, WdlNamespaceWithWorkflow}
import wom.values._

object Main extends App {
    sealed trait Termination
    case class SuccessfulTermination(output: String) extends Termination
    case class SuccessfulTerminationIR(ir: IR.Namespace) extends Termination
    case class UnsuccessfulTermination(output: String) extends Termination
    case class BadUsageTermination(info: String) extends Termination

    type OptionsMap = Map[String, List[String]]

    object Actions extends Enumeration {
        val Compile, Config, DXNI, Internal, Version  = Value
    }
    object InternalOp extends Enumeration {
        val Collect,
            WfFragment,
            TaskEpilog, TaskProlog, TaskRelaunch,
            WorkflowOutputReorg = Value
    }

    case class DxniOptions(force: Boolean,
                           outputFile: Option[Path],
                           recursive: Boolean,
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
                    case "extras" =>
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
                    case ("imports"|"p") =>
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

    def extrasParse(extraFields: Map[String, JsValue]) : Extras = {
        def wdlExpressionFromJsValue(jsv: JsValue) : WdlExpression = {
            val wValue: WomValue = jsv match {
                case JsBoolean(b) => WomBoolean(b.booleanValue)
                case JsNumber(bnm) => WomInteger(bnm.intValue)
                //            case JsNumber(bnm) => WomFloat(bnm.doubleValue)
                case JsString(s) => WomString(s)
                case other => throw new Exception(s"Unsupported json value ${other}")
            }
            WdlExpression.fromString(wValue.toWomString)
        }

        // Guardrail, check the fields are actually supported
        for (k <- extraFields.keys) {
            if (!(Utils.EXTRA_KEYS_SUPPORTED contains k))
                throw new Exception(s"Unsupported special option ${k}, we currently support ${Utils.EXTRA_KEYS_SUPPORTED}")
        }
        val defaultRuntimeAttributes = extraFields.get("default_runtime_attributes") match {
            case None =>
                Map.empty[String, WdlExpression]
            case Some(x) =>
                x.asJsObject.fields.map{ case (name, jsValue) =>
                    name -> wdlExpressionFromJsValue(jsValue)
                }.toMap
        }
        Extras(defaultRuntimeAttributes)
    }

    // Get basic information about the dx environment, and process
    // the compiler flags
    private def compilerOptions(options: OptionsMap) : CompilerOptions = {
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
        val extras = options.get("extras") match {
            case None => None
            case Some(List(p)) =>
                val contents = Utils.readFileContent(Paths.get(p))
                Some(extrasParse(contents.parseJson.asJsObject.fields))
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
        val verboseKeys: Set[String] = options.get("verbose") match {
            case None => Set.empty
            case Some(modulesToTrace) => modulesToTrace.toSet
        }
        val verbose = Verbose(options contains "verbose",
                              options contains "quiet",
                              verboseKeys)
        CompilerOptions(options contains "archive",
                        compileMode,
                        defaults,
                        extras,
                        options contains "force",
                        imports,
                        inputs,
                        options contains "locked",
                        options contains "reorg",
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
        DxniOptions(options contains "force",
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
                    val ir: IR.Namespace = compiler.CompilerTop.applyOnlyIR(wdlSourceFile, cOpt)
                    return SuccessfulTerminationIR(ir)

                case CompilerFlag.Default =>
                    val (dxProject, folder) = pathOptions(options, cOpt.verbose)
                    val retval = compiler.CompilerTop.apply(wdlSourceFile, folder, dxProject, cOpt)
                    val desc = retval.getOrElse("")
                    return SuccessfulTermination(desc)
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
        val (dOpt, dxProject, folder) =
            try {
                val dxniOpt = dxniOptions(options)
                val (dxProject, folder) = pathOptions(options, dxniOpt.verbose)
                (dxniOpt, dxProject, folder)
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
            compiler.DxNI.apply(dxProject, folder, output, dOpt.recursive, dOpt.force, dOpt.verbose)
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
        val cef = new CompilerErrorFormatter(wdlDefPath, ns.terminalMap)
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
                            val r = runner.Task(task, cef, true)
                            r.epilog(inputSpec, outputSpec, inputs)
                        case InternalOp.TaskProlog =>
                            val r = runner.Task(task, cef, true)
                            r.prolog(inputSpec, outputSpec, inputs)
                        case InternalOp.TaskRelaunch =>
                            val r = runner.Task(task, cef, true)
                            r.relaunch(inputSpec, outputSpec, inputs)
                    }
                } else {
                    val inputs = WdlVarLinks.loadJobInputsAsLinks(inputLines, inputSpec, None)
                    val nswf = ns.asInstanceOf[WdlNamespaceWithWorkflow]
                    op match {
                        case InternalOp.Collect =>
                            runner.WfFragment.apply(nswf,
                                                    inputSpec, outputSpec, inputs, orgInputs,
                                                    RunnerWfFragmentMode.Collect, true)
                        case InternalOp.WfFragment =>
                            runner.WfFragment.apply(nswf,
                                                    inputSpec, outputSpec, inputs, orgInputs,
                                                    RunnerWfFragmentMode.Launch, true)
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
            |      -archive               Archive older versions of applets
            |      -compileMode <string>  Compilation mode, a debugging flag
            |      -defaults <string>     File with Cromwell formatted default values (JSON)
            |      -destination <string>  Output path on the platform for workflow
            |      -extras <string>       JSON formatted file with extra options, for example
            |                             default runtime options for tasks.
            |      -inputs <string>       File with Cromwell formatted inputs
            |      -p | -imports <string> Directory to search for imported WDL files
            |      -locked                Create a locked-down workflow
            |      -reorg                 Reorganize workflow output files
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
        case SuccessfulTerminationIR(s) => println("Intermediate representation")
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
