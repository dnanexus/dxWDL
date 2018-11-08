package dxWDL

import com.dnanexus.{DXProject}
import com.typesafe.config._
import dxWDL.util._
import java.nio.file.{Path, Paths}
import scala.collection.mutable.HashMap
import spray.json._
import spray.json.JsString
import wom.callable.{CallableTaskDefinition, ExecutableTaskDefinition}
import wom.executable.WomBundle

object Main extends App {
    sealed trait Termination
    case class SuccessfulTermination(output: String) extends Termination
    case class SuccessfulTerminationIR(ir: dxWDL.compiler.IR.Bundle) extends Termination
    case class UnsuccessfulTermination(output: String) extends Termination
    case class BadUsageTermination(info: String) extends Termination

    type OptionsMap = Map[String, List[String]]

    object Actions extends Enumeration {
        val Compile, Config, Internal, Version  = Value
    }
    object InternalOp extends Enumeration {
        val Collect,
            WfFragment,
            TaskCheckInstanceType, TaskEpilog, TaskProlog, TaskRelaunch,
            WorkflowOutputReorg = Value
    }

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
                    case "leaveWorkflowsOpen" =>
                        checkNumberOfArguments(keyword, 0, subargs)
                        (keyword, "")
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
            case None => CompilerFlag.All
            case Some(List(x)) if (x.toLowerCase == "IR".toLowerCase) => CompilerFlag.IR
            case Some(List(x)) if (x.toLowerCase == "NativeWithoutRuntimeAsset".toLowerCase) => CompilerFlag.NativeWithoutRuntimeAsset
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
                        options contains "leaveWorkflowsOpen",
                        options contains "locked",
                        options contains "projectWideReuse",
                        options contains "reorg",
                        runtimeDebugLevel,
                        verbose)
    }

    def compile(args: Seq[String]): Termination = {
        if (args.isEmpty)
            return BadUsageTermination("WDL file to compile is missing")
        val sourceFile = Paths.get(args.head)
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
                    val ir: compiler.IR.Bundle = compiler.Top.applyOnlyIR(sourceFile, cOpt)
                    return SuccessfulTerminationIR(ir)

                case CompilerFlag.All
                       | CompilerFlag.NativeWithoutRuntimeAsset =>
                    val (dxProject, folder) = pathOptions(options, cOpt.verbose)
                    val retval = compiler.Top.apply(sourceFile, folder, dxProject, cOpt)
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

    private def isTaskOp(op: InternalOp.Value) : Boolean = {
        op match {
            case InternalOp.TaskCheckInstanceType |
                    InternalOp.TaskEpilog |
                    InternalOp.TaskProlog |
                    InternalOp.TaskRelaunch => true
            case _ => false
        }
    }

    // Extract the only task from a namespace
    private def getMainTask(bundle: WomBundle) : CallableTaskDefinition = {
        // check if the primary is nonempty
        val task: Option[CallableTaskDefinition] = bundle.primaryCallable match  {
            case Some(task : CallableTaskDefinition) => Some(task)
            case Some(exec : ExecutableTaskDefinition) => Some(exec.callableTaskDefinition)
            case _ => None
        }
        task match {
            case Some(x) => x
            case None =>
                // primary is empty, check the allCallables map
                if (bundle.allCallables.size != 1)
                    throw new Exception("WDL file must contains exactly one task")
                val (_, task) = bundle.allCallables.head
                task match {
                    case task : CallableTaskDefinition => task
                    case exec : ExecutableTaskDefinition => exec.callableTaskDefinition
                    case _ => throw new Exception("Cannot find task inside WDL file")
                }
        }
    }

    private def appletAction(op: InternalOp.Value,
                             wdlDefPath: Path,
                             jobInputPath: Path,
                             jobOutputPath: Path,
                             rtDebugLvl: Int): Termination = {
        val (_, womBundle: WomBundle, allSources) = ParseWomSourceFile.apply(wdlDefPath)
        val task : CallableTaskDefinition = getMainTask(womBundle)
        assert(allSources.size == 1)
        val sourceDict  = ParseWomSourceFile.scanForTasks(allSources.values.head)
        assert(sourceDict.size == 1)
        val taskSourceCode = sourceDict.values.head

        // Parse the inputs, convert to WOM values. Delay downloading files
        // from the platform, we may not need to access them.
        val inputLines : String = Utils.readFileContent(jobInputPath)
        val inputs = runner.JobInputOutput.loadInputs(inputLines, task)

        // Figure out the available instance types, and their prices,
        // by reading the file
        val dbRaw = Utils.readFileContent(Paths.get("/" + Utils.INSTANCE_TYPE_DB_FILENAME))
        val instanceTypeDB = dbRaw.parseJson.convertTo[InstanceTypeDB]
        val r = runner.Task(task, taskSourceCode, instanceTypeDB, rtDebugLvl)

        if (op == InternalOp.TaskCheckInstanceType) {
            // special operation to check if this task is on the right instance type
            val correctInstanceType:Boolean = r.checkInstanceType(inputs)
            SuccessfulTermination(correctInstanceType.toString)
        } else {
            val outputFields: Map[String, JsValue] =
                if (isTaskOp(op)) {
                    // Running tasks
                    op match {
                        case InternalOp.TaskEpilog => r.epilog(inputs)
                        case InternalOp.TaskProlog => r.prolog(inputs)
                        case InternalOp.TaskRelaunch => r.relaunch(inputs)
                    }
                } else {
                    throw new Exception("not currently supported")
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
                case Actions.Internal => internalOp(args.tail)
                case Actions.Version => SuccessfulTermination(Utils.getVersion())
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
