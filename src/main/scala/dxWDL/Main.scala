package dxWDL

import com.dnanexus.{DXApplet, DXProject, DXUtil, DXContainer, DXWorkflow}
import com.typesafe.config._
import java.io.{File, FileWriter, PrintWriter}
import java.nio.file.{Path, Paths, Files}
import net.jcazevedo.moultingyaml._
import scala.util.{Failure, Success, Try}
import spray.json._
import spray.json.DefaultJsonProtocol
import spray.json.JsString
import wdl4s.{ImportResolver, Task, WdlNamespace, WdlNamespaceWithWorkflow, WdlSource,
    Workflow, WorkflowOutput}

object Main extends App {
    sealed trait Termination
    case class SuccessfulTermination(output: String) extends Termination
    case class UnsuccessfulTermination(output: String) extends Termination
    case class BadUsageTermination(info: String) extends Termination

    type OptionsMap = Map[String, String]

    object Actions extends Enumeration {
        val Compile, Config, Internal, Version  = Value
    }
    object InternalOp extends Enumeration {
        val Eval, LaunchScatter,
            TaskEpilog, TaskProlog, TaskRelaunch = Value
    }

    // Compiler state.
    // Packs common arguments passed between methods.
    case class State(ns: WdlNamespace,
                     outputs: Seq[WorkflowOutput],
                     wdlSourceFile: Path,
                     resolver: ImportResolver,
                     verbose: Boolean)

    private def normKey(s: String) : String= {
        s.replaceAll("_", "").toUpperCase
    }

    // load configuration information
    private def getVersion() : String = {
        val config = ConfigFactory.load()
        val version = config.getString("dxWDL.version")
        version
    }

    private def getAssetId() : String = {
        val config = ConfigFactory.load()
        val assetId = config.getString("dxWDL.asset_id")
        assert(assetId.startsWith("record"))
        assetId
    }

    def outputs(ns: WdlNamespace) : Seq[WorkflowOutput] = {
        ns match {
            case nswf: WdlNamespaceWithWorkflow =>
                val wf = nswf.workflow
                wf.outputs
            case _ => List.empty[WorkflowOutput]
        }
    }

    // parse extra command line arguments
    def parseCmdlineOptions(arglist: List[String]) : OptionsMap = {
        def keyword(word: String) : String = {
            // normalize a keyword, remove leading dashes, and convert
            // letters to lowercase.
            //
            // "--Archive" -> "archive"
            word.replaceAll("-", "").toLowerCase
        }
        def nextOption(map : OptionsMap, list: List[String]) : OptionsMap = {
            list match {
                case Nil => map
                case head :: tail =>
                    (keyword(head) :: tail) match {
                        case Nil =>
                            throw new IllegalArgumentException(s"sanity")
                        case "archive" :: tail =>
                            nextOption(map ++ Map("archive" -> ""), tail)
                        case ("force"|"f"|"overwrite") :: tail =>
                            nextOption(map ++ Map("force" -> ""), tail)
                        case "inputs" :: value :: tail =>
                            nextOption(map ++ Map("inputFile" -> value.toString), tail)
                        case "mode" :: value :: tail =>
                            nextOption(map ++ Map("mode" -> value.toString), tail)
                        case "destination" :: value :: tail =>
                            nextOption(map ++ Map("destination" -> value.toString), tail)
                        case "sort" :: value :: tail =>
                            nextOption(map ++ Map("sort" -> value.toString), tail)
                        case "verbose" :: tail =>
                            nextOption(map ++ Map("verbose" -> ""), tail)
                        case option :: tail =>
                            throw new IllegalArgumentException(s"Bad option ${option}, or missing arguments")
                    }
            }
        }
        val options = nextOption(Map(),arglist)
        options
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
        val humanReadable = yo.prettyPrint
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
        val lines: String = WdlPrettyPrinter(true, Some(cState.outputs))
            .apply(rewrittenNs, 0)
            .mkString("\n")
        val cleanNs = WdlNamespace.loadUsingSource(
            lines, None, Some(List(cState.resolver))
        ).get
        if (cState.verbose)
            writeToFile(cState.wdlSourceFile, "." + suffix, lines)
        cleanNs
    }

    def compileBody(wdlSourceFile : Path, options: OptionsMap) : String = {
        val verbose = options contains "verbose"
        val force = options contains "force"
        val archive = options contains "archive"

        // There are three possible syntaxes:
        //    project-id:/folder
        //    project-id:
        //    /folder
        val (project, folder) = options.get("destination") match {
            case None => (None, "/")
            case Some(d) if d contains ":" =>
                val vec = d.split(":")
                vec.length match {
                    case 1 if (d.endsWith(":")) =>
                        (Some(vec(0)), "/")
                    case 2 => (Some(vec(0)), vec(1))
                    case _ => throw new Exception(s"Invalid path syntex <${d}>")
                }
            case Some(d) if d.startsWith("/") =>
                (None, d)
            case Some(d) => throw new Exception(s"Invalid path syntex <${d}>")
        }
        if (folder.isEmpty)
            throw new Exception(s"destination cannot specify empty folder")

        val dxProject : DXProject = project match {
            case None =>
                // get the default project
                val dxEnv = com.dnanexus.DXEnvironment.create()
                dxEnv.getProjectContext()
            case Some(p) => DXProject.getInstance(p)
        }
        val dxWDLrtId = getAssetId()
        val mode: Option[String] = options.get("mode")
        val sortMode = options.get("sort") match {
            case None => CompilerTopologicalSort.Mode.Check
            case Some("relaxed") => CompilerTopologicalSort.Mode.SortRelaxed
            case Some(_) => CompilerTopologicalSort.Mode.Sort
        }

        // get list of available instance types
        val instanceTypeDB = InstanceTypeDB.query(dxProject)

        // Resolving imports. Look for referenced files in the
        // source directory.
        def resolver(filename: String) : WdlSource = {
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
        val cState = State(orgNs, outputs(orgNs), wdlSourceFile, resolver, verbose)

        // Topologically sort the WDL file so no forward references exist in
        // subsequent steps. Create new file to hold the result.
        //
        // Additionally perform check for cycles in the workflow
        // Assuming the source file is xxx.wdl, the new name will
        // be xxx.sorted.wdl.
        val sortedNs1 = CompilerTopologicalSort.apply(orgNs, sortMode, verbose)
        val sortedNs = washNamespace(sortedNs1, "sorted", cState)

        // Simplify the original workflow, for example,
        // convert call arguments from expressions to variables.
        val ns1 = CompilerSimplify.apply(sortedNs, verbose)
        val ns = washNamespace(ns1, "simplified", cState)

        // Compile the WDL workflow into an Intermediate Representation (IR)
        val cef = new CompilerErrorFormatter(ns.terminalMap)
        val irNs = CompilerIR.apply(ns, instanceTypeDB, folder, cef, verbose)

        // Write out the intermediate representation
        prettyPrintIR(wdlSourceFile, irNs, verbose)

        // Backend compiler pass
        mode match {
            case None =>
                // Generate dx:applets and dx:workflow from the IR
                val wdlInputs = options.get("inputFile").map(Paths.get(_))
                CompilerNative.apply(irNs, wdlInputs, dxProject, instanceTypeDB,
                                     dxWDLrtId,
                                     folder, cef, force, archive, verbose)
            case Some(x) if x.toLowerCase == "fe" => "applet-xxxx"
            case _ => throw new Exception(s"Unknown mode ${mode}")
        }
    }

    def compile(args: Seq[String]): Termination = {
        try {
            val wdlSourceFile = args.head
            val options = parseCmdlineOptions(args.tail.toList)
            val dxc = compileBody(Paths.get(wdlSourceFile), options)
            SuccessfulTermination(dxc)
        } catch {
            case e : Throwable =>
                UnsuccessfulTermination(Utils.exceptionToString(e))
        }
    }

    // Extract the only task from a namespace
    def taskOfNamespace(ns: WdlNamespace) : Task = {
        val numTasks = ns.tasks.length
        if (numTasks != 1)
            throw new Exception(s"WDL file contains ${numTasks} tasks, instead of 1")
        ns.tasks.head
    }

    def workflowOfNamespace(ns: WdlNamespace): Workflow = {
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
        try {
            op match {
                case InternalOp.Eval =>
                    RunnerEval.apply(workflowOfNamespace(ns), jobInputPath, jobOutputPath, jobInfoPath)
                case InternalOp.LaunchScatter =>
                    RunnerScatter.apply(workflowOfNamespace(ns), jobInputPath, jobOutputPath, jobInfoPath)
                case InternalOp.TaskEpilog =>
                    RunnerTask.epilog(taskOfNamespace(ns), jobInputPath, jobOutputPath, jobInfoPath)
                case InternalOp.TaskProlog =>
                    RunnerTask.prolog(taskOfNamespace(ns), jobInputPath, jobOutputPath, jobInfoPath)
                case InternalOp.TaskRelaunch =>
                    RunnerTask.relaunch(taskOfNamespace(ns), jobInputPath, jobOutputPath, jobInfoPath)
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
                case Actions.Internal => internalOp(args.tail)
                case Actions.Version => SuccessfulTermination(getVersion())
            }
        }
    }

    val UsageMessage =
        """|java -jar dxWDL.jar <action> <parameters> [options]
           |
           |Actions:
           |
           |compile <WDL file>
           |  Compile a wdl file into a dnanexus workflow.
           |  Optionally, specify a destination path on the
           |  platform. A dx JSON inputs file is generated from the
           |  WDL inputs file, if specified.
           |  options:
           |    -archive :            Archive older versions of applets
           |    -destination <path> : Output folder for workflow
           |    -force :              Delete existing applets/workflows
           |    -inputs <file> :      Cromwell style input file
           |    -mode <mode> :        Compilation mode, a debugging flag
           |    -sort <mode> :        Sort call graph, to avoid forward references
           |    -verbose :            Print detailed progress reports
           |
           |config
           |  Print the configuration options
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
            Console.err.println(UsageMessage)
            System.exit(1)
        case BadUsageTermination(s) =>
            Console.err.println(s)
            System.exit(1)
        case UnsuccessfulTermination(s) =>
            Console.err.println(s)
            System.exit(1)
    }
}
