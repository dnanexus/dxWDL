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
import wdl4s.{Task, WdlNamespace, WdlNamespaceWithWorkflow, Workflow}

object Main extends App {
    sealed trait Termination
    case class SuccessfulTermination(output: String) extends Termination
    case class UnsuccessfulTermination(output: String) extends Termination
    case class BadUsageTermination(info: String) extends Termination

    type OptionsMap = Map[String, String]

    object Actions extends Enumeration {
        val Compile, Eval, LaunchScatter,
            TaskEpilog, TaskProlog, TaskRelaunch,
            Version, WorkflowName, Yaml  = Value
    }

    // load configuration information
    def getVersion() : String = {
        val config = ConfigFactory.load()
        val version = config.getString("dxWDL.version")
        //System.err.println(s"version=${version}")
        //config
        version
    }

    // parse extra command line arguments
    def parseCmdlineOptions(arglist: List[String]) : OptionsMap = {
        // verify version ID
        def verifyVersion(options: OptionsMap) = {
            val version = getVersion()
            options.get("expectedVersion") match {
                case Some(vid) if vid != version =>
                    throw new Exception(s"""|Version mismatch, library is ${version},
                                            |expected version is ${vid}"""
                                            .stripMargin.replaceAll("\n", " "))
                case _ => ()
            }
        }
        def nextOption(map : OptionsMap, list: List[String]) : OptionsMap = {
            list match {
                case Nil => map
                case "-asset" :: value :: tail =>
                    nextOption(map ++ Map("dxWDLrtId" -> value.toString), tail)
                case "-expected_version" :: value :: tail =>
                    nextOption(map ++ Map("expectedVersion" -> value.toString), tail)
                case "-force" :: tail =>
                    nextOption(map ++ Map("force" -> ""), tail)
                case "-inputFile" :: value :: tail =>
                    nextOption(map ++ Map("inputFile" -> value.toString), tail)
                case "-mode" :: value :: tail =>
                    nextOption(map ++ Map("mode" -> value.toString), tail)
                case "-o" :: value :: tail =>
                    nextOption(map ++ Map("destination" -> value.toString), tail)
                case "-verbose" :: tail =>
                    nextOption(map ++ Map("verbose" -> ""), tail)
                case option :: tail =>
                    throw new IllegalArgumentException(s"Unknown option ${option}")

            }
        }

        val options = nextOption(Map(),arglist)
        verifyVersion(options)
        options
    }

    def yaml(args: Seq[String]): Termination = {
        if (args.length != 1)
            return BadUsageTermination("")

        val wdlSourceFile = Paths.get(args.head)

        // Resolving imports. Look for referenced files in the
        // source directory.
        val sourceDir = wdlSourceFile.getParent()
        def resolver(filename: String) : String = {
            Utils.readFileContent(sourceDir.resolve(filename))
        }
        val ns = WdlNamespace.loadUsingPath(wdlSourceFile, None, Some(List(resolver))).get
        SuccessfulTermination(WdlYamlTree(ns).print())
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


    def prettyPrintWorkflowIR(wdlSourceFile : Path,
                              irWf: IR.Workflow,
                              verbose: Boolean) : Unit = {
        val trgName: String = Utils.replaceFileSuffix(wdlSourceFile, ".ir.yaml")
        val trgPath = Utils.appCompileDirPath.resolve(trgName).toFile
        val yo = IR.yaml(irWf)
        val humanReadable = yo.prettyPrint
        val fos = new FileWriter(trgPath)
        val pw = new PrintWriter(fos)
        pw.print(humanReadable)
        pw.flush()
        pw.close()
        Utils.trace(verbose, s"Wrote intermediate representation to ${trgPath.toString}")
    }

    def prettyPrintAppletsIR(wdlSourceFile : Path,
                             irApplets: Vector[IR.Applet],
                             verbose: Boolean) : Unit = {
        val trgName: String = Utils.replaceFileSuffix(wdlSourceFile, ".ir.yaml")
        val trgPath = Utils.appCompileDirPath.resolve(trgName).toFile
        val humanReadable = irApplets.map(irTs =>
            IR.yaml(irTs).prettyPrint
        ).mkString("\n\n")
        val fos = new FileWriter(trgPath)
        val pw = new PrintWriter(fos)
        pw.print(humanReadable)
        pw.flush()
        pw.close()
        Utils.trace(verbose, s"Wrote intermediate representation to ${trgPath.toString}")
    }

    def compileBody(wdlSourceFile : Path, options: OptionsMap) : String = {
        val verbose = options contains "verbose"
        val force = options contains "force"

        // deal with the various options
        val destination : String = options.get("destination") match {
            case None => ""
            case Some(d) => d
        }

        // There are three possible syntaxes:
        //    project-id:/folder
        //    project-id:
        //    /folder
        val vec = destination.split(":")
        val (project, folder) = vec.length match {
            case 0 => (None, "/")
            case 1 =>
                if (destination.endsWith(":"))
                    (Some(vec(0)), "/")
                else
                    (None, vec(0))
            case 2 => (Some(vec(0)), vec(1))
            case _ => throw new Exception(s"Invalid path syntex ${destination}")
        }
        val dxProject : DXProject = project match {
            case None =>
                // get the default project
                val dxEnv = com.dnanexus.DXEnvironment.create()
                dxEnv.getProjectContext()
            case Some(p) => DXProject.getInstance(p)
        }
        val dxWDLrtId: String = options.get("dxWDLrtId") match {
            case None => throw new Exception("dxWDLrt asset ID not specified")
            case Some(id) => id
        }
        val mode: Option[String] = options.get("mode")

        // get list of available instance types
        val instanceTypeDB = InstanceTypeDB.queryWithBackup(dxProject)

        // Simplify the source file
        val ns:WdlNamespace = CompilerPreprocess.apply(wdlSourceFile, verbose)

        // Backbone of compilation process.
        // 1) Compile the WDL workflow into an Intermediate Representation (IR)
        // 2) Generate dx:applets and dx:workflow from the IR
        val cef = new CompilerErrorFormatter(ns.terminalMap)
        val (irWf, irApplets) = CompilerFrontEnd.apply(ns, instanceTypeDB, folder, cef, verbose)

        // Backend compiler pass
        irWf match {
            case None =>
                // We have only tasks
                // Write out the intermediate representation
                prettyPrintAppletsIR(wdlSourceFile, irApplets, verbose)
                mode match {
                    case None =>
                        val dxApplets = irApplets.map(x =>
                            CompilerBackend.apply(x, dxProject, instanceTypeDB, dxWDLrtId,
                                                  folder, cef, force, verbose)
                        )
                        val ids: Seq[String] = dxApplets.map(x => x.getId())
                        ids.mkString(", ")
                    case Some(x) if x.toLowerCase == "fe" => "applet-xxxx"
                    case _ => throw new Exception(s"Unknown mode ${mode}")
                }

            case Some(iRepWf) =>
                // Write out the intermediate representation
                prettyPrintWorkflowIR(wdlSourceFile, iRepWf, verbose)

                mode match {
                    case None =>
                        val (dxwfl,stageDict, callDict) =
                            CompilerBackend.apply(iRepWf, dxProject, instanceTypeDB,
                                                  dxWDLrtId,
                                                  folder, cef, force, verbose)
                        options.get("inputFile") match {
                            case None => ()
                            case Some(wdlInputFile) =>
                                InputFile.apply(iRepWf, stageDict, callDict,
                                                Paths.get(wdlInputFile), verbose)
                        }
                        dxwfl.getId()
                    case Some(x) if x.toLowerCase == "fe" => "workflow-xxxx"
                    case _ => throw new Exception(s"Unknown mode ${mode}")
                }
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

    private def appletAction(action: Actions.Value, args : Seq[String]): Termination = {
        if (args.length != 2) {
            BadUsageTermination("All applet actions take a WDL file, and a home directory")
        } else {
            val wdlDefPath = args(0)
            val homeDir = Paths.get(args(1))
            val (jobInputPath, jobOutputPath, jobErrorPath, jobInfoPath) =
                Utils.jobFilesOfHomeDir(homeDir)
            val ns = WdlNamespace.loadUsingPath(Paths.get(wdlDefPath), None, None).get

            try {
                action match {
                    case Actions.Eval =>
                        RunnerEval.apply(workflowOfNamespace(ns), jobInputPath, jobOutputPath, jobInfoPath)
                    case Actions.LaunchScatter =>
                        RunnerScatter.apply(workflowOfNamespace(ns), jobInputPath, jobOutputPath, jobInfoPath)
                    case Actions.TaskEpilog =>
                        RunnerTask.epilog(taskOfNamespace(ns), jobInputPath, jobOutputPath, jobInfoPath)
                    case Actions.TaskProlog =>
                        RunnerTask.prolog(taskOfNamespace(ns), jobInputPath, jobOutputPath, jobInfoPath)
                    case Actions.TaskRelaunch =>
                        RunnerTask.relaunch(taskOfNamespace(ns), jobInputPath, jobOutputPath, jobInfoPath)
                }
                SuccessfulTermination(s"success ${action}")
            } catch {
                case e : Throwable =>
                    writeJobError(jobErrorPath, e)
                    UnsuccessfulTermination(s"failure running ${action}")
            }
        }
    }

    // Parse a WDL file, and get the workflow name, if it exists
    private def getWorkflowName(args : Seq[String]): Termination = {
        if (args.length != 1) {
            BadUsageTermination("WorkflowName requires a single argument")
        } else {
            val wdlDefPath = args(0)
            val ns = WdlNamespace.loadUsingPath(Paths.get(wdlDefPath), None, None).get
            val wfName = ns match {
                case nswf : WdlNamespaceWithWorkflow => nswf.workflow.unqualifiedName
                case _ => throw new Exception("WDL file contains no workflow")
            }
            SuccessfulTermination(wfName)
        }
    }

    private def getAction(req: String): Option[Actions.Value] = {
        def normalize(s: String) : String= {
            s.replaceAll("_", "").toUpperCase
        }
        Actions.values find (x => normalize(x.toString) == normalize(req))
    }

    def dispatchCommand(args: Seq[String]): Termination = {
        if (args.isEmpty)
            BadUsageTermination("")
        else getAction(args.head) match {
            case None => BadUsageTermination("")
            case Some(x) => x match {
                case Actions.Compile => compile(args.tail)
                case Actions.Eval => appletAction(x, args.tail)
                case Actions.LaunchScatter => appletAction(x, args.tail)
                case Actions.TaskProlog => appletAction(x, args.tail)
                case Actions.TaskEpilog => appletAction(x, args.tail)
                case Actions.TaskRelaunch => appletAction(x, args.tail)
                case Actions.Version => SuccessfulTermination(getVersion())
                case Actions.WorkflowName => getWorkflowName(args.tail)
                case Actions.Yaml => yaml(args.tail)
            }
        }
    }

    val UsageMessage =
        """|java -jar dxWDL.jar <action> <parameters>
           |
           |Actions:
           |
           |yaml <WDL file>
           |
           |  Perform full validation and print a YAML version of the
           |  syntax tree.
           |
           |compile <WDL file> <-asset dxId> [-o targetPath] [-expected_version vid]
           |  [-inputFile wdlInputFile] [-verbose] [-force] [-mode debug_flag]
           |
           |  Compile a wdl file into a dnanexus workflow. An asset
           |  ID for the dxWDL runtime is required. Optionally, specify a
           |  destination path on the platform. A dx JSON inputs file is generated
           |  from the WDL inputs file, if specified.
           |
           |workflowName <WDL file>
           |
           |  Parse and extract the workflow name from a WDL file
           |
           |taskProlog <WDL file> <home directory>
           |
           |  Run the initial part of a dx-applet
           |  originally compiled from a WDL workflow.
           |  Process the input arguments, and generate a bash script.
           |
           |taskEpilog <WDL file> <home directory>
           |
           |  After the bash script generated by the above command
           |  is done, collect the outputs and format them into
           |  WDL and dx.
           |
           |launchScatter <WDL file> <home directory>
           |
           |  Launch a WDL scatter compiled into a dx-applet
           |
           |eval <WDL file> <home directory>
           |
           |  Run an applet that calculates WDL expressions
           |
           |version
           |
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
