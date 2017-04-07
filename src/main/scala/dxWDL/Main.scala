package dxWDL

import com.dnanexus.{DXApplet, DXProject, DXUtil, DXContainer, DXSearch, DXWorkflow}
import java.io.{File, FileWriter, PrintWriter}
import java.nio.file.{Path, Paths, Files}
import scala.util.{Failure, Success, Try}
import spray.json._
import spray.json.DefaultJsonProtocol
import spray.json.JsString
import wdl4s.{WdlNamespace, WdlNamespaceWithWorkflow, Workflow}

object Main extends App {
    sealed trait Termination
    case class SuccessfulTermination(output: String) extends Termination
    case class UnsuccessfulTermination(output: String) extends Termination
    case class BadUsageTermination(info: String) extends Termination

    object Actions extends Enumeration {
        val AppletEpilog, AppletProlog, Compile, LaunchScatter,
            Version, WorkflowCommon, Yaml  = Value
    }

    def yaml(args: Seq[String]): Termination = {
        continueIf(args.length == 1) {
            loadWdl(args.head) { ns =>
                SuccessfulTermination(WdlYamlTree(ns).print())
            }
        }
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

    // Add a suffix to a filename, before the regular suffix. For example:
    //  xxx.wdl -> xxx.simplified.wdl
    def replaceFileSuffix(src: Path, suffix: String) : String = {
        val fName = src.toFile().getName()
        val index = fName.lastIndexOf('.')
        if (index == -1) {
            fName + suffix
        }
        else {
            val prefix = fName.substring(0, index)
            prefix + suffix
        }
    }

    def prettyPrintIr(wdlSourceFile : Path,
                      irWf: IR.Workflow) : Unit = {
        val trgName: String = replaceFileSuffix(wdlSourceFile, ".ir")
        val trgPath = Utils.appCompileDirPath.resolve(trgName).toFile

        val humanReadable: String = IR.yaml(irWf).print()
        val fos = new FileWriter(trgPath)
        val pw = new PrintWriter(fos)
        pw.print(humanReadable)
        pw.flush()
        pw.close()
    }

    def compileBody(wdlSourceFile : Path,
                    options: Map[String, String]) : String = {
        // verify version ID
        options.get("expectedVersion") match {
            case Some(vid) if vid != Utils.VERSION =>
                throw new Exception(s"""|Version mismatch, library is ${Utils.VERSION},
                                        |expected version is ${vid}"""
                                        .stripMargin.replaceAll("\n", " "))
            case _ => ()
        }

        val verbose = options.get("verbose") match {
            case None => false
            case Some(_) => true
        }

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

        // Simplify the source file
        // Create a new file to hold the result.
        //
        // Assuming the source file is xxx.wdl, the new name will
        // be xxx.simplified.wdl.
        val simplWdlPath = CompilerPreprocess.apply(wdlSourceFile, verbose)

        // extract the workflow
        val ns = WdlNamespaceWithWorkflow.load(Utils.readFileContent(simplWdlPath),
                                               Seq.empty).get
        val wf = ns match {
            case nswf : WdlNamespaceWithWorkflow => nswf.workflow
            case _ => throw new Exception("WDL does not have a workflow")
        }

        // remove old workflow and applets
        val oldWf = DXSearch.findDataObjects().nameMatchesExactly(wf.unqualifiedName)
            .inFolder(dxProject, folder).withClassWorkflow().execute().asList()
        dxProject.removeObjects(oldWf)
        val oldApplets = DXSearch.findDataObjects().nameMatchesGlob(wf.unqualifiedName + ".*")
            .inFolder(dxProject, folder).withClassWorkflow().execute().asList()
        dxProject.removeObjects(oldApplets)

        // Backbone of compilation process.
        // 1) Compile the WDL workflow into an Intermediate Representation (IR)
        // 2) Generate dx:applets and dx:workflow from the IR
        val cef = new CompilerErrorFormatter(wf.wdlSyntaxErrorFormatter.terminalMap)
        val irWf = CompilerFrontEnd.apply(ns, folder, cef, verbose)

        // Write out the intermediate representation
        prettyPrintIr(wdlSourceFile, irWf)

        mode match {
            case None =>
                val dxwfl = CompilerBackend.apply(irWf, dxProject, dxWDLrtId, simplWdlPath,
                                                  folder, cef, verbose)
                dxwfl.getId()
            case Some(x) =>
                x.toLowerCase match {
                    case "ir" | "fe" => "workflow-xxxx"
                    case _ => throw new Exception(s"Unknown mode ${mode}")
                }
        }
    }

    def compile(args: Seq[String]): Termination = {
        try {
            val wdlSrcFile = args.head

            // parse extra command line arguments
            val arglist = args.tail.toList
            type OptionMap = Map[String, String]

            def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
                list match {
                    case Nil => map
                    case "-o" :: value :: tail =>
                        nextOption(map ++ Map("destination" -> value.toString), tail)
                    case "-asset" :: value :: tail =>
                        nextOption(map ++ Map("dxWDLrtId" -> value.toString), tail)
                    case "-expected_version" :: value :: tail =>
                        nextOption(map ++ Map("expectedVersion" -> value.toString), tail)
                    case "-mode" :: value :: tail =>
                        nextOption(map ++ Map("mode" -> value.toString), tail)
                    case "-verbose" :: tail =>
                        nextOption(map ++ Map("verbose" -> ""), tail)
                    case option :: tail =>
                        throw new IllegalArgumentException(s"Unknown option ${option}")

                }
            }
            val options = nextOption(Map(),arglist)
            val dxc = compileBody(Paths.get(wdlSrcFile), options)
            SuccessfulTermination(dxc)
        } catch {
            case e : Throwable =>

                BadUsageTermination(Utils.exceptionToString(e))
        }
    }

    def appletAction(action: Actions.Value, args : Seq[String]): Termination = {
        if (args.length != 2) {
            BadUsageTermination("All applet actions take a WDL file, and a home directory")
        } else {
            val wdlDefPath = args(0)
            val homeDir = Paths.get(args(1))
            val (jobInputPath, jobOutputPath, jobErrorPath, jobInfoPath) = Utils.jobFilesOfHomeDir(homeDir)

            val wdlSource : String = Utils.readFileContent(Paths.get(wdlDefPath))
            val nswf : WdlNamespaceWithWorkflow =
                WdlNamespaceWithWorkflow.load(wdlSource, Seq.empty).get
            val wf : Workflow = nswf.workflow

            try {
                action match {
                    case Actions.AppletProlog =>
                        AppletRunner.prolog(wf, jobInputPath, jobOutputPath, jobInfoPath)
                    case Actions.AppletEpilog =>
                        AppletRunner.epilog(wf, jobInputPath, jobOutputPath, jobInfoPath)
                    case Actions.LaunchScatter =>
                        ScatterRunner.apply(wf, jobInputPath, jobOutputPath, jobInfoPath)
                    case Actions.WorkflowCommon =>
                        WorkflowCommonRunner.apply(wf, jobInputPath, jobOutputPath, jobInfoPath)
                }
                SuccessfulTermination(s"success ${action}")
            } catch {
                case e : Throwable =>
                    writeJobError(jobErrorPath, e)
                    UnsuccessfulTermination(s"failure running ${action}")
            }
        }
    }

    private[this] def continueIf(valid: => Boolean)(block: => Termination): Termination = if (valid) block else BadUsageTermination("")

    private[this] def loadWdl(path: String)(f: WdlNamespace => Termination): Termination = {
        try {
            val wdlSource : String = Utils.readFileContent(Paths.get(path))
            val nswf : WdlNamespaceWithWorkflow =
                WdlNamespaceWithWorkflow.load(wdlSource, Seq.empty).get
            f(nswf)
        } catch {
            case e : Throwable =>
                UnsuccessfulTermination(Utils.exceptionToString(e))
        }
    }

    private def getAction(args: Seq[String]): Option[Actions.Value] = for {
        arg <- args.headOption
        argCapitalized = arg.capitalize
        action <- Actions.values find (_.toString == argCapitalized)
    } yield action

    def dispatchCommand(args: Seq[String]): Termination = {
        getAction(args) match {
            case Some(x) if x == Actions.AppletProlog => appletAction(x, args.tail)
            case Some(x) if x == Actions.AppletEpilog => appletAction(x, args.tail)
            case Some(x) if x == Actions.Compile => compile(args.tail)
            case Some(x) if x == Actions.LaunchScatter => appletAction(x, args.tail)
            case Some(x) if x == Actions.WorkflowCommon => appletAction(x, args.tail)
            case Some(x) if x == Actions.Version => SuccessfulTermination(Utils.VERSION)
            case Some(x) if x == Actions.Yaml => yaml(args.tail)
            case _ => BadUsageTermination("")
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
           |compile <WDL file> <-asset dxId> [-o targetPath] [-expected_version vid] [-verbose]
           |       [-mode debug_flag]
           |
           |  Compile a wdl file into a dnanexus workflow. An asset
           |  ID for the dxWDL runtime is required. Optionally, specify a
           |  destination path on the platform.
           |
           |appletProlog <WDL file> <home directory>
           |
           |  Run the initial part of a dx-applet
           |  originally compiled from a WDL workflow.
           |  Process the input arguments, and generate a bash script.
           |
           |appletEpilog <WDL file> <home directory>
           |
           |  After the bash script generated by the above command
           |  is done, collect the outputs and format them into
           |  WDL and dx.
           |
           |launchScatter <WDL file> <home directory>
           |
           |  Launch a WDL scatter compiled into a dx-applet
           |
           |workflowCommon <WDL file> <home directory>
           |
           |  Perform the toplevel declarations at the beginning of the workflow
           |
           |version
           |
           |  Report the current version
           |""".stripMargin

    val termination = dispatchCommand(args)

    termination match {
        case SuccessfulTermination(s) => println(s)
        case BadUsageTermination(s) if (s == "") => Console.err.println(UsageMessage)
        case BadUsageTermination(s) => Console.err.println(s)
        case UnsuccessfulTermination(s) =>
            Console.err.println(s)
            System.exit(1)
    }
}
