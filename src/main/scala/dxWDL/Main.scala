package dxWDL

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
                    case "-verbose" :: tail =>
                        nextOption(map ++ Map("verbose" -> ""), tail)
                    case option :: tail =>
                        throw new IllegalArgumentException(s"Unknown option ${option}")

                }
            }
            val options = nextOption(Map(),arglist)

            loadWdl(wdlSrcFile) { ns =>
                val dxc = Compile.apply(ns, Paths.get(wdlSrcFile), options)
                SuccessfulTermination(dxc)
            }
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
