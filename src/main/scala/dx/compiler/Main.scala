package dx.compiler

import java.nio.file.{Path, Paths}

import com.typesafe.config.ConfigFactory
import dx.InvalidInputException
import dx.api.{DxApi, DxProject}
import dx.core.io.DxPathConfig
import dx.core.languages.Language
import dx.core.getVersion
import dx.core.util.MainUtils._
import spray.json._
import wdlTools.util.{Logger, TraceLevel, Util}

object Main {
  private val DEFAULT_RUNTIME_TRACE_LEVEL: Int = TraceLevel.Verbose

  case class SuccessTree(pretty: Either[String, JsValue]) extends SuccessfulTermination {
    lazy val message: String = {
      pretty match {
        case Left(str)                 => str
        case Right(js) if js != JsNull => js.prettyPrint
      }
    }
  }
  case class SuccessIR(bundle: IR.Bundle,
                       override val message: String = "Intermediate representation")
      extends SuccessfulTermination

  object CompilerAction extends Enumeration {
    type CompilerAction = Value
    val Compile, Config, DXNI, Version, Describe = Value
  }

  case class DxniBaseOptions(force: Boolean,
                             outputFile: Path,
                             language: Language.Value,
                             dxApi: DxApi)

  case class DxniAppletOptions(apps: Boolean,
                               force: Boolean,
                               outputFile: Path,
                               recursive: Boolean,
                               language: Language.Value,
                               dxProject: DxProject,
                               folderOrPath: Either[String, String],
                               dxApi: DxApi)

  private def createDxApi(options: OptionsMap): DxApi = {
    val verboseKeys: Set[String] = options.get("verboseKey") match {
      case None                 => Set.empty
      case Some(modulesToTrace) => modulesToTrace.toSet
    }
    val traceLevel: Int = getTraceLevel(
        options.get("runtimeDebugLevel"),
        if (options.contains("verbose")) TraceLevel.Verbose else TraceLevel.None
    )
    val logger = Logger(quiet = options.contains("quiet"), traceLevel = traceLevel, verboseKeys)
    DxApi(logger)
  }

  private def parseExecTree(execTreeTypeAsString: String): TreePrinter = {
    execTreeTypeAsString.toLowerCase match {
      case "json"   => JsonTreePrinter
      case "pretty" => PrettyTreePrinter
      case _ =>
        throw new Exception(
            s"--execTree must be either json or pretty, found $execTreeTypeAsString"
        )
    }
  }

  private def pathOptions(options: OptionsMap, dxApi: DxApi): (DxProject, String) = {
    var folderOpt: Option[String] = options.get("folder") match {
      case None            => None
      case Some(Vector(f)) => Some(f)
      case _               => throw new Exception("sanity")
    }
    var projectOpt: Option[String] = options.get("project") match {
      case None            => None
      case Some(Vector(p)) => Some(p)
      case _               => throw new Exception("sanity")
    }
    val destinationOpt: Option[String] = options.get("destination") match {
      case None            => None
      case Some(Vector(d)) => Some(d)
      case Some(other)     => throw new Exception(s"Invalid path syntex <${other}>")
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
          case 1 if d.endsWith(":") =>
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
      case (None, _)          => throw new Exception("project is unspecified")
      case (Some(p), None)    => (p, "/")
      case (Some(p), Some(d)) => (p, d)
    }

    if (folderRaw.isEmpty)
      throw new Exception(s"Cannot specify empty folder")
    if (!folderRaw.startsWith("/"))
      throw new Exception(s"Folder must start with '/'")
    val dxFolder = folderRaw
    val dxProject =
      try {
        dxApi.resolveProject(projectRaw)
      } catch {
        case e: Exception =>
          dxApi.logger.error(e.getMessage)
          throw new Exception(
              s"""|Could not find project ${projectRaw}, you probably need to be logged into
                  |the platform""".stripMargin
          )
      }
    dxApi.logger.trace(s"""|project ID: ${dxProject.id}
                           |folder: ${dxFolder}""".stripMargin)
    (dxProject, dxFolder)
  }

  // parse extra command line arguments
  private def parseCmdlineOptions(arglist: List[String]): OptionsMap = {
    def keywordValueIsVector = Set("inputs", "imports", "verboseKey")

    val cmdLineOpts = splitCmdLine(arglist)
    val opts = cmdLineOpts.foldLeft(Map.empty[String, List[String]]) {
      case (_, Nil) => throw new Exception("sanity: empty command line option")
      case (opts, keyOrg :: subargs) =>
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
          case "execTree" =>
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
          case "force" | "f" | "overwrite" =>
            checkNumberOfArguments(keyword, 0, subargs)
            ("force", "")
          case "help" =>
            checkNumberOfArguments(keyword, 0, subargs)
            (keyword, "")
          case "input" | "inputs" =>
            checkNumberOfArguments(keyword, 1, subargs)
            (keyword, subargs.head)
          case "imports" | "p" =>
            checkNumberOfArguments(keyword, 1, subargs)
            ("imports", subargs.head)
          case "language" =>
            checkNumberOfArguments(keyword, 1, subargs)
            (keyword, subargs.head)
          case "leaveWorkflowsOpen" =>
            checkNumberOfArguments(keyword, 0, subargs)
            (keyword, "")
          case "locked" =>
            checkNumberOfArguments(keyword, 0, subargs)
            (keyword, "")
          case "o" | "output" | "outputFile" =>
            checkNumberOfArguments(keyword, 1, subargs)
            ("outputFile", subargs.head)
          case "path" =>
            checkNumberOfArguments(keyword, 1, subargs)
            (keyword, subargs.head)
          case "project" =>
            checkNumberOfArguments(keyword, 1, subargs)
            (keyword, subargs.head)
          case "projectWideReuse" =>
            checkNumberOfArguments(keyword, 0, subargs)
            (keyword, "")
          case "q" | "quiet" =>
            checkNumberOfArguments(keyword, 0, subargs)
            ("quiet", "")
          case "r" | "recursive" =>
            checkNumberOfArguments(keyword, 0, subargs)
            ("recursive", "")
          case "reorg" =>
            checkNumberOfArguments(keyword, 0, subargs)
            (keyword, "")
          case "runtimeDebugLevel" =>
            checkNumberOfArguments(keyword, 1, subargs)
            (keyword, subargs.head)
          case "streamAllFiles" =>
            checkNumberOfArguments(keyword, 0, subargs)
            ("streamAllFiles", "")
          case "verbose" =>
            checkNumberOfArguments(keyword, 0, subargs)
            (keyword, "")
          case "verboseKey" =>
            checkNumberOfArguments(keyword, 1, subargs)
            (keyword, subargs.head)
          case _ =>
            throw new IllegalArgumentException(s"Unregonized keyword ${keyword}")
        }
        opts.get(nKeyword) match {
          case None =>
            // first time
            opts + (nKeyword -> List(value))
          case Some(x) if keywordValueIsVector contains nKeyword =>
            // append to the already existing verbose flags
            opts + (nKeyword -> (value :: x))
          case Some(_) =>
            // overwrite the previous flag value
            opts + (nKeyword -> List(value))
        }
    }
    opts.view.mapValues(_.toVector).toMap
  }

  def compile(args: Seq[String]): Termination = {
    if (args.isEmpty)
      return BadUsageTermination("WDL file to compile is missing")
    val sourceFile = Paths.get(args.head)
    val options =
      try {
        parseCmdlineOptions(args.tail.toList)
      } catch {
        case e: Throwable =>
          return BadUsageTermination(exception = Some(e))
      }
    if (options contains "help")
      return BadUsageTermination()

    try {
      val cOpt = compilerOptions(options)
      val top = Top(cOpt)
      val (dxProject, folder) = pathOptions(options, cOpt.dxApi)
      cOpt.compileMode match {
        case CompilerFlag.IR =>
          val ir: IR.Bundle = top.applyOnlyIR(sourceFile, dxProject)
          SuccessIR(ir)

        case CompilerFlag.All | CompilerFlag.NativeWithoutRuntimeAsset =>
          val dxPathConfig = DxPathConfig.apply(baseDNAxDir, cOpt.streamAllFiles, cOpt.dxApi.logger)
          val (retval, treeDesc) =
            top.apply(sourceFile, folder, dxProject, dxPathConfig, cOpt.execTree)
          treeDesc match {
            case None =>
              Success(retval)
            case Some(treePretty) =>
              SuccessTree(treePretty)
          }
      }
    } catch {
      case e: Throwable =>
        Failure(exception = Some(e))
    }
  }

  private def dxniApplets(dOpt: DxniAppletOptions): Termination = {
    try {
      DxNI.apply(dOpt.dxProject,
                 dOpt.folderOrPath,
                 dOpt.outputFile,
                 dOpt.recursive,
                 dOpt.force,
                 dOpt.language,
                 dOpt.dxApi)
      Success()
    } catch {
      case e: Throwable => Failure(exception = Some(e))
    }
  }

  private def dxniApps(dOpt: DxniBaseOptions): Termination = {
    try {
      DxNI.applyApps(dOpt.outputFile, dOpt.force, dOpt.language, dOpt.dxApi)
      Success()
    } catch {
      case e: Throwable => Failure(exception = Some(e))
    }
  }

  def dxni(args: Seq[String]): Termination = {
    try {
      val options = parseCmdlineOptions(args.toList)
      if (options contains "help")
        return BadUsageTermination()

      val apps = options contains "apps"
      if (apps) {
        // Search for global apps
        val dOpt = dxniAppOptions(options)
        dxniApps(dOpt)
      } else {
        // Search for applets inside or a folder, path, or project.
        val dOpt = dxniAppletOptions(options)
        dxniApplets(dOpt)
      }
    } catch {
      case e: Throwable => BadUsageTermination(exception = Some(e))
    }
  }

  private def parseDescribeOptions(arglist: List[String]): OptionsMap = {
    val describeOpts = splitCmdLine(arglist)
    describeOpts.foldLeft(Map.empty[String, Vector[String]]) {
      case (opts, Nil) => opts
      case (opts, keyOrg :: subargs) =>
        val keyword = normKeyword(keyOrg)
        val (nKeyword, value) = keyword match {
          case "pretty" =>
            checkNumberOfArguments(keyword, 0, subargs)
            (keyword, "")
          case "help" =>
            checkNumberOfArguments(keyword, 0, subargs)
            (keyword, "")
          case _ =>
            throw new IllegalArgumentException(s"Unregonized keyword ${keyword}")

        }
        opts + (nKeyword -> Vector(value))
    }
  }

  def describe(args: Seq[String]): Termination = {
    if (args.isEmpty) {
      return BadUsageTermination("Workflow ID is not provided")
    }

    val options =
      try {
        parseDescribeOptions(args.tail.toList)
      } catch {
        case e: Throwable =>
          return BadUsageTermination(exception = Some(e))
      }

    val dxApi = createDxApi(options)

    // validate workflow
    val workflowId = args.head
    val wf =
      try {
        dxApi.workflow(workflowId)
      } catch {
        case e: Throwable =>
          return BadUsageTermination(exception = Some(e))
      }

    if (options contains "help")
      return BadUsageTermination()

    val execTreeJS = Tree.formDXworkflow(wf)

    if (options contains "pretty") {
      val prettyTree = Tree.generateTreeFromJson(execTreeJS.asJsObject)
      SuccessTree(Left(prettyTree))
    } else {
      SuccessTree(Right(execTreeJS))
    }
  }

  // Get basic information about the dx environment, and process
  // the compiler flags
  private def compilerOptions(options: OptionsMap): CompilerOptions = {
    val dxApi = createDxApi(options)
    val compileMode: CompilerFlag.Value = options.get("compileMode") match {
      case None                                                 => CompilerFlag.All
      case Some(Vector(x)) if x.toLowerCase == "IR".toLowerCase => CompilerFlag.IR
      case Some(Vector(x)) if x.toLowerCase == "NativeWithoutRuntimeAsset".toLowerCase =>
        CompilerFlag.NativeWithoutRuntimeAsset
      case Some(other) => throw new Exception(s"unrecognized compiler flag ${other}")
    }
    val defaults: Option[Path] = options.get("defaults") match {
      case None            => None
      case Some(Vector(p)) => Some(Paths.get(p))
      case _               => throw new Exception("defaults specified twice")
    }
    val extras = options.get("extras") match {
      case None => None
      case Some(Vector(p)) =>
        val contents = Util.readFileContent(Paths.get(p))
        Some(Extras.parse(contents.parseJson, dxApi))
      case _ => throw new Exception("extras specified twice")
    }
    val inputs: Vector[Path] = options.get("inputs") match {
      case None     => Vector.empty
      case Some(pl) => pl.map(p => Paths.get(p))
    }
    val imports: Vector[Path] = options.get("imports") match {
      case None     => Vector.empty
      case Some(pl) => pl.map(p => Paths.get(p))
    }
    val treePrinter: Option[TreePrinter] = options.get("execTree") match {
      case None => None
      case Some(treeType) =>
        Some(parseExecTree(treeType.head)) // take first element and drop the rest?
    }
    val runtimeTraceLevel =
      getTraceLevel(options.get("runtimeDebugLevel"), DEFAULT_RUNTIME_TRACE_LEVEL)
    if (extras.isDefined) {
      if (extras.contains("reorg") && (options contains "reorg")) {
        throw new InvalidInputException(
            "ERROR: cannot provide --reorg option when reorg is specified in extras."
        )
      }
      if (extras.contains("reorg") && (options contains "locked")) {
        throw new InvalidInputException(
            "ERROR: cannot provide --locked option when reorg is specified in extras."
        )
      }
    }
    CompilerOptions(
        options contains "archive",
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
        options contains "streamAllFiles",
        // options contains "execTree",
        treePrinter,
        runtimeTraceLevel,
        dxApi
    )
  }

  private def dxniAppOptions(options: OptionsMap): DxniBaseOptions = {
    val outputFile: Path = options.get("outputFile") match {
      case None            => throw new Exception("output file not specified")
      case Some(Vector(p)) => Paths.get(p)
      case _               => throw new Exception("only one output file can be specified")
    }
    val dxApi = createDxApi(options)
    val language = options.get("language") match {
      case None => Language.WDLvDraft2
      case Some(Vector(buf)) =>
        val bufNorm = buf.toLowerCase
          .replaceAll("\\.", "")
          .replaceAll("_", "")
          .replaceAll("-", "")
        if (!bufNorm.startsWith("wdl"))
          throw new Exception(s"unknown language ${bufNorm}. Only WDL is supported")
        val suffix = bufNorm.substring("wdl".length)
        if (suffix contains "draft2")
          Language.WDLvDraft2
        else if (suffix contains "draft3")
          Language.WDLv1_0
        else if (suffix contains "10")
          Language.WDLv1_0
        else if (suffix contains "20")
          Language.WDLv2_0
        else if (suffix contains "development")
          Language.WDLv2_0
        else
          throw new Exception(s"unknown language ${bufNorm}. Supported: WDL_draft2, WDL_v1")
      case _ => throw new Exception("only one language can be specified")
    }
    DxniBaseOptions(options contains "force", outputFile, language, dxApi)
  }

  private def dxniAppletOptions(options: OptionsMap): DxniAppletOptions = {
    val dOpt = dxniAppOptions(options)
    val project: String = options.get("project") match {
      case None            => throw new Exception("no project specified")
      case Some(Vector(p)) => p
      case _               => throw new Exception("project specified multiple times")
    }
    val dxProject =
      try {
        dOpt.dxApi.resolveProject(project)
      } catch {
        case e: Exception =>
          dOpt.dxApi.logger.error(e.getMessage)
          throw new Exception(
              s"""|Could not find project ${project}, you probably need to be logged into
                  |the platform""".stripMargin
          )
      }
    val folder = options.get("folder") match {
      case None             => None
      case Some(Vector(fl)) =>
        // Validate the folder. It would have been nicer to be able
        // to check if a folder exists, instead of validating by
        // listing its contents, which could be very large.
        try {
          dxProject.listFolder(fl)
        } catch {
          case e: Throwable =>
            throw new Exception(s"err when validating folder ${fl} : ${e}")
        }
        Some(fl)
      case Some(_) => throw new Exception("folder specified multiple times")
    }
    val path: Option[String] = options.get("path") match {
      case None            => None
      case Some(Vector(p)) => Some(p)
      case Some(_)         => throw new Exception("path specified multiple times")
    }
    val folderOrPath: Either[String, String] = (folder, path) match {
      case (None, None)       => Left("/") // use the root folder as the default
      case (Some(fl), None)   => Left(fl)
      case (None, Some(p))    => Right(p)
      case (Some(_), Some(_)) => throw new Exception("both folder and path specified")
    }
    DxniAppletOptions(options contains "apps",
                      dOpt.force,
                      dOpt.outputFile,
                      options contains "recursive",
                      dOpt.language,
                      dxProject,
                      folderOrPath,
                      dOpt.dxApi)
  }

  def dispatchCommand(args: Seq[String]): Termination = {
    if (args.isEmpty) {
      return BadUsageTermination()
    }
    val action = CompilerAction.values.find(x => normKey(x.toString) == normKey(args.head))
    action match {
      case None => BadUsageTermination()
      case Some(x) =>
        x match {
          case CompilerAction.Compile  => compile(args.tail)
          case CompilerAction.Describe => describe(args.tail)
          case CompilerAction.Config   => Success(ConfigFactory.load().toString)
          case CompilerAction.DXNI     => dxni(args.tail)
          case CompilerAction.Version  => Success(getVersion)
        }
    }
  }

  private val usageMessage =
    s"""|java -jar dxWDL.jar <action> <parameters> [options]
        |
        |Actions:
        |  describe <DxWorkflow ID>
        |    Generate the execution tree as JSON for a given dnanexus workflow ID.
        |    Workflow needs to be have been previoulsy compiled by dxWDL.
        |    options
        |      -pretty                Print exec tree in pretty format
        |
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
        |      -execTree [json,pretty] Write out a json representation of the workflow
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
        |      -streamAllFiles        mount all files with dxfuse, do not use the download agent
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
        |      -language <string>     Which language to use? (wdl_draft2, wdl_v1.0)
        |
        |Common options
        |    -destination             Full platform path (project:/folder)
        |    -f | force               Delete existing applets/workflows
        |    -folder <string>         Platform folder
        |    -project <string>        Platform project
        |    -quiet                   Do not print warnings or informational outputs
        |    -verbose                 Print detailed progress reports
        |    -verboseKey [module]     Detailed information for a specific module
        |""".stripMargin

  def main(args: Seq[String]): Unit = {
    terminate(dispatchCommand(args), Some(usageMessage))
  }
}
