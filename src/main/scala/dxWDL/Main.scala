package dxWDL

import com.typesafe.config._
import java.nio.file.{Path, Paths}

import spray.json.{JsString, _}
import dxWDL.base._
import dxWDL.compiler.Tree
import dxWDL.dx._
import dxWDL.util._

import scala.collection.mutable

object Main extends App {
  sealed trait Termination
  case class SuccessfulTermination(output: String) extends Termination
  case class SuccessfulTerminationTree(pretty: Either[String, JsValue]) extends Termination
  case class SuccessfulTerminationIR(bundle: dxWDL.compiler.IR.Bundle) extends Termination
  case class UnsuccessfulTermination(output: String) extends Termination
  case class BadUsageTermination(info: String) extends Termination

  type OptionsMap = Map[String, List[String]]

  object Actions extends Enumeration {
    val Compile, Config, DXNI, Internal, Version, Describe = Value
  }
  object InternalOp extends Enumeration {
    val Collect, WfOutputs, WfInputs, WorkflowOutputReorg, WfCustomReorgOutputs, WfFragment,
        TaskCheckInstanceType, TaskProlog, TaskInstantiateCommand, TaskEpilog, TaskRelaunch = Value
  }

  case class DxniBaseOptions(force: Boolean,
                             outputFile: Path,
                             language: Language.Value,
                             verbose: Verbose)

  case class DxniAppletOptions(apps: Boolean,
                               force: Boolean,
                               outputFile: Path,
                               recursive: Boolean,
                               language: Language.Value,
                               dxProject: DxProject,
                               folderOrPath: Either[String, String],
                               verbose: Verbose)

  // This directory exists only at runtime in the cloud. Beware of using
  // it in code paths that run at compile time.
  private lazy val baseDNAxDir: Path = Paths.get("/home/dnanexus")

  // Setup the standard paths used for applets. These are used at
  // runtime, not at compile time. On the cloud instance running the
  // job, the user is "dnanexus", and the home directory is
  // "/home/dnanexus".
  private def buildRuntimePathConfig(streamAllFiles: Boolean, verbose: Boolean): DxPathConfig = {
    DxPathConfig.apply(baseDNAxDir, streamAllFiles, verbose)
  }

  private def normKey(s: String): String = {
    s.replaceAll("_", "").toUpperCase
  }

  // Split arguments into sub-lists, one per each option.
  // For example:
  //    --sort relaxed --reorg --compile-mode IR
  // =>
  //    [[--sort, relaxed], [--reorg], [--compile-mode, IR]]
  //
  def splitCmdLine(arglist: List[String]): List[List[String]] = {
    def isKeyword(word: String): Boolean = word.startsWith("-")

    val keywordAndOptions: List[List[String]] = arglist.foldLeft(List.empty[List[String]]) {
      case (head :: tail, word) if isKeyword(word) =>
        List(word) :: head :: tail
      case (head :: tail, word) =>
        (word :: head) :: tail
      case (head, word) if isKeyword(word) =>
        List(word) :: head
      case (Nil, word) if isKeyword(word) => List(List(word))
      case (Nil, word) if !isKeyword(word) =>
        throw new Exception("Keyword must precede options")
    }
    keywordAndOptions.map(_.reverse).reverse
  }

  def normKeyword(word: String): String = {
    // normalize a keyword, remove leading dashes
    // letters to lowercase.
    //
    // "--Archive" -> "archive"
    // "--archive-only -> "archiveonly"
    word.replaceAll("-", "")
  }

  def checkNumberOfArguments(keyword: String, expectedNumArgs: Int, subargs: List[String]): Unit = {
    if (expectedNumArgs != subargs.length)
      throw new Exception(s"""|Wrong number of arguments for ${keyword}.
                              |Expected ${expectedNumArgs}, input is
                              |${subargs}""".stripMargin.replaceAll("\n", " "))
  }
  // parse extra command line arguments
  def parseCmdlineOptions(arglist: List[String]): OptionsMap = {
    def keywordValueIsList = Set("inputs", "imports", "verboseKey")

    val cmdLineOpts = splitCmdLine(arglist)
    val options = mutable.HashMap.empty[String, List[String]]
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
        options.get(nKeyword) match {
          case None =>
            // first time
            options(nKeyword) = List(value)
          case Some(x) if keywordValueIsList contains nKeyword =>
            // append to the already existing verbose flags
            options(nKeyword) = value :: x
          case Some(_) =>
            // overwrite the previous flag value
            options(nKeyword) = List(value)
        }
    }
    options.toMap
  }

  // Report an error, since this is called from a bash script, we
  // can't simply raise an exception. Instead, we write the error to
  // a standard JSON file.
  def writeJobError(jobErrorPath: Path, e: Throwable): Unit = {
    val errType = e match {
      case _: AppException         => "AppError"
      case _: AppInternalException => "AppInternalError"
      case _: Throwable            => "AppInternalError"
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

  private def pathOptions(options: OptionsMap, verbose: Verbose): (DxProject, String) = {
    var folderOpt: Option[String] = options.get("folder") match {
      case None          => None
      case Some(List(f)) => Some(f)
      case _             => throw new Exception("sanity")
    }
    var projectOpt: Option[String] = options.get("project") match {
      case None          => None
      case Some(List(p)) => Some(p)
      case _             => throw new Exception("sanity")
    }
    val destinationOpt: Option[String] = options.get("destination") match {
      case None          => None
      case Some(List(d)) => Some(d)
      case Some(other)   => throw new Exception(s"Invalid path syntex <${other}>")
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
        DxPath.resolveProject(projectRaw)
      } catch {
        case e: Exception =>
          Utils.error(e.getMessage)
          throw new Exception(
              s"""|Could not find project ${projectRaw}, you probably need to be logged into
                  |the platform""".stripMargin
          )
      }
    Utils.trace(verbose.on, s"""|project ID: ${dxProject.id}
                                |folder: ${dxFolder}""".stripMargin)
    (dxProject, dxFolder)
  }

  private def parseRuntimeDebugLevel(numberStr: String): Int = {
    val rtDebugLvl =
      try {
        numberStr.toInt
      } catch {
        case _: java.lang.NumberFormatException =>
          throw new Exception(
              s"""|the runtimeDebugLevel flag takes an integer input,
                  |${numberStr} is not of type int""".stripMargin
                .replaceAll("\n", " ")
          )
      }
    if (rtDebugLvl < 0 || rtDebugLvl > 2)
      throw new Exception(
          s"""|the runtimeDebugLevel flag must be one of {0, 1, 2}.
              |Value ${rtDebugLvl} is out of bounds.""".stripMargin
            .replaceAll("\n", " ")
      )
    rtDebugLvl
  }

  private def parseExecTree(execTreeTypeAsString: String): TreePrinter =
    execTreeTypeAsString.toLowerCase match {
      case "json"   => JsonTreePrinter
      case "pretty" => PrettyTreePrinter
      case _ =>
        throw new Exception(
            s"--execTree must be either json or pretty, found $execTreeTypeAsString"
        )
    }

  private def parseStreamAllFiles(s: String): Boolean = {
    s.toLowerCase match {
      case "true"  => true
      case "false" => false
      case other =>
        throw new Exception(
            s"""|the streamAllFiles flag must be a boolean (true,false).
                |Value ${other} is illegal.""".stripMargin
              .replaceAll("\n", " ")
        )
    }
  }

  // Get basic information about the dx environment, and process
  // the compiler flags
  private def compilerOptions(options: OptionsMap): CompilerOptions = {
    // First: get the verbosity mode. It is used almost everywhere.
    val verboseKeys: Set[String] = options.get("verboseKey") match {
      case None                 => Set.empty
      case Some(modulesToTrace) => modulesToTrace.toSet
    }
    val verbose = Verbose(options contains "verbose", options contains "quiet", verboseKeys)

    val compileMode: CompilerFlag.Value = options.get("compileMode") match {
      case None                                               => CompilerFlag.All
      case Some(List(x)) if x.toLowerCase == "IR".toLowerCase => CompilerFlag.IR
      case Some(List(x)) if x.toLowerCase == "NativeWithoutRuntimeAsset".toLowerCase =>
        CompilerFlag.NativeWithoutRuntimeAsset
      case Some(other) => throw new Exception(s"unrecognized compiler flag ${other}")
    }
    val defaults: Option[Path] = options.get("defaults") match {
      case None          => None
      case Some(List(p)) => Some(Paths.get(p))
      case _             => throw new Exception("defaults specified twice")
    }
    val extras = options.get("extras") match {
      case None => None
      case Some(List(p)) =>
        val contents = Utils.readFileContent(Paths.get(p))
        Some(Extras.parse(contents.parseJson, verbose))
      case _ => throw new Exception("extras specified twice")
    }
    val inputs: List[Path] = options.get("inputs") match {
      case None     => List.empty
      case Some(pl) => pl.map(p => Paths.get(p))
    }
    val imports: List[Path] = options.get("imports") match {
      case None     => List.empty
      case Some(pl) => pl.map(p => Paths.get(p))
    }

    val treePrinter: Option[TreePrinter] = options.get("execTree") match {
      case None => None
      case Some(treeType) =>
        Some(parseExecTree(treeType.head)) // take first element and drop the rest?
    }
    val runtimeDebugLevel: Option[Int] =
      options.get("runtimeDebugLevel") match {
        case None                  => None
        case Some(List(numberStr)) => Some(parseRuntimeDebugLevel(numberStr))
        case _                     => throw new Exception("debug level specified twice")
      }

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
        runtimeDebugLevel,
        verbose
    )
  }

  private def dxniAppOptions(options: OptionsMap): DxniBaseOptions = {
    val outputFile: Path = options.get("outputFile") match {
      case None          => throw new Exception("output file not specified")
      case Some(List(p)) => Paths.get(p)
      case _             => throw new Exception("only one output file can be specified")
    }
    val verboseKeys: Set[String] = options.get("verbose") match {
      case None                 => Set.empty
      case Some(modulesToTrace) => modulesToTrace.toSet
    }
    val verbose = Verbose(options contains "verbose", options contains "quiet", verboseKeys)
    val language = options.get("language") match {
      case None => Language.WDLvDraft2
      case Some(List(buf)) =>
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
    DxniBaseOptions(options contains "force", outputFile, language, verbose)
  }

  private def dxniAppletOptions(options: OptionsMap): DxniAppletOptions = {
    val dOpt = dxniAppOptions(options)
    val project: String = options.get("project") match {
      case None          => throw new Exception("no project specified")
      case Some(List(p)) => p
      case _             => throw new Exception("project specified multiple times")
    }
    val dxProject =
      try {
        DxPath.resolveProject(project)
      } catch {
        case e: Exception =>
          Utils.error(e.getMessage)
          throw new Exception(
              s"""|Could not find project ${project}, you probably need to be logged into
                  |the platform""".stripMargin
          )
      }
    val folder = options.get("folder") match {
      case None           => None
      case Some(List(fl)) =>
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
      case None          => None
      case Some(List(p)) => Some(p)
      case Some(_)       => throw new Exception("path specified multiple times")
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
                      dOpt.verbose)
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
          return BadUsageTermination(Utils.exceptionToString(e))
      }
    if (options contains "help")
      return BadUsageTermination("")

    try {
      val cOpt = compilerOptions(options)
      val top = compiler.Top(cOpt)
      val (dxProject, folder) = pathOptions(options, cOpt.verbose)
      cOpt.compileMode match {
        case CompilerFlag.IR =>
          val ir: compiler.IR.Bundle = top.applyOnlyIR(sourceFile, dxProject)
          SuccessfulTerminationIR(ir)

        case CompilerFlag.All | CompilerFlag.NativeWithoutRuntimeAsset =>
          val dxPathConfig = DxPathConfig.apply(baseDNAxDir, cOpt.streamAllFiles, cOpt.verbose.on)
          val (retval, treeDesc) =
            top.apply(sourceFile, folder, dxProject, dxPathConfig, cOpt.execTree)
          treeDesc match {
            case None =>
              SuccessfulTermination(retval)
            case Some(treePretty) =>
              SuccessfulTerminationTree(treePretty)
          }
      }
    } catch {
      case e: Throwable =>
        UnsuccessfulTermination(Utils.exceptionToString(e))
    }
  }

  def parseDescribeOptions(argList: List[String]): OptionsMap = {
    val describeOpts = splitCmdLine(argList)
    val options = mutable.HashMap.empty[String, List[String]]

    describeOpts.foreach {
      case Nil => Nil
      case keyOrg :: subargs =>
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
        options.get(nKeyword) match {
          case None =>
            // first time
            options(nKeyword) = List(value)
          case Some(_) =>
            // overwrite the previous flag value
            options(nKeyword) = List(value)
        }
    }
    options.toMap
  }

  def describe(args: Seq[String]): Termination = {
    if (args.isEmpty) {
      return BadUsageTermination("Workflow ID is not provided")
    }

    // validate workflow
    val workflowId = args.head
    val wf =
      try {
        DxWorkflow.getInstance(workflowId)
      } catch {
        case e: Throwable =>
          return BadUsageTermination(Utils.exceptionToString(e))
      }

    val options =
      try {
        parseDescribeOptions(args.tail.toList)
      } catch {
        case e: Throwable =>
          return BadUsageTermination(Utils.exceptionToString(e))
      }

    if (options contains "help")
      return BadUsageTermination("")

    val execTreeJS = Tree.formDXworkflow(wf)

    if (options contains "pretty") {
      val prettyTree = Tree.generateTreeFromJson(execTreeJS.asJsObject)
      SuccessfulTerminationTree(Left(prettyTree))
    } else {
      SuccessfulTerminationTree(Right(execTreeJS))
    }
  }

  private def dxniApplets(dOpt: DxniAppletOptions): Termination = {
    try {
      compiler.DxNI.apply(dOpt.dxProject,
                          dOpt.folderOrPath,
                          dOpt.outputFile,
                          dOpt.recursive,
                          dOpt.force,
                          dOpt.language,
                          dOpt.verbose)
      SuccessfulTermination("")
    } catch {
      case e: Throwable =>
        UnsuccessfulTermination(Utils.exceptionToString(e))
    }
  }

  private def dxniApps(dOpt: DxniBaseOptions): Termination = {
    try {
      compiler.DxNI.applyApps(dOpt.outputFile, dOpt.force, dOpt.language, dOpt.verbose)
      SuccessfulTermination("")
    } catch {
      case e: Throwable =>
        UnsuccessfulTermination(Utils.exceptionToString(e))
    }
  }

  def dxni(args: Seq[String]): Termination = {
    try {
      val options = parseCmdlineOptions(args.toList)
      if (options contains "help")
        return BadUsageTermination("")

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
      case e: Throwable =>
        BadUsageTermination(Utils.exceptionToString(e))
    }
  }

  private def taskAction(op: InternalOp.Value,
                         taskSourceCode: String,
                         instanceTypeDB: InstanceTypeDB,
                         jobInputPath: Path,
                         jobOutputPath: Path,
                         dxPathConfig: DxPathConfig,
                         dxIoFunctions: DxIoFunctions,
                         defaultRuntimeAttributes: Option[WdlRuntimeAttrs],
                         delayWorkspaceDestruction: Option[Boolean],
                         rtDebugLvl: Int): Termination = {
    // Parse the inputs, convert to WDL values. Delay downloading files
    // from the platform, we may not need to access them.
    val verbose = rtDebugLvl > 0
    val inputLines: String = Utils.readFileContent(jobInputPath)
    val originalInputs: JsValue = inputLines.parseJson

    val (task, typeAliases, document) = ParseWdlSourceFile(verbose).parseWdlTask(taskSourceCode)

    // setup the utility directories that the task-runner employs
    dxPathConfig.createCleanDirs()

    val jobInputOutput =
      exec.JobInputOutput(dxIoFunctions, typeAliases, document.version.value, rtDebugLvl)
    val inputs = jobInputOutput.loadInputs(originalInputs, task)
    System.err.println(s"""|Main processing inputs in taskAction
                           |originalInputs:
                           |${originalInputs.prettyPrint}
                           |
                           |processed inputs:
                           |${inputs.mkString("\n")}
                           |""".stripMargin)
    val taskRunner = exec.TaskRunner(
        task,
        document,
        typeAliases,
        instanceTypeDB,
        dxPathConfig,
        dxIoFunctions,
        jobInputOutput,
        defaultRuntimeAttributes,
        delayWorkspaceDestruction,
        rtDebugLvl
    )

    // Running tasks
    op match {
      case InternalOp.TaskCheckInstanceType =>
        // special operation to check if this task is on the right instance type
        val correctInstanceType: Boolean = taskRunner.checkInstanceType(inputs)
        SuccessfulTermination(correctInstanceType.toString)

      case InternalOp.TaskProlog =>
        val (localizedInputs, dxUrl2path) = taskRunner.prolog(inputs)
        taskRunner.writeEnvToDisk(localizedInputs, dxUrl2path)
        SuccessfulTermination(s"success ${op}")

      case InternalOp.TaskInstantiateCommand =>
        val (localizedInputs, dxUrl2path) = taskRunner.readEnvFromDisk()
        val env = taskRunner.instantiateCommand(localizedInputs)
        taskRunner.writeEnvToDisk(env, dxUrl2path)
        SuccessfulTermination(s"success ${op}")

      case InternalOp.TaskEpilog =>
        val (env, dxUrl2path) = taskRunner.readEnvFromDisk()
        val outputFields: Map[String, JsValue] = taskRunner.epilog(env, dxUrl2path)

        // write outputs, ignore null values, these could occur for optional
        // values that were not specified.
        val json = JsObject(outputFields.filter {
          case (_, jsValue) => jsValue != null && jsValue != JsNull
        })
        val ast_pp = json.prettyPrint
        Utils.writeFileContent(jobOutputPath, ast_pp)
        SuccessfulTermination(s"success ${op}")

      case InternalOp.TaskRelaunch =>
        val outputFields: Map[String, JsValue] = taskRunner.relaunch(inputs, originalInputs)
        val json = JsObject(outputFields.filter {
          case (_, jsValue) => jsValue != null && jsValue != JsNull
        })
        val ast_pp = json.prettyPrint
        Utils.writeFileContent(jobOutputPath, ast_pp)
        SuccessfulTermination(s"success ${op}")

      case _ =>
        UnsuccessfulTermination(s"Illegal task operation ${op}")
    }
  }

  // Execute a part of a workflow
  private def workflowFragAction(op: InternalOp.Value,
                                 wdlSourceCode: String,
                                 instanceTypeDB: InstanceTypeDB,
                                 metaInfo: JsValue,
                                 jobInputPath: Path,
                                 jobOutputPath: Path,
                                 dxPathConfig: DxPathConfig,
                                 dxIoFunctions: DxIoFunctions,
                                 defaultRuntimeAttributes: Option[WdlRuntimeAttrs],
                                 delayWorkspaceDestruction: Option[Boolean],
                                 rtDebugLvl: Int): Termination = {
    val dxProject = DxUtils.dxCrntProject
    //val dxProject = DxUtils.dxEnv.getProjectContext()
    val verbose = rtDebugLvl > 0

    // Parse the inputs, convert to WDL values. Delay downloading files
    // from the platform, we may not need to access them.
    val inputLines: String = Utils.readFileContent(jobInputPath)
    val inputsRaw: JsValue = inputLines.parseJson

    val (wf, taskDir, typeAliases, document) =
      ParseWdlSourceFile(verbose).parseWdlWorkflow(wdlSourceCode)

    // setup the utility directories that the frag-runner employs
    val fragInputOutput =
      exec.WfFragInputOutput(dxIoFunctions,
                             dxProject,
                             typeAliases,
                             document.version.value,
                             rtDebugLvl)

    // process the inputs
    val fragInputs = fragInputOutput.loadInputs(inputsRaw, metaInfo)
    val outputFields: Map[String, JsValue] =
      op match {
        case InternalOp.WfFragment =>
          val fragRunner = exec.WfFragRunner(
              wf,
              taskDir,
              typeAliases,
              document,
              instanceTypeDB,
              fragInputs.execLinkInfo,
              dxPathConfig,
              dxIoFunctions,
              inputsRaw,
              fragInputOutput,
              defaultRuntimeAttributes,
              delayWorkspaceDestruction,
              rtDebugLvl
          )
          fragRunner.apply(fragInputs.blockPath, fragInputs.env, RunnerWfFragmentMode.Launch)
        case InternalOp.Collect =>
          val fragRunner = exec.WfFragRunner(
              wf,
              taskDir,
              typeAliases,
              document,
              instanceTypeDB,
              fragInputs.execLinkInfo,
              dxPathConfig,
              dxIoFunctions,
              inputsRaw,
              fragInputOutput,
              defaultRuntimeAttributes,
              delayWorkspaceDestruction,
              rtDebugLvl
          )
          fragRunner.apply(fragInputs.blockPath, fragInputs.env, RunnerWfFragmentMode.Collect)
        case InternalOp.WfInputs =>
          val wfInputs =
            exec.WfInputs(wf, document, typeAliases, dxPathConfig, dxIoFunctions, rtDebugLvl)
          wfInputs.apply(fragInputs.env)
        case InternalOp.WfOutputs =>
          val wfOutputs =
            exec.WfOutputs(wf, document, typeAliases, dxPathConfig, dxIoFunctions, rtDebugLvl)
          wfOutputs.apply(fragInputs.env)

        case InternalOp.WfCustomReorgOutputs =>
          val wfCustomReorgOutputs = exec.WfOutputs(
              wf,
              document,
              typeAliases,
              dxPathConfig,
              dxIoFunctions,
              rtDebugLvl
          )
          // add ___reconf_status as output.
          wfCustomReorgOutputs.apply(fragInputs.env, addStatus = true)

        case InternalOp.WorkflowOutputReorg =>
          val wfReorg = exec.WorkflowOutputReorg(wf,
                                                 document,
                                                 typeAliases,
                                                 dxPathConfig,
                                                 dxIoFunctions,
                                                 rtDebugLvl)
          val refDxFiles = fragInputOutput.findRefDxFiles(inputsRaw, metaInfo)
          wfReorg.apply(refDxFiles)

        case _ =>
          throw new Exception(s"Illegal workflow fragment operation ${op}")
      }

    // write outputs, ignore null values, these could occur for optional
    // values that were not specified.
    val json = JsObject(outputFields)
    val ast_pp = json.prettyPrint
    Utils.writeFileContent(jobOutputPath, ast_pp)

    SuccessfulTermination(s"success ${op}")
  }

  private def getWdlSourceCodeFromDetails(details: JsValue): String = {
    val fields = details.asJsObject.fields
    val JsString(wdlSourceCode) = fields.getOrElse("wdlSourceCode", fields("womSourceCode"))
    wdlSourceCode
  }

  // Get the WDL source code, and the instance type database from the
  // details field stored on the platform
  private def retrieveFromDetails(
      jobInfoPath: Path
  ): (String, InstanceTypeDB, JsValue, Option[WdlRuntimeAttrs], Option[Boolean]) = {
    val jobInfo = Utils.readFileContent(jobInfoPath).parseJson
    val executable: DxExecutable = jobInfo.asJsObject.fields.get("executable") match {
      case None =>
        val dxJob = DxJob(DxUtils.dxEnv.getJob)
        dxJob.describe().executable
      case Some(JsString(x)) if x.startsWith("app-") =>
        DxApp(x)
      case Some(JsString(x)) if x.startsWith("applet-") =>
        DxApplet(x, None)
      case Some(other) =>
        throw new Exception(s"Malformed executable field ${other} in job info")
    }

    val details: JsValue = executable.describe(Set(Field.Details)).details.get
    val wdlSourceCodeEncoded = getWdlSourceCodeFromDetails(details)
    val wdlSourceCode = Utils.base64DecodeAndGunzip(wdlSourceCodeEncoded)

    val JsString(instanceTypeDBEncoded) = details.asJsObject.fields("instanceTypeDB")
    val dbRaw = Utils.base64DecodeAndGunzip(instanceTypeDBEncoded)
    val instanceTypeDB = dbRaw.parseJson.convertTo[InstanceTypeDB]

    val runtimeAttrs: Option[WdlRuntimeAttrs] =
      details.asJsObject.fields.get("runtimeAttrs") match {
        case None         => None
        case Some(JsNull) => None
        case Some(x)      => Some(x.convertTo[WdlRuntimeAttrs])
      }

    val delayWorkspaceDestruction: Option[Boolean] =
      details.asJsObject.fields.get("delayWorkspaceDestruction") match {
        case Some(JsBoolean(flag)) => Some(flag)
        case None                  => None
      }
    (wdlSourceCode, instanceTypeDB, details, runtimeAttrs, delayWorkspaceDestruction)
  }

  // Make a list of all the files cloned for access by this applet.
  // Bulk describe all the them.
  private def runtimeBulkFileDescribe(jobInputPath: Path): Map[String, (DxFile, DxFileDescribe)] = {
    val inputs: JsValue = Utils.readFileContent(jobInputPath).parseJson

    val allFilesReferenced = inputs.asJsObject.fields.flatMap {
      case (_, jsElem) => DxUtils.findDxFiles(jsElem)
    }.toVector

    // Describe all the files, in one go
    val descAll = DxFile.bulkDescribe(allFilesReferenced)
    descAll.map {
      case (DxFile(fid, proj), desc) =>
        fid -> (DxFile(fid, proj), desc)
      case (other, _) =>
        throw new Exception(s"wrong object type ${other} (should be a file)")
    }
  }

  def internalOp(args: Seq[String]): Termination = {
    val operation = InternalOp.values find (x => normKey(x.toString) == normKey(args.head))
    operation match {
      case None =>
        UnsuccessfulTermination(s"unknown internal action ${args.head}")
      case Some(op) if args.length == 4 =>
        val homeDir = Paths.get(args(1))
        val rtDebugLvl = parseRuntimeDebugLevel(args(2))
        val streamAllFiles = parseStreamAllFiles(args(3))
        val (jobInputPath, jobOutputPath, jobErrorPath, jobInfoPath) =
          Utils.jobFilesOfHomeDir(homeDir)
        val dxPathConfig = buildRuntimePathConfig(streamAllFiles, rtDebugLvl >= 1)
        val fileInfoDir = runtimeBulkFileDescribe(jobInputPath)
        val dxIoFunctions = DxIoFunctions(fileInfoDir, dxPathConfig, rtDebugLvl)

        // Get the WDL source code (currently WDL, could be also CWL in the future)
        // Parse the inputs, convert to WDL values.
        val (wdlSourceCode,
             instanceTypeDB,
             metaInfo,
             defaultRuntimeAttrs,
             delayWorkspaceDestruction) =
          retrieveFromDetails(jobInfoPath)

        try {
          op match {
            case InternalOp.Collect | InternalOp.WfFragment | InternalOp.WfInputs |
                InternalOp.WfOutputs | InternalOp.WorkflowOutputReorg |
                InternalOp.WfCustomReorgOutputs =>
              workflowFragAction(
                  op,
                  wdlSourceCode,
                  instanceTypeDB,
                  metaInfo,
                  jobInputPath,
                  jobOutputPath,
                  dxPathConfig,
                  dxIoFunctions,
                  defaultRuntimeAttrs,
                  delayWorkspaceDestruction,
                  rtDebugLvl
              )
            case InternalOp.TaskCheckInstanceType | InternalOp.TaskProlog |
                InternalOp.TaskInstantiateCommand | InternalOp.TaskEpilog |
                InternalOp.TaskRelaunch =>
              taskAction(op,
                         wdlSourceCode,
                         instanceTypeDB,
                         jobInputPath,
                         jobOutputPath,
                         dxPathConfig,
                         dxIoFunctions,
                         defaultRuntimeAttrs,
                         delayWorkspaceDestruction,
                         rtDebugLvl)
          }
        } catch {
          case e: Throwable =>
            writeJobError(jobErrorPath, e)
            UnsuccessfulTermination(s"failure running ${op}")
        }
      case Some(_) =>
        BadUsageTermination(s"""|Bad arguments to internal operation
                                |  ${args}
                                |Usage:
                                |  java -jar dxWDL.jar internal <action> <home dir> <debug level>
                                |""".stripMargin)
    }
  }

  def dispatchCommand(args: Seq[String]): Termination = {
    if (args.isEmpty)
      return BadUsageTermination("")
    val action = Actions.values find (x => normKey(x.toString) == normKey(args.head))
    action match {
      case None => BadUsageTermination("")
      case Some(x) =>
        x match {
          case Actions.Compile  => compile(args.tail)
          case Actions.Describe => describe(args.tail)
          case Actions.Config   => SuccessfulTermination(ConfigFactory.load().toString)
          case Actions.DXNI     => dxni(args.tail)
          case Actions.Internal => internalOp(args.tail)
          case Actions.Version  => SuccessfulTermination(Utils.getVersion)
        }
    }
  }

  val usageMessage =
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

  val termination = dispatchCommand(args.toSeq)

  termination match {
    case SuccessfulTermination(s) =>
      println(s)
    case SuccessfulTerminationTree(pretty) =>
      pretty match {
        case Left(str) => println(str)
        case Right(js) => if (js != JsNull) println(js.prettyPrint)
      }
    case SuccessfulTerminationIR(_) =>
      println("Intermediate representation")
    case BadUsageTermination(s) if s == "" =>
      Console.err.println(usageMessage)
      System.exit(1)
    case BadUsageTermination(s) =>
      Utils.error(s)
      System.exit(1)
    case UnsuccessfulTermination(s) =>
      Utils.error(s)
      System.exit(1)
  }
  System.exit(0)
}
