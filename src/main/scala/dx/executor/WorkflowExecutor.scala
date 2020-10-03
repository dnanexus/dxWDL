package dx.executor

import dx.AppInternalException
import dx.api.{DxAnalysis, DxApp, DxApplet, DxExecution, DxFile, DxWorkflow, Field, FolderContents}
import dx.core.io.DxWorkerPaths
import dx.core.{Constants, getVersion}
import dx.core.ir.{Block, ExecutableLink, Parameter, ParameterLink, Type, TypeSerde, Value}
import dx.executor.wdl.WdlWorkflowSupportFactory
import spray.json._
import wdlTools.util.{Enum, TraceLevel}

object WorkflowAction extends Enum {
  type WorkflowAction = Value
  val Inputs, Outputs, OutputReorg, CustomReorgOutputs, Run, Continue, Collect = Value
}

trait BlockContext[B <: Block[B]] {
  def block: Block[B]

  def launch(): Map[String, ParameterLink]

  def continue(): Map[String, ParameterLink]

  def collect(): Map[String, ParameterLink]
}

object WorkflowSupport {
  val SeqNumber = "seqNumber"
  val JobNameLengthLimit = 50
  val ParentsKey = "parents___"
}

abstract class WorkflowSupport[B <: Block[B]](jobMeta: JobMeta) {
  private val logger = jobMeta.logger
  private val seqNumIter = Iterator.from(1)

  protected def nextSeqNum: Int = seqNumIter.next()

  /**
    *
    * @return
    */
  def typeAliases: Map[String, Type]

  /**
    * Evaluates any expressions in workflow inputs.
    * @return
    */
  def evaluateInputs(jobInputs: Map[String, (Type, Value)]): Map[String, (Type, Value)]

  /**
    * Evaluates any expressions in workflow outputs.
    */
  def evaluateOutputs(jobInputs: Map[String, (Type, Value)],
                      addReorgStatus: Boolean): Map[String, (Type, Value)]

  def evaluateBlockInputs(jobInputs: Map[String, (Type, Value)]): BlockContext[B]

  lazy val execLinkInfo: Map[String, ExecutableLink] =
    jobMeta.getExecutableDetail("execLinkInfo") match {
      case Some(JsObject(fields)) =>
        fields.map {
          case (key, link) =>
            key -> ExecutableLink.deserialize(link, typeAliases, jobMeta.dxApi)
        }
      case None =>
        Map.empty
      case other =>
        throw new Exception(s"Bad value ${other}")
    }

  def getExecutableLink(name: String): ExecutableLink = {
    execLinkInfo.getOrElse(
        name,
        throw new AppInternalException(s"Could not find linking information for ${name}")
    )
  }

  lazy val fqnDictTypes: Map[String, Type] =
    jobMeta.getExecutableDetail(Constants.WfFragmentInputTypes) match {
      case Some(JsObject(fields)) =>
        fields.map {
          case (key, typeJs) =>
            // Transform back to a fully qualified name with dots
            val keyDecoded = Parameter.decodeDots(key)
            val wdlType = TypeSerde.deserialize(typeJs, typeAliases)
            keyDecoded -> wdlType
          case other => throw new Exception(s"Bad value ${other}")
        }
      case other =>
        throw new Exception(s"Bad value ${other}")
    }

  protected def launchJob(executableLink: ExecutableLink,
                          name: String,
                          inputs: Map[String, (Type, Value)],
                          nameDetail: Option[String] = None,
                          instanceType: Option[String] = None): (DxExecution, String) = {
    val jobName: String = nameDetail.map(hint => s"${name} ${hint}").getOrElse(name)
    val callInputsJs = JsObject(jobMeta.outputSerializer.createFieldsFromMap(inputs))
    logger.traceLimited(s"execDNAx ${callInputsJs.prettyPrint}", minLevel = TraceLevel.VVerbose)

    // Last check that we have all the compulsory arguments.
    // Note that we don't have the information here to tell difference between optional and non
    // optionals. Right now, we are emitting warnings for optionals or arguments that have defaults.
    executableLink.inputs.keys.filterNot(callInputsJs.fields.contains).foreach { argName =>
      logger.warning(s"Missing argument ${argName} to call ${executableLink.name}", force = true)
    }

    // We may need to run a collect subjob. Add the the sequence
    // number to each invocation, so the collect subjob will be
    // able to put the results back together in the correct order.
    val seqNum: Int = nextSeqNum

    // If this is a task that specifies the instance type
    // at runtime, launch it in the requested instance.
    val dxExecution = executableLink.dxExec match {
      case app: DxApp =>
        app.newRun(
            jobName,
            callInputsJs,
            instanceType = instanceType,
            details = Some(JsObject(WorkflowSupport.SeqNumber -> JsNumber(seqNum))),
            delayWorkspaceDestruction = jobMeta.delayWorkspaceDestruction
        )
      case applet: DxApplet =>
        applet.newRun(
            jobName,
            callInputsJs,
            instanceType = instanceType,
            details = Some(JsObject(WorkflowSupport.SeqNumber -> JsNumber(seqNum))),
            delayWorkspaceDestruction = jobMeta.delayWorkspaceDestruction
        )
      case workflow: DxWorkflow =>
        workflow.newRun(
            jobName,
            callInputsJs,
            Some(JsObject(WorkflowSupport.SeqNumber -> JsNumber(seqNum))),
            jobMeta.delayWorkspaceDestruction
        )
      case other =>
        throw new Exception(s"Unsupported executable ${other}")
    }
    (dxExecution, jobName)
  }
}

trait WorkflowSupportFactory {
  def create(jobMeta: JobMeta): Option[WorkflowSupport[_]]
}

object WorkflowExecutor {
  val MaxNumFilesMoveLimit = 1000
  val IntermediateResultsFolder = "intermediate"

  val workflowSupportFactories: Vector[WorkflowSupportFactory] = Vector(
      WdlWorkflowSupportFactory()
  )

  def createWorkflowSupport(jobMeta: JobMeta): WorkflowSupport[_] = {
    workflowSupportFactories
      .collectFirst { factory =>
        factory.create(jobMeta) match {
          case Some(executor) => executor
        }
      }
      .getOrElse(
          throw new Exception("Cannot determine language/version from source code")
      )
  }
}

case class WorkflowExecutor(jobMeta: JobMeta, dxWorkerPaths: Option[DxWorkerPaths] = None) {
  private[executor] val workflowSupport: WorkflowSupport[_] =
    WorkflowExecutor.createWorkflowSupport(jobMeta)
  private val dxApi = jobMeta.dxApi
  private val logger = jobMeta.logger

  private lazy val jobInputs: Map[String, (Type, Value)] = jobMeta.jsInputs.collect {
    case (name, jsValue) if !name.endsWith(ParameterLink.FlatFilesSuffix) =>
      val fqn = Parameter.decodeDots(name)
      val irType = workflowSupport.fqnDictTypes.getOrElse(
          fqn,
          throw new Exception(s"Did not find variable ${fqn} (${name}) in the block environment")
      )
      val irValue = jobMeta.inputDeserializer.deserializeInputWithType(jsValue, irType)
      fqn -> (irType, irValue)
  }

  private def evaluateInputs(): Map[String, ParameterLink] = {
    if (logger.isVerbose) {
      logger.traceLimited(s"dxWDL version: ${getVersion}")
      logger.traceLimited(s"Environment: ${jobInputs}")
      logger.traceLimited("Artificial applet for unlocked workflow inputs")
    }
    val inputs = workflowSupport.evaluateInputs(jobInputs)
    jobMeta.createOutputLinks(inputs)
  }

  private def evaluateOutputs(addReorgStatus: Boolean = false): Map[String, ParameterLink] = {
    if (logger.isVerbose) {
      logger.traceLimited(s"dxWDL version: ${getVersion}")
      logger.traceLimited(s"Environment: ${jobInputs}")
      logger.traceLimited("Evaluating workflow outputs")
    }
    val outputs = workflowSupport.evaluateOutputs(jobInputs, addReorgStatus)
    jobMeta.createOutputLinks(outputs)
  }

  private def evaluateFragInputs(): BlockContext[_] = {
    if (logger.isVerbose) {
      logger.traceLimited(s"dxWDL version: ${getVersion}")
      logger.traceLimited(s"link info=${workflowSupport.execLinkInfo}")
      logger.traceLimited(s"Environment: ${jobInputs}")
    }
    val blockCtx = workflowSupport.evaluateBlockInputs(jobInputs)
    logger.traceLimited(
        s"""|Block ${jobMeta.blockPath} to execute:
            |${blockCtx.block.prettyFormat}
            |""".stripMargin
    )
    blockCtx
  }

  private def getAnalysisOutputFiles(analysis: DxAnalysis): Map[String, DxFile] = {
    val desc = analysis.describe(Set(Field.Input, Field.Output))
    val fileOutputs: Set[DxFile] = DxFile.findFiles(dxApi, desc.output.get).toSet
    val fileInputs: Set[DxFile] = DxFile.findFiles(dxApi, desc.input.get).toSet
    val analysisFiles: Vector[DxFile] = (fileOutputs -- fileInputs).toVector
    if (logger.isVerbose) {
      logger.traceLimited(s"analysis has ${fileOutputs.size} output files")
      logger.traceLimited(s"analysis has ${fileInputs.size} input files")
      logger.traceLimited(s"analysis has ${analysisFiles.size} real outputs")
    }
    val filteredAnalysisFiles: Map[String, DxFile] =
      if (analysisFiles.size > WorkflowExecutor.MaxNumFilesMoveLimit) {
        dxApi.logger.traceLimited(
            s"WARNING: Large number of outputs (${analysisFiles.size}), not moving objects"
        )
        Map.empty
      } else {
        logger.traceLimited("Checking timestamps")
        // Retain only files that were created AFTER the analysis started
        val describedFiles = dxApi.describeFilesBulk(analysisFiles)
        val analysisCreated: java.util.Date = desc.getCreationDate
        describedFiles.collect {
          case dxFile if dxFile.describe().getCreationDate.compareTo(analysisCreated) >= 0 =>
            dxFile.id -> dxFile
        }.toMap
      }
    logger.traceLimited(s"analysis has ${filteredAnalysisFiles.size} verified output files")
    filteredAnalysisFiles
  }

  private def reorganizeOutputsDefault(): Map[String, ParameterLink] = {
    logger.traceLimited(s"dxWDL version: ${getVersion}")
    val analysis = jobMeta.analysis.get
    val analysisFiles = getAnalysisOutputFiles(analysis)
    if (analysisFiles.isEmpty) {
      logger.trace(s"no output files to reorganize for analysis ${analysis.id}")
    } else {
      val outputIds: Set[String] = jobMeta.jsInputs
        .collect {
          case (name, jsValue) if !name.endsWith(ParameterLink.FlatFilesSuffix) =>
            val fqn = Parameter.decodeDots(name)
            if (!workflowSupport.fqnDictTypes.contains(fqn)) {
              throw new Exception(
                  s"Did not find variable ${fqn} (${name}) in the block environment"
              )
            }
            DxFile.findFiles(dxApi, jsValue)
        }
        .flatten
        .map(_.id)
        .toSet
      val (exportFiles, intermediateFiles) = analysisFiles.partition {
        case (id, _) => outputIds.contains(id)
      }
      val exportNames: Vector[String] = exportFiles.values.map(_.describe().name).toVector
      logger.traceLimited(s"exportFiles=${exportNames}")
      if (intermediateFiles.isEmpty) {
        logger.trace("no intermediate files to reorganize for analysis ${analysis.id}")
      } else {
        val outputFolder = analysis.describe().folder
        logger.traceLimited(
            s"proj=${jobMeta.projectDesc.name} outFolder=${outputFolder}"
        )
        val intermediateFolder = s"outputFolder/${WorkflowExecutor.IntermediateResultsFolder}"
        val project = jobMeta.project
        val folderContents: FolderContents = project.listFolder(outputFolder)
        if (!folderContents.subFolders.contains(intermediateFolder)) {
          logger.traceLimited(s"Creating intermediate results sub-folder ${intermediateFolder}")
          project.newFolder(intermediateFolder, parents = true)
        } else {
          logger.traceLimited(
              s"Intermediate results sub-folder ${intermediateFolder} already exists"
          )
        }
        project.moveObjects(intermediateFiles.values.toVector, intermediateFolder)
      }
    }
    // reorg app has no outputs
    Map.empty
  }

  def apply(action: WorkflowAction.WorkflowAction): (Map[String, ParameterLink], String) = {
    try {
      val outputs: Map[String, ParameterLink] = action match {
        case WorkflowAction.Inputs =>
          evaluateInputs()
        case WorkflowAction.Run =>
          evaluateFragInputs().launch()
        case WorkflowAction.Continue =>
          evaluateFragInputs().continue()
        case WorkflowAction.Collect =>
          evaluateFragInputs().collect()
        case WorkflowAction.Outputs =>
          evaluateOutputs()
        case WorkflowAction.CustomReorgOutputs =>
          evaluateOutputs(addReorgStatus = true)
        case WorkflowAction.OutputReorg =>
          reorganizeOutputsDefault()
        case _ =>
          throw new Exception(s"Illegal workflow fragment operation ${action}")
      }
      jobMeta.writeOutputLinks(outputs)
      (outputs, s"success ${action}")
    } catch {
      case e: Throwable =>
        jobMeta.error(e)
        throw e
    }
  }
}
