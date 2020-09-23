package dx.compiler

import java.nio.file.{Files, Path}

import com.typesafe.config.{Config, ConfigFactory}
import dx.api.{
  DxApi,
  DxApplet,
  DxAppletDescribe,
  DxDataObject,
  DxExecutable,
  DxFile,
  DxPath,
  DxProject,
  DxRecord,
  DxUtils,
  DxWorkflow,
  DxWorkflowDescribe,
  Field,
  InstanceTypeDB
}
import dx.core.{Native, getVersion}
import dx.core.io.DxWorkerPaths
import dx.core.ir._
import dx.core.util.CompressionUtils
import dx.translator.Extras
import spray.json._
import wdlTools.util.{FileSourceResolver, FileUtils, JsUtils, Logger, TraceLevel}

import scala.jdk.CollectionConverters._

/**
  * Compile IR to native applets and workflows.
  * @param extras extra configuration
  * @param runtimePathConfig path configuration on the runtime environment
  * @param runtimeTraceLevel trace level to use at runtime
  * @param includeAsset whether to package the runtime asset with generated applications
  * @param archive whether to archive existing applications
  * @param force whether to delete existing executables
  * @param leaveWorkflowsOpen whether to leave generated workflows in the open state
  * @param locked whether to generate locked workflows
  * @param projectWideReuse whether to allow project-wide reuse of applications
  * @param fileResolver the FileSourceResolver
  * @param dxApi the DxApi
  * @param logger the Logge
  */
case class Compiler(extras: Option[Extras],
                    runtimePathConfig: DxWorkerPaths,
                    scatterChunkSize: Int,
                    runtimeTraceLevel: Int,
                    includeAsset: Boolean,
                    archive: Boolean,
                    force: Boolean,
                    leaveWorkflowsOpen: Boolean,
                    locked: Boolean,
                    projectWideReuse: Boolean,
                    streamAllFiles: Boolean,
                    fileResolver: FileSourceResolver = FileSourceResolver.get,
                    dxApi: DxApi = DxApi.get,
                    logger: Logger = Logger.get) {
  // logger for extra trace info
  private val logger2: Logger = dxApi.logger.withTraceIfContainsKey("Native")

  // temp dir where applications will be compiled - it is deleted on shutdown
  private lazy val appCompileDirPath: Path = {
    val p = Files.createTempDirectory("dxWDL_Compile")
    sys.addShutdownHook({
      FileUtils.deleteRecursive(p)
    })
    p
  }

  private case class BundleCompiler(bundle: Bundle, project: DxProject, folder: String) {
    private val parameterLinkSerializer = ParameterLinkSerializer(fileResolver)
    // database of available instance types for the user/org that owns the project
    private val instanceTypeDb =
      InstanceTypeDB.create(project, dxApi = Some(dxApi), logger = logger)
    // directory of the currently existing applets - we don't want to build them
    // if we don't have to.
    private val executableDir =
      DxExecutableDirectory(bundle, project, folder, projectWideReuse, dxApi, logger)

    private def getAssetRecord: Option[DxRecord] = {
      // get billTo and region from the project, then find the runtime asset
      // in the current region.
      val projectRegion = project.describe(Set(Field.Region)).region match {
        case Some(s) => s
        case None    => throw new Exception(s"Cannot get region for project ${project}")
      }
      // Find the runtime dxWDL asset with the correct version. Look inside the
      // project configured for this region. The regions live in dxWDL.conf
      val config = ConfigFactory.load(RuntimeConfigFile)
      val regionToProjectOption: Vector[Config] =
        config.getConfigList("dxWDL.region2project").asScala.toVector
      val regionToProjectConf: Map[String, String] = regionToProjectOption.map { pair =>
        val region = pair.getString("region")
        val project = pair.getString("path")
        region -> project
      }.toMap
      // The mapping from region to project name is list of (region, proj-name) pairs.
      // Get the project for this region.
      val destination = regionToProjectConf.get(projectRegion) match {
        case None       => throw new Exception(s"Region ${projectRegion} is currently unsupported")
        case Some(dest) => dest
      }
      val destRegexp = "(?:(.*):)?(.+)".r
      val (regionalProjectName, folder) = destination match {
        case destRegexp(null, project)   => (project, "/")
        case destRegexp(project, folder) => (project, folder)
        case _ =>
          throw new Exception(s"Bad syntax for destination ${destination}")
      }
      val regionalProject = dxApi.resolveProject(regionalProjectName)
      val assetUri = s"${DxPath.DxUriPrefix}${regionalProject.getId}:${folder}/${RuntimeAsset}"
      logger.trace(s"Looking for asset id at ${assetUri}")
      val dxAsset = dxApi.resolveOnePath(assetUri, Some(regionalProject)) match {
        case dxFile: DxRecord => dxFile
        case other =>
          throw new Exception(s"Found dx object of wrong type ${other} at ${assetUri}")
      }
      // We need the dxWDL runtime library cloned into this project, so it will
      // be available to all subjobs we run.
      dxApi.cloneAsset(dxAsset, project, RuntimeAsset, regionalProject)
      Some(dxAsset)
    }

    private def getAssetLink(record: DxRecord): JsValue = {
      // Extract the archive from the details field
      val desc = record.describe(Set(Field.Details))
      val dxLink =
        try {
          JsUtils.get(desc.details.get, Some("archiveFileId"))
        } catch {
          case _: Throwable =>
            throw new Exception(
                s"record does not have an archive field ${desc.details}"
            )
        }
      val dxFile = DxFile.fromJson(dxApi, dxLink)
      JsObject(
          "name" -> JsString(dxFile.describe().name),
          "id" -> JsObject(DxUtils.DxLinkKey -> JsString(dxFile.id))
      )
    }

    private val runtimeAsset: Option[JsValue] = if (includeAsset) {
      getAssetRecord.map(getAssetLink)
    } else {
      None
    }

    // Add a checksum to a request
    private def checksumRequest(name: String,
                                desc: Map[String, JsValue]): (Map[String, JsValue], String) = {
      logger2.trace(
          s"""|${name} -> checksum request
              |fields = ${JsObject(desc).prettyPrint}

              |""".stripMargin
      )
      // We need to sort the hash-tables. They are natually unsorted,
      // causing the same object to have different checksums.
      val digest =
        CompressionUtils.md5Checksum(JsUtils.makeDeterministic(JsObject(desc)).prettyPrint)
      // Add the checksum to the properies
      val existingDetails: Map[String, JsValue] =
        desc.get("details") match {
          case Some(JsObject(details)) => details
          case None                    => Map.empty
          case other =>
            throw new Exception(s"Bad properties json value ${other}")
        }
      val updatedDetails = existingDetails ++
        Map(
            Native.Version -> JsString(getVersion),
            Native.Checksum -> JsString(digest)
        )
      // Add properties and attributes we don't want to fall under the checksum
      // This allows, for example, moving the dx:executable, while
      // still being able to reuse it.
      val updatedRequest = desc ++ Map(
          "project" -> JsString(project.id),
          "folder" -> JsString(folder),
          "parents" -> JsBoolean(true),
          "details" -> JsObject(updatedDetails)
      )
      (updatedRequest, digest)
    }

    // Create linking information for a dx:executable
    private def createLinkForCall(irCall: Callable, dxObj: DxExecutable): ExecutableLink = {
      val callInputs: Map[String, Type] = irCall.inputVars.map(p => p.name -> p.dxType).toMap
      val callOutputs: Map[String, Type] = irCall.outputVars.map(p => p.name -> p.dxType).toMap
      ExecutableLink(irCall.name, callInputs, callOutputs, dxObj)
    }

    /**
      * Get an existing application if one exists with the given name and matching the
      * given digest.
      * @param name the application name
      * @param digest the application digest
      * @return the existing executable, or None if the executable does not exist, has
      *         change, or a there are multiple executables that cannot be resolved
      *         unambiguously to a single executable.
      */
    private def getExistingExecutable(name: String, digest: String): Option[DxDataObject] = {
      // return the application if it already exists in the project
      executableDir.lookupInProject(name, digest) match {
        case None => ()
        case Some(executable) =>
          (executable.dxClass, executable.desc) match {
            case ("App", _) =>
              dxApi.logger.trace(s"Found existing version of app ${name}")
            case ("Applet", Some(desc: DxAppletDescribe)) =>
              dxApi.logger.trace(
                  s"Found existing version of applet ${name} in folder ${desc.folder}"
              )
            case ("Workflow", Some(desc: DxWorkflowDescribe)) =>
              dxApi.logger.trace(
                  s"Found existing version of workflow ${name} in folder ${desc.folder}"
              )
            case other =>
              throw new Exception(s"bad object ${other}")
          }
          return Some(executable.dataObj)
      }

      val existingExecutables = executableDir.lookup(name)

      existingExecutables match {
        case Vector() =>
          // a rebuild is required and there are no existing executables to clean up
          return None
        case Vector(existingInfo) =>
          // Check if applet code has changed
          existingInfo.checksum match {
            case None =>
              throw new Exception(s"There is an existing non-dxWDL applet ${name}")
            case Some(existingDigest) if digest != existingDigest =>
              dxApi.logger.trace(
                  s"${existingInfo.dxClass} ${name} has changed, rebuild required"
              )
            case _ =>
              dxApi.logger.trace(s"${existingInfo.dxClass} ${name} has not changed")
              return Some(existingInfo.dataObj)
          }
        case v =>
          dxApi.logger.warning(
              s"More than one ${v.head.dxClass} ${name} found in path ${project.id}:${folder}"
          )
      }

      if (archive) {
        // archive the applet/workflow(s)
        executableDir.archive(existingExecutables)
      } else if (force) {
        // remove all existing executables
        executableDir.remove(existingExecutables)
      } else {
        val dxClass = existingExecutables.head.dxClass
        throw new Exception(s"${dxClass} ${name} already exists in ${project.id}:${folder}")
      }

      None
    }

    private def getIdFromResponse(response: JsObject): String = {
      response.fields.get("id") match {
        case Some(JsString(x)) => x
        case None              => throw new Exception("API call did not returnd an ID")
        case other             => throw new Exception(s"API call returned invalid ID ${other}")
      }
    }

    /**
      * Builds an applet if it doesn't exist or has changed since the last
      * compilation, otherwise returns the existing applet.
      * @param applet the applet IR
      * @param dependencyDict previously compiled executables that can be linked
      * @return
      */
    private def maybeBuildApplet(
        applet: Application,
        dependencyDict: Map[String, CompiledExecutable]
    ): (DxApplet, Vector[ExecutableLink]) = {
      logger2.trace(s"Compiling applet ${applet.name}")
      val appletCompiler =
        ApplicationCompiler(
            bundle.typeAliases,
            instanceTypeDb,
            runtimeAsset,
            runtimePathConfig,
            runtimeTraceLevel,
            streamAllFiles,
            scatterChunkSize,
            extras,
            parameterLinkSerializer,
            dxApi,
            logger2
        )
      // limit the applet dictionary to actual dependencies
      val dependencies: Map[String, ExecutableLink] = applet.kind match {
        case ExecutableKindWfFragment(calls, _, _) =>
          calls.map { name =>
            val CompiledExecutable(irCall, dxObj, _, _) = dependencyDict(name)
            name -> createLinkForCall(irCall, dxObj)
          }.toMap
        case _ => Map.empty
      }
      // Calculate a checksum of the inputs that went into the making of the applet.
      val (appletApiRequest, digest) = checksumRequest(
          applet.name,
          appletCompiler.apply(applet, dependencies)
      )
      // write the request to a file, in case we need it for debugging
      if (logger2.traceLevel >= TraceLevel.Verbose) {
        val requestFile = s"${applet.name}_req.json"
        FileUtils.writeFileContent(appCompileDirPath.resolve(requestFile),
                                   JsObject(appletApiRequest).prettyPrint)
      }
      // fetch existing applet or build a new one
      val dxApplet = getExistingExecutable(applet.name, digest) match {
        case Some(dxApplet: DxApplet) =>
          // applet exists and it has not changed
          dxApplet
        case None =>
          // build a new applet
          val response = dxApi.appletNew(appletApiRequest)
          val id = getIdFromResponse(response)
          val dxApplet = dxApi.applet(id)
          executableDir.insert(applet.name, dxApplet, digest)
          dxApplet
        case other =>
          throw new Exception(s"expected applet ${other}")
      }
      (dxApplet, dependencies.values.toVector)
    }

    /**
      * Builds a workflow if it doesn't exist or has changed since the last
      * compilation, otherwise returns the existing workflow.
      * @param workflow the workflow to compile
      * @param dependencyDict previously compiled executables that can be linked
      * @return
      */
    private def maybeBuildWorkflow(
        workflow: Workflow,
        dependencyDict: Map[String, CompiledExecutable]
    ): (DxWorkflow, JsValue) = {
      logger2.trace(s"Compiling workflow ${workflow.name}")
      val workflowCompiler =
        WorkflowCompiler(extras, parameterLinkSerializer, dxApi, logger2)
      // Calculate a checksum of the inputs that went into the making of the applet.
      val (workflowApiRequest, execTree) = workflowCompiler.apply(workflow, dependencyDict)
      val (requestWithChecksum, digest) = checksumRequest(workflow.name, workflowApiRequest)
      // Add properties we do not want to fall under the checksum.
      // This allows, for example, moving the dx:executable, while
      // still being able to reuse it.
      val updatedRequest = requestWithChecksum ++ Map(
          "project" -> JsString(project.id),
          "folder" -> JsString(folder),
          "parents" -> JsBoolean(true)
      )
      val dxWf = getExistingExecutable(workflow.name, digest) match {
        case Some(wf: DxWorkflow) =>
          // workflow exists and has not changed
          wf
        case None =>
          val response = dxApi.workflowNew(updatedRequest)
          val id = getIdFromResponse(response)
          val dxWorkflow = dxApi.workflow(id)
          if (!leaveWorkflowsOpen) {
            // Close the workflow
            dxWorkflow.close()
          }
          executableDir.insert(workflow.name, dxWorkflow, digest)
          dxWorkflow
        case other =>
          throw new Exception(s"expected a workflow, got ${other}")
      }
      (dxWf, execTree)
    }

    def apply: CompilerResults = {
      dxApi.logger.trace(
          s"Generate dx:applets and dx:workflows for ${bundle} in ${project.id}${folder}"
      )
      val executables = bundle.dependencies.foldLeft(Map.empty[String, CompiledExecutable]) {
        case (accu, name) =>
          bundle.allCallables(name) match {
            case application: Application =>
              val execRecord = application.kind match {
                case ExecutableKindNative(ExecutableType.App | ExecutableType.Applet,
                                          Some(id),
                                          _,
                                          _,
                                          _) =>
                  // native applets do not depend on other data-objects
                  CompiledExecutable(application, dxApi.executable(id))
                case ExecutableKindWorkflowCustomReorg(id) =>
                  CompiledExecutable(application, dxApi.executable(id))
                case _ =>
                  val (dxApplet, dependencies) = maybeBuildApplet(application, accu)
                  CompiledExecutable(application, dxApplet, dependencies)
              }
              accu + (application.name -> execRecord)
            case wf: Workflow =>
              val (dxWorkflow, execTree) = maybeBuildWorkflow(wf, accu)
              accu + (wf.name -> CompiledExecutable(wf, dxWorkflow, execTree = Some(execTree)))
          }
      }
      val primary: Option[CompiledExecutable] = bundle.primaryCallable.flatMap { c =>
        executables.get(c.name)
      }
      CompilerResults(primary, executables)
    }
  }

  /**
    * Compile the IR bundle to a native applet or workflow.
    * @param bundle the IR bundle
    * @param project the destination project
    * @param folder the destination folder
    */
  def apply(bundle: Bundle, project: DxProject, folder: String): CompilerResults = {
    BundleCompiler(bundle, project, folder).apply
  }
}
