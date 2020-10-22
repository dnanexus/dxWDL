package dx.compiler

import dx.api.{
  DxAccessLevel,
  DxApi,
  DxFile,
  DxInstanceType,
  DxPath,
  DxUtils,
  InstanceTypeDB,
  InstanceTypeRequest
}
import dx.core.Constants
import dx.core.io.{DxWorkerPaths, StreamFiles}
import dx.core.ir._
import wdlTools.util.CodecUtils
import dx.translator.{DockerRegistry, DxAccess, DxExecPolicy, DxRunSpec, DxTimeout, Extras}
import dx.translator.CallableAttributes._
import dx.translator.RunSpec._
import spray.json._
import wdlTools.generators.Renderer
import wdlTools.util.Logger

object ApplicationCompiler {
  val DefaultAppletTimeoutInDays = 2
  // templates
  private val GenericDockerPreambleTemplate = "templates/generic_docker_preamble.ssp"
  //private val EcrDockerPreambleTemplate = "templates/ecr_docker_preamble.ssp"
  private val DynamicAppletJobTemplate = "templates/dynamic_applet_script.ssp"
  private val StaticAppletJobTemplate = "templates/static_applet_script.ssp"
  private val WorkflowFragmentTempalate = "templates/workflow_fragment_script.ssp"
  private val CommandTempalate = "templates/workflow_command_script.ssp"
  // keys used in templates
  private val RegistryKey = "registry"
  private val UsernameKey = "username"
}

case class ApplicationCompiler(typeAliases: Map[String, Type],
                               instanceTypeDb: InstanceTypeDB,
                               runtimeAsset: Option[JsValue],
                               runtimePathConfig: DxWorkerPaths,
                               runtimeTraceLevel: Int,
                               streamFiles: StreamFiles.StreamFiles,
                               scatterChunkSize: Int,
                               extras: Option[Extras],
                               parameterLinkSerializer: ParameterLinkSerializer,
                               dxApi: DxApi = DxApi.get,
                               logger: Logger = Logger.get)
    extends ExecutableCompiler(extras, parameterLinkSerializer, dxApi) {

  // renderer for job script templates
  private lazy val renderer = Renderer()

  // Preamble required for accessing a private docker registry (if required)
  private lazy val dockerRegistry: Option[DockerRegistry] = extras.flatMap(_.dockerRegistry)
  private lazy val dockerPreamble: String = {
    dockerRegistry match {
      case None                                                  => ""
      case Some(DockerRegistry(registry, username, credentials)) =>
        // check that the credentials file is a valid platform path
        try {
          logger.ignore(dxApi.resolveFile(credentials))
        } catch {
          case e: Throwable =>
            throw new Exception(s"""|credentials has to point to a platform file.
                                    |It is now:
                                    |   ${credentials}
                                    |Error:
                                    |  ${e}
                                    |""".stripMargin)
        }
        // render the preamble
        renderer.render(
            ApplicationCompiler.GenericDockerPreambleTemplate,
            Map(
                ApplicationCompiler.RegistryKey -> registry,
                ApplicationCompiler.UsernameKey -> username,
                // strip the URL from the dx:// prefix, so we can use dx-download directly
                "credentials" -> credentials.substring(DxPath.DxUriPrefix.length)
            )
        )
    }
  }

  private def generateJobScript(applet: Application): String = {
    val templateAttrs: Map[String, Any] = Map(
        "rtTraceLevel" -> runtimeTraceLevel,
        "streamFiles" -> streamFiles
    )
    applet.kind match {
      case ExecutableKindApplet =>
        val template = applet.instanceType match {
          case DynamicInstanceType => ApplicationCompiler.DynamicAppletJobTemplate
          case _                   => ApplicationCompiler.StaticAppletJobTemplate
        }
        renderer.render(
            template,
            templateAttrs ++ Map(
                "dockerPreamble" -> dockerPreamble,
                "dxPathConfig" -> runtimePathConfig
            )
        )
      case _: ExecutableKindWfFragment =>
        renderer.render(
            ApplicationCompiler.WorkflowFragmentTempalate,
            templateAttrs
        )
      case other =>
        ExecutableKind.getCommand(other) match {
          case Some(command) =>
            renderer.render(
                ApplicationCompiler.CommandTempalate,
                templateAttrs + ("command" -> command)
            )
          case _ =>
            throw new RuntimeException(
                s"should not generate job script for kind ${other}"
            )
        }
    }
  }

  private def createRunSpec(applet: Application): (JsValue, Map[String, JsValue]) = {
    // find the dxWDL asset
    val instanceType: DxInstanceType = applet.instanceType match {
      case StaticInstanceType(dxInstanceType, memoryMB, diskGB, diskType, cpu, gpu, os) =>
        val request = InstanceTypeRequest(dxInstanceType, memoryMB, diskGB, diskType, cpu, gpu, os)
        instanceTypeDb.apply(request)
      case DefaultInstanceType | DynamicInstanceType =>
        instanceTypeDb.defaultInstanceType
    }
    // Generate the applet's job script
    val jobScript = generateJobScript(applet)
    // build the run spec
    val runSpecRequired = Map(
        "code" -> JsString(jobScript),
        "interpreter" -> JsString("bash"),
        "systemRequirements" ->
          JsObject(
              "main" ->
                JsObject("instanceType" -> JsString(instanceType.name))
          ),
        "distribution" -> JsString(Constants.OsDistribution),
        "release" -> JsString(Constants.OsRelease),
        "version" -> JsString(Constants.OsVersion)
    )
    // Add default timeout
    val defaultTimeout =
      DxRunSpec(
          None,
          None,
          None,
          Some(DxTimeout(Some(ApplicationCompiler.DefaultAppletTimeoutInDays), Some(0), Some(0)))
      ).toRunSpecJson
    // Start with the default dx-attribute section, and override
    // any field that is specified in the runtime hints or the individual task section.
    val extrasOverrides = extras.flatMap(_.defaultTaskDxAttributes) match {
      case Some(dta) => dta.getRunSpecJson
      case None      => Map.empty
    }
    // runtime hints in the task override defaults from extras
    val taskOverrides: Map[String, JsValue] = applet.requirements
      .collect {
        case RestartRequirement(max, default, errors) =>
          val defaultMap: Map[String, Long] = default match {
            case Some(i) => Map("*" -> i)
            case _       => Map.empty
          }
          DxExecPolicy(errors ++ defaultMap, max).toJson
        case TimeoutRequirement(days, hours, minutes) =>
          DxTimeout(days.orElse(Some(0)), hours.orElse(Some(0)), minutes.orElse(Some(0))).toJson
      }
      .flatten
      .toMap
    // task-specific settings from extras override runtime hints in the task
    val taskSpecificOverrides = applet.kind match {
      case ExecutableKindApplet =>
        extras.flatMap(_.perTaskDxAttributes.get(applet.name)) match {
          case Some(dta) => dta.getRunSpecJson
          case None      => Map.empty
        }
      case _ => Map.empty
    }
    // If the docker image is a tarball, add a link in the details field.
    val dockerFile: Option[DxFile] = applet.container match {
      case DxFileDockerImage(_, dxfile) => Some(dxfile)
      case NoImage                      => None
      case NetworkDockerImage           => None
    }
    val bundledDepends = runtimeAsset match {
      case Some(jsv) => Map("bundledDepends" -> JsArray(Vector(jsv)))
      case None      => Map.empty
    }
    val runSpec = JsObject(
        runSpecRequired ++ defaultTimeout ++ extrasOverrides ++ taskOverrides ++ taskSpecificOverrides ++ bundledDepends
    )
    val details: Map[String, JsValue] = dockerFile match {
      case Some(dxFile) => Map("docker-image" -> dxFile.asJson)
      case None         => Map.empty
    }
    (runSpec, details)
  }

  // Convert the applet meta to JSON, and overlay details from task-specific extras
  private def applicationAttributesToNative(
      applet: Application,
      defaultTags: Set[String]
  ): (Map[String, JsValue], Map[String, JsValue]) = {
    val (commonMeta, commonDetails) = callableAttributesToNative(applet, defaultTags)
    val applicationMeta = applet.attributes.collect {
      case DeveloperNotesAttribute(text) => "developerNotes" -> JsString(text)
      // These are currently ignored because they only apply to apps
      //case VersionAttribute(text) => Some("version" -> JsString(text))
      //case OpenSourceAttribute(isOpenSource) =>
      //  Some("openSource" -> JsBoolean(isOpenSource))
      //case CategoriesAttribute(categories) =>
      //  Some("categories" -> categories.mapValues(anyToJs))
    }
    // Default and WDL-specified details can be overridden in task-specific extras
    val taskSpecificDetails = (applet.kind match {
      case ExecutableKindApplet =>
        extras.flatMap(_.perTaskDxAttributes.get(applet.name).map(_.getDetailsJson))
      case _ => None
    }).getOrElse(Map.empty)
    (commonMeta ++ applicationMeta, commonDetails ++ taskSpecificDetails)
  }

  def createAccess(applet: Application): JsValue = {
    // defaults are taken from
    // extras global defaults < task runtime section < task-specific extras
    val defaultAccess: DxAccess = extras.map(_.getDefaultAccess).getOrElse(DxAccess.empty)
    val taskAccess: DxAccess = applet.requirements
      .collectFirst {
        case AccessRequirement(network, project, allProjects, developer, projectCreation) =>
          DxAccess(network,
                   project.map(DxAccessLevel.withName),
                   allProjects.map(DxAccessLevel.withName),
                   developer,
                   projectCreation)
      }
      .getOrElse(DxAccess.empty)
    val taskSpecificAccess: DxAccess = (applet.kind match {
      case ExecutableKindApplet =>
        extras.map(_.getTaskAccess(applet.name))
      case _ => None
    }).getOrElse(DxAccess.empty)
    // If we are using a private docker registry, add the allProjects: VIEW
    // access to tasks.
    val allProjectsAccess: DxAccess = dockerRegistry match {
      case None    => DxAccess.empty
      case Some(_) => DxAccess.empty.copy(allProjects = Some(DxAccessLevel.View))
    }
    // update depending on applet type
    val appletKindAccess = applet.kind match {
      case ExecutableKindApplet if applet.container == NetworkDockerImage =>
        // docker requires network access, because we are downloading the image from the network
        Some(DxAccess.empty.copy(network = Vector("*")))
      case ExecutableKindApplet =>
        None
      case ExecutableKindWorkflowOutputReorg =>
        // The reorg applet requires higher permissions to organize the output directory.
        Some(DxAccess.empty.copy(project = Some(DxAccessLevel.Contribute)))
      case _ =>
        // Scatters need network access, because they spawn subjobs that (may) use dx-docker.
        // We end up allowing all applets to use the network
        Some(DxAccess.empty.copy(network = Vector("*")))
    }
    // merge all
    val access = defaultAccess
      .merge(taskAccess)
      .merge(taskSpecificAccess)
      .merge(allProjectsAccess)
      .mergeOpt(appletKindAccess)
    access.toJson match {
      case fields if fields.isEmpty => JsNull
      case fields                   => JsObject(fields)
    }
  }

  /**
    * Builds an '/applet/new' request.
    * For applets that call other applets, we pass a directory of the callees,
    * so they can be found at runtime.
    * @param applet applet IR
    * @param executableDict mapping of callable names to executables
    * @return
    */
  def apply(
      applet: Application,
      executableDict: Map[String, ExecutableLink]
  ): Map[String, JsValue] = {
    logger.trace(s"Building /applet/new request for ${applet.name}")
    // convert inputs and outputs to dxapp inputSpec
    val inputSpec: Vector[JsValue] = applet.inputs
      .sortWith(_.name < _.name)
      .flatMap { param =>
        try {
          inputParameterToNative(param)
        } catch {
          case ex: Throwable =>
            throw new Exception(
                s"Error converting input parameter ${param} to native type",
                ex
            )
        }
      }
    val outputSpec: Vector[JsValue] = applet.outputs
      .sortWith(_.name < _.name)
      .flatMap { param =>
        try {
          outputParameterToNative(param)
        } catch {
          case ex: Throwable =>
            throw new Exception(
                s"Error converting output parameter ${param} to native type",
                ex
            )
        }
      }
    // build the dxapp runSpec
    val (runSpec, runSpecDetails) = createRunSpec(applet)
    // A fragemnt is hidden, not visible under default settings. This
    // allows the workflow copying code to traverse it, and link to
    // anything it calls.
    val hidden: Boolean =
      applet.kind match {
        case _: ExecutableKindWfFragment => true
        case _                           => false
      }
    // create linking information - results in two maps, one that's added to the
    // application details, and one with links to applets that could get called
    // at runtime (if this applet is copied, we need to maintain referential integrity)
    val (dxLinks, linkInfo) = executableDict.map {
      case (name, link) =>
        val linkName = s"link_${name}"
        (
            linkName -> JsObject(DxUtils.DxLinkKey -> JsString(link.dxExec.id)),
            name -> ExecutableLink.serialize(link)
        )
    }.unzip
    // build the details JSON
    val defaultTags = Set(Constants.CompilerTag)
    val (taskMeta, taskDetails) = applicationAttributesToNative(applet, defaultTags)
    val delayDetails = delayWorkspaceDestructionToNative
    // meta information used for running workflow fragments
    val metaDetails: Map[String, JsValue] =
      applet.kind match {
        case ExecutableKindWfFragment(_, blockPath, inputs) =>
          Map(
              Constants.ExecLinkInfo -> JsObject(linkInfo.toMap),
              Constants.BlockPath -> JsArray(blockPath.map(JsNumber(_))),
              Constants.WfFragmentInputTypes -> TypeSerde.serialize(inputs),
              Constants.ScatterChunkSize -> JsNumber(scatterChunkSize)
          )
        case ExecutableKindWfInputs | ExecutableKindWfOutputs | ExecutableKindWfCustomReorgOutputs |
            ExecutableKindWorkflowOutputReorg =>
          val types = applet.inputVars.map(p => p.name -> p.dxType).toMap
          Map(Constants.WfFragmentInputTypes -> TypeSerde.serialize(types))
        case _ =>
          Map.empty
      }
    // compress and base64 encode the source code
    val sourceEncoded = CodecUtils.gzipAndBase64Encode(applet.document.toString)
    // serialize the pricing model, and make the prices opaque.
    val dbOpaque = InstanceTypeDB.opaquePrices(instanceTypeDb)
    val dbOpaqueEncoded = CodecUtils.gzipAndBase64Encode(dbOpaque.toJson.prettyPrint)
    // serilize default runtime attributes
    val defaultRuntimeAttributes =
      extras
        .map(ex => JsObject(ValueSerde.serializeMap(ex.defaultRuntimeAttributes)))
        .getOrElse(JsNull)
    val auxDetails = Map(
        Constants.SourceCode -> JsString(sourceEncoded),
        Constants.InstanceTypeDb -> JsString(dbOpaqueEncoded),
        Constants.RuntimeAttributes -> defaultRuntimeAttributes
    )
    // combine all details into a single Map
    val details: Map[String, JsValue] =
      taskDetails ++ runSpecDetails ++ delayDetails ++ dxLinks.toMap ++ metaDetails ++ auxDetails
    // build the API request
    val requestRequired = Map(
        "name" -> JsString(applet.name),
        "inputSpec" -> JsArray(inputSpec),
        "outputSpec" -> JsArray(outputSpec),
        "runSpec" -> runSpec,
        "dxapi" -> JsString(dxApi.version),
        "details" -> JsObject(details),
        "hidden" -> JsBoolean(hidden)
    )
    // look for ignoreReuse in runtime hints and in extras - the later overrides the former
    val ignoreReuse = applet.requirements
      .collectFirst {
        case IgnoreReuseRequirement(value) => value
      }
      .orElse(
          extras.flatMap(_.ignoreReuse)
      )
      .map(ignoreReuse => Map("ignoreReuse" -> JsBoolean(ignoreReuse)))
      .getOrElse(Map.empty)
    // build the dxapp access section
    val access = createAccess(applet) match {
      case JsNull  => Map.empty
      case jsValue => Map("access" -> jsValue)
    }
    taskMeta ++ requestRequired ++ access ++ ignoreReuse
  }
}
