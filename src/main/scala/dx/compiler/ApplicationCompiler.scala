package dx.compiler

import dx.api.{ConstraintOper, DxApi, DxConstraint, DxIOSpec, DxPath, DxUtils, InstanceTypeDbQuery}
import dx.compiler.ir.RunSpec.DynamicInstanceType
import dx.compiler.ir.{DockerRegistry, ParameterAttributes}
import dx.core.io.DxPathConfig
import dx.core.ir.Type.TBoolean
import dx.core.ir.{
  Application,
  ExecutableKind,
  ExecutableKindApplet,
  ExecutableKindWfFragment,
  ExecutableLink,
  Parameter,
  ParameterAttribute,
  Type,
  Value
}
import dx.core.ir.Value.{VArray, VBoolean, VFile, VFloat, VInt, VNull, VString}
import dx.core.languages.wdl.{ParameterLinkSerde, WdlExecutableLink}
import dx.core.util.CompressionUtils
import spray.json.{JsArray, JsBoolean, JsNull, JsNumber, JsObject, JsString, JsTrue, JsValue}
import wdlTools.generators.Renderer
import wdlTools.generators.code.WdlV1Generator
import wdlTools.types.WdlTypes
import wdlTools.util.Logger

case class ApplicationCompiler(runtimePathConfig: DxPathConfig,
                               runtimeTraceLevel: Int,
                               dockerRegistry: Option[DockerRegistry],
                               parameterLinkSerializer: ParameterLinkSerde,
                               dxApi: DxApi = DxApi.get,
                               logger: Logger = Logger.get) {

  // renderer for job script templates
  private lazy val renderer = Renderer()
  private val DockerPreambleTemplate = "templates/docker_preamble.ssp"
  private val DynamicAppletJobTemplate = "templates/dynamic_applet_script.ssp"
  private val StaticAppletJobTemplate = "templates/static_applet_script.ssp"
  private val WorkflowFragmentTempalate = "templates/workflow_fragment_script.ssp"
  private val CommandTempalate = "templates/command_script.ssp"

  // Preamble required for accessing a private docker registry (if required)
  private lazy val dockerPreamble: String = {
    dockerRegistry match {
      case None                                                  => ""
      case Some(DockerRegistry(registry, username, credentials)) =>
        // check that the credentials file is a valid platform path
        try {
          dxApi.logger.ignore(dxApi.resolveDxUriFile(credentials))
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
            DockerPreambleTemplate,
            Map(
                "registry" -> registry,
                "username" -> username,
                // strip the URL from the dx:// prefix, so we can use dx-download directly
                "credentials" -> credentials.substring(DxPath.DxUriPrefix.length)
            )
        )
    }
  }

  private def constraintToNative(constraint: ParameterAttributes.Constraint): JsValue = {
    constraint match {
      case ParameterAttributes.StringConstraint(s) => JsString(s)
      case ParameterAttributes.CompoundConstraint(oper, constraints) =>
        val dxOper = oper match {
          case ConstraintOper.And => DxConstraint.And
          case ConstraintOper.Or  => DxConstraint.Or
          case _                  => throw new Exception(s"Invalid operation ${oper}")
        }
        JsObject(Map(dxOper -> JsArray(constraints.map(constraintToNative))))
    }
  }

  private def defaultValueToNative(value: Value): JsValue = {
    value match {
      case VNull       => JsNull
      case VBoolean(b) => JsBoolean(b)
      case VInt(i)     => JsNumber(i)
      case VFloat(f)   => JsNumber(f)
      case VString(s)  => JsString(s)
      case VFile(f)    => dxApi.resolveDxUriFile(f).getLinkAsJson
      // TODO: case VDirectory(d) =>
      case VArray(array) => JsArray(array.map(defaultValueToNative))
    }
  }

  // Create the IO Attributes
  private def parameterAttributeToNative(attrs: Vector[ParameterAttribute],
                                         hasDefault: Boolean): Map[String, JsValue] = {
    attrs.flatMap {
      case ParameterAttributes.GroupAttribute(text) =>
        Some(DxIOSpec.Group -> JsString(text))
      case ParameterAttributes.HelpAttribute(text) =>
        Some(DxIOSpec.Help -> JsString(text))
      case ParameterAttributes.LabelAttribute(text) =>
        Some(DxIOSpec.Label -> JsString(text))
      case ParameterAttributes.PatternsAttribute(patterns) =>
        patterns match {
          case ParameterAttributes.PatternsArray(array) =>
            Some(DxIOSpec.Patterns -> JsArray(array.map(JsString(_))))
          // If we have the alternative patterns object, extrac the values, if any at all
          case ParameterAttributes.PatternsObject(name, klass, tags) =>
            Vector(
                if (name.isEmpty) None else Some("name" -> JsArray(name.map(JsString(_)))),
                if (tags.isEmpty) None else Some("tags" -> JsArray(tags.map(JsString(_)))),
                klass.map("class" -> JsString(_))
            ).flatten match {
              case Vector() => None
              case v        => Some(DxIOSpec.Patterns -> JsObject(v.toMap))
            }
        }
      case ParameterAttributes.ChoicesAttribute(choices) =>
        Some(DxIOSpec.Choices -> JsArray(choices.map {
          case ParameterAttributes.SimpleChoice(VString(value))  => JsString(value)
          case ParameterAttributes.SimpleChoice(VInt(value))     => JsNumber(value)
          case ParameterAttributes.SimpleChoice(VFloat(value))   => JsNumber(value)
          case ParameterAttributes.SimpleChoice(VBoolean(value)) => JsBoolean(value)
          case ParameterAttributes.FileChoice(value, name) => {
            // TODO: support project and record choices
            val dxLink = dxApi.resolveDxUriFile(value).getLinkAsJson
            if (name.isDefined) {
              JsObject(Map("name" -> JsString(name.get), "value" -> dxLink))
            } else {
              dxLink
            }
          }
          // TODO: ParameterAttributes.DirectoryChoice
        }))
      case ParameterAttributes.SuggestionsAttribute(suggestions) =>
        Some(DxIOSpec.Suggestions -> JsArray(suggestions.map {
          case ParameterAttributes.SimpleSuggestion(VString(value))  => JsString(value)
          case ParameterAttributes.SimpleSuggestion(VInt(value))     => JsNumber(value)
          case ParameterAttributes.SimpleSuggestion(VFloat(value))   => JsNumber(value)
          case ParameterAttributes.SimpleSuggestion(VBoolean(value)) => JsBoolean(value)
          case ParameterAttributes.FileSuggestion(value, name, project, path) => {
            // TODO: support project and record suggestions
            val dxLink: Option[JsValue] = value match {
              case Some(str) => Some(dxApi.resolveDxUriFile(str).getLinkAsJson)
              case None      => None
            }
            if (name.isDefined || project.isDefined || path.isDefined) {
              val attrs: Map[String, JsValue] = Vector(
                  if (dxLink.isDefined) Some("value" -> dxLink.get) else None,
                  if (name.isDefined) Some("name" -> JsString(name.get)) else None,
                  if (project.isDefined) Some("project" -> JsString(project.get)) else None,
                  if (path.isDefined) Some("path" -> JsString(path.get)) else None
              ).flatten.toMap
              JsObject(attrs)
            } else if (dxLink.isDefined) {
              dxLink.get
            } else {
              throw new Exception(
                  "Either 'value' or 'project' + 'path' must be defined for suggestions"
              )
            }
          }
          // TODO: ParameterAttributes.DirectorySuggestion
        }))
      case ParameterAttributes.TypeAttribute(constraint) =>
        Some(DxIOSpec.Type -> constraintToNative(constraint))
      case ParameterAttributes.DefaultAttribute(value) if !hasDefault =>
        // The default was specified in parameter_meta and was not specified in the
        // parameter declaration
        Some(DxIOSpec.Default -> defaultValueToNative(value))
      case _ => None
    }.toMap
  }

  /**
    * Converts an IR Paramter to a native input/output spec.
    *
    * For primitive types, and arrays of such types, we can map directly
    * to the equivalent dx types. For example,
    * Int  -> int
    * Array[String] -> array:string
    *
    * Arrays can be empty, which is why they are always marked "optional".
    * This notifies the platform runtime system not to throw an exception
    * for an empty input/output array.
    *
    * Ragged arrays, maps, and objects, cannot be mapped in such a trivial way.
    * These are called "Complex Types", or "Complex". They are handled
    * by passing a JSON structure and a vector of dx:files.
    *
    * @param parameter the parameter
    * @return
    */
  private def parameterToNative(parameter: Parameter): Vector[JsValue] = {
    val name = parameter.dxName

    val defaultVals: Map[String, JsValue] = parameter.defaultValue match {
      case None => Map.empty
      case Some(wdlValue) =>
        val wvl = parameterLinkSerializer.createLink(parameter.dxType, wdlValue)
        parameterLinkSerializer.createFields(wvl, name).toMap
    }

    def jsMapFromDefault(name: String): Map[String, JsValue] = {
      defaultVals.get(name) match {
        case None      => Map.empty
        case Some(jsv) => Map("default" -> jsv)
      }
    }

    def jsMapFromOptional(optional: Boolean): Map[String, JsValue] = {
      if (optional) {
        Map("optional" -> JsBoolean(true))
      } else {
        Map.empty[String, JsValue]
      }
    }

    def mkPrimitive(dxType: String, optional: Boolean): Vector[JsValue] = {
      Vector(
          JsObject(
              Map("name" -> JsString(name), "class" -> JsString(dxType))
                ++ jsMapFromOptional(optional)
                ++ jsMapFromDefault(name)
                ++ jsMapFromAttrs(attrs, defaultVals.contains(name))
          )
      )
    }

    def mkPrimitiveArray(dxType: String, optional: Boolean): Vector[JsValue] = {
      Vector(
          JsObject(
              Map("name" -> JsString(name), "class" -> JsString("array:" ++ dxType))
                ++ jsMapFromOptional(optional)
                ++ jsMapFromDefault(name)
                ++ jsMapFromAttrs(attrs, defaultVals.contains(name))
          )
      )
    }

    def mkComplex(optional: Boolean): Vector[JsValue] = {
      // A large JSON structure passed as a hash, and a
      // vector of platform files.
      Vector(
          JsObject(
              Map("name" -> JsString(name), "class" -> JsString("hash"))
                ++ jsMapFromOptional(optional)
                ++ jsMapFromDefault(name)
                ++ jsMapFromAttrs(attrs, defaultVals.contains(name))
          ),
          JsObject(
              Map(
                  "name" -> JsString(name + ParameterLinkSerde.FlatFilesSuffix),
                  "class" -> JsString("array:file"),
                  "optional" -> JsBoolean(true)
              )
                ++ jsMapFromDefault(name + ParameterLinkSerde.FlatFilesSuffix)
                ++ jsMapFromAttrs(attrs, defaultVals.contains(name))
          )
      )
    }

    def handleType(t: Type, optional: Boolean): Vector[JsValue] = {
      wdlType match {
        // primitive types
        case TBoolean          => mkPrimitive("boolean", optional)
        case WdlTypes.T_Int    => mkPrimitive("int", optional)
        case WdlTypes.T_Float  => mkPrimitive("float", optional)
        case WdlTypes.T_String => mkPrimitive("string", optional)
        case WdlTypes.T_File   => mkPrimitive("file", optional)

        // single dimension arrays of primitive types
        // non-empty array
        case WdlTypes.T_Array(WdlTypes.T_Boolean, true) => mkPrimitiveArray("boolean", optional)
        case WdlTypes.T_Array(WdlTypes.T_Int, true)     => mkPrimitiveArray("int", optional)
        case WdlTypes.T_Array(WdlTypes.T_Float, true)   => mkPrimitiveArray("float", optional)
        case WdlTypes.T_Array(WdlTypes.T_String, true)  => mkPrimitiveArray("string", optional)
        case WdlTypes.T_Array(WdlTypes.T_File, true)    => mkPrimitiveArray("file", optional)

        // array that may be empty
        case WdlTypes.T_Array(WdlTypes.T_Boolean, false) =>
          mkPrimitiveArray("boolean", optional = true)
        case WdlTypes.T_Array(WdlTypes.T_Int, false) => mkPrimitiveArray("int", optional = true)
        case WdlTypes.T_Array(WdlTypes.T_Float, false) =>
          mkPrimitiveArray("float", optional = true)
        case WdlTypes.T_Array(WdlTypes.T_String, false) =>
          mkPrimitiveArray("string", optional = true)
        case WdlTypes.T_Array(WdlTypes.T_File, false) => mkPrimitiveArray("file", optional = true)

        // complex type, that may contains files
        case _ => mkComplex(optional)
      }
    }

    cVar.wdlType match {
      case t: WdlTypes.T_Optional =>
        handleType(Type.unwrapOptional(t), optional = true)
      case t =>
        handleType(t, optional = false)
    }
  }

  private def generateJobScript(applet: Application): String = {
    val templateAttrs = Map(
        "rtTraceLevel" -> runtimeTraceLevel,
        "streamAllFiles" -> runtimePathConfig.streamAllFiles
    )
    applet.kind match {
      case ExecutableKindApplet =>
        val template = applet.instanceType match {
          case DynamicInstanceType => DynamicAppletJobTemplate
          case _                   => StaticAppletJobTemplate
        }
        renderer.render(
            template,
            templateAttrs + (
                "dockerPreamble" -> dockerPreamble,
                "dxPathConfig" -> runtimePathConfig
            )
        )
      case _: ExecutableKindWfFragment =>
        renderer.render(
            WorkflowFragmentTempalate,
            templateAttrs
        )
      case other =>
        ExecutableKind.getCommand(other) match {
          case Some(command) =>
            renderer.render(
                CommandTempalate,
                templateAttrs + ("command" -> command)
            )
          case _ =>
            throw new RuntimeException(
                s"should not generate job script for kind ${other}"
            )
        }
    }
  }

  /**
    * Builds an '/applet/new' request.
    * For applets that call other applets, we pass a directory
    * of the callees, so they can be found at runtime.
    * @param applet applet IR
    * @param dependencyLinks
    * @return
    */
  def apply(
      applet: Application,
      dependencyLinks: Map[String, ExecutableLink]
  ): (Map[String, JsValue], String) = {
    logger.trace(s"Building /applet/new request for ${applet.name}")

    val inputSpec: Vector[JsValue] = applet.inputs
      .sortWith(_.name < _.name)
      .flatMap(cVar => cVarToSpec(cVar))

    // create linking information
    val linkInfo: Map[String, JsValue] =
      dependencyLinks.map {
        case (name, ali) =>
          name -> WdlExecutableLink.writeJson(ali, typeAliases)
      }

    val metaInfo: Map[String, JsValue] =
      applet.kind match {
        case IR.AppletKindWfFragment(_, blockPath, fqnDictTypes) =>
          // meta information used for running workflow fragments
          Map(
              "execLinkInfo" -> JsObject(linkInfo),
              "blockPath" -> JsArray(blockPath.map(JsNumber(_))),
              "fqnDictTypes" -> JsObject(fqnDictTypes.map {
                case (k, t) =>
                  val tStr = TypeSerialization(typeAliases).toString(t)
                  k -> JsString(tStr)
              })
          )

        case IR.AppletKindWfInputs | IR.AppletKindWfOutputs | IR.AppletKindWfCustomReorgOutputs |
            IR.AppletKindWorkflowOutputReorg =>
          // meta information used for running workflow fragments
          val fqnDictTypes = JsObject(applet.inputVars.map { cVar =>
            val tStr = TypeSerialization(typeAliases).toString(cVar.wdlType)
            cVar.name -> JsString(tStr)
          }.toMap)
          Map("fqnDictTypes" -> fqnDictTypes)

        case _ =>
          Map.empty
      }

    val outputSpec: Vector[JsValue] = applet.outputs
      .sortWith(_.name < _.name)
      .flatMap(cVar => cVarToSpec(cVar))

    // put the WDL source code into the details field.
    // Add the pricing model, and make the prices opaque.
    val generator = WdlV1Generator()
    val sourceLines = generator.generateDocument(applet.document)
    val sourceCode = CompressionUtils.gzipAndBase64Encode(sourceLines.mkString("\n"))
    val dbOpaque = InstanceTypeDbQuery(dxApi).opaquePrices(instanceTypeDB)
    val dbOpaqueInstance = CompressionUtils.gzipAndBase64Encode(dbOpaque.toJson.prettyPrint)
    val runtimeAttrs = extras match {
      case None      => JsNull
      case Some(ext) => ext.defaultRuntimeAttributes.toJson
    }

    val defaultTags = Vector(JsString("dxWDL"))
    val (taskMeta, taskDetails) = buildTaskMetadata(applet, defaultTags)

    // Compute all the bits that get merged together into 'details'
    val auxInfo = Map("wdlSourceCode" -> JsString(sourceCode),
                      "instanceTypeDB" -> JsString(dbOpaqueInstance),
                      "runtimeAttrs" -> runtimeAttrs)

    // Links to applets that could get called at runtime. If
    // this applet is copied, we need to maintain referential integrity.
    val dxLinks = dependencyLinks.map {
      case (name, execLinkInfo) =>
        ("link_" + name) -> JsObject(DxUtils.DxLinkKey -> JsString(execLinkInfo.dxExec.getId))
    }

    // Generate the applet's job script
    val jobScript = generateJobScript(applet)

    val (runSpec: JsValue, runSpecDetails: Map[String, JsValue]) =
      calcRunSpec(applet, jobScript)

    val delayWD: Map[String, JsValue] = extras match {
      case None => Map.empty
      case Some(ext) =>
        ext.delayWorkspaceDestruction match {
          case Some(true) => Map("delayWorkspaceDestruction" -> JsTrue)
          case _          => Map.empty
        }
    }

    // merge all the separate details maps
    val details: Map[String, JsValue] =
      taskDetails ++ auxInfo ++ dxLinks ++ metaInfo ++ runSpecDetails ++ delayWD

    val access: JsValue = calcAccess(applet)

    // A fragemnt is hidden, not visible under default settings. This
    // allows the workflow copying code to traverse it, and link to
    // anything it calls.
    val hidden: Boolean =
      applet.kind match {
        case _: IR.AppletKindWfFragment => true
        case _                          => false
      }

    // pack all the core arguments into a single request
    val reqCore = Map(
        "name" -> JsString(applet.name),
        "inputSpec" -> JsArray(inputSpec),
        "outputSpec" -> JsArray(outputSpec),
        "runSpec" -> runSpec,
        "dxapi" -> JsString("1.0.0"),
        "details" -> JsObject(details),
        "hidden" -> JsBoolean(hidden)
    )
    // look for ignoreReuse in runtime hints and in extras - the later overrides the former
    val ignoreReuseHint: Map[String, JsValue] = applet.runtimeHints match {
      case Some(hints) =>
        hints
          .flatMap {
            case IR.RuntimeHintIgnoreReuse(flag) => Some(Map("ignoreReuse" -> JsBoolean(flag)))
            case _                               => None
          }
          .headOption
          .getOrElse(Map.empty)
      case _ => Map.empty
    }
    val ignoreReuseExtras: Map[String, JsValue] = extras match {
      case None => Map.empty
      case Some(ext) =>
        ext.ignoreReuse match {
          case None       => Map.empty
          case Some(flag) => Map("ignoreReuse" -> JsBoolean(flag))
        }
    }
    val accessField =
      if (access == JsNull) Map.empty
      else Map("access" -> access)

    // Add a checksum
    val reqCoreAll = taskMeta ++ reqCore ++ accessField ++ ignoreReuseHint ++ ignoreReuseExtras
    checksumReq(applet.name, reqCoreAll)
  }
}
