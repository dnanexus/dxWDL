package dx.dxni.wdl

import dx.api._
import dx.core.languages.Language
import dx.core.languages.Language.Language
import dx.core.languages.wdl.{Utils, VersionSupport}
import dx.dxni.{NativeInterfaceGenerator, NativeInterfaceGeneratorFactory}
import wdlTools.syntax.{CommentMap, SourceLocation, WdlVersion}
import wdlTools.types.TypeCheckingRegime.TypeCheckingRegime
import wdlTools.types.WdlTypes.T_Task
import wdlTools.types.{TypeCheckingRegime, WdlTypes, TypedAbstractSyntax => TAT}
import wdlTools.util.{FileSourceResolver, Logger, StringFileSource}

case class WdlNativeInterfaceGenerator(wdlVersion: WdlVersion,
                                       fileResolver: FileSourceResolver = FileSourceResolver.get,
                                       regime: TypeCheckingRegime = TypeCheckingRegime.Moderate,
                                       dxApi: DxApi = DxApi.get,
                                       logger: Logger = Logger.get)
    extends NativeInterfaceGenerator {
  private lazy val wdl = VersionSupport(wdlVersion, fileResolver, regime, dxApi, logger)

  /**
    * Generate a WDL stub fore a DNAnexus applet.
    * @param id the applet ID
    * @param appletName the applet name
    * @param inputSpec the applet inputs
    * @param outputSpec the applet outputs
    * @return an AST.Task
    */
  private def createDnanexusAppletStub(
      id: String,
      appletName: String,
      inputSpec: Map[String, WdlTypes.T],
      outputSpec: Map[String, WdlTypes.T],
      loc: SourceLocation = Utils.locPlaceholder
  ): TAT.Task = {
    val meta = TAT.MetaSection(
        Map(
            "type" -> TAT.MetaValueString("native", loc),
            "id" -> TAT.MetaValueString(id, loc)
        ),
        loc
    )
    TAT.Task(
        appletName,
        T_Task(appletName, inputSpec.map {
          case (name, wdlType) => name -> (wdlType, false)
        }, outputSpec),
        inputSpec.map {
          case (name, wdlType) =>
            TAT.RequiredInputDefinition(name, wdlType, loc)
        }.toVector,
        outputSpec.map {
          case (name, wdlType) =>
            val expr = Utils.getDefaultValueOfType(wdlType)
            TAT.OutputDefinition(name, wdlType, expr, loc)
        }.toVector,
        TAT.CommandSection(Vector.empty, loc),
        Vector.empty,
        Some(meta),
        parameterMeta = None,
        runtime = None,
        hints = None,
        loc = loc
    )
  }

  private def wdlTypeFromDxClass(appletName: String,
                                 argName: String,
                                 ioClass: DxIOClass.Value,
                                 isOptional: Boolean): WdlTypes.T = {
    if (isOptional) {
      ioClass match {
        case DxIOClass.Boolean      => WdlTypes.T_Optional(WdlTypes.T_Boolean)
        case DxIOClass.Int          => WdlTypes.T_Optional(WdlTypes.T_Int)
        case DxIOClass.Float        => WdlTypes.T_Optional(WdlTypes.T_Float)
        case DxIOClass.String       => WdlTypes.T_Optional(WdlTypes.T_String)
        case DxIOClass.File         => WdlTypes.T_Optional(WdlTypes.T_File)
        case DxIOClass.BooleanArray => WdlTypes.T_Array(WdlTypes.T_Boolean, nonEmpty = false)
        case DxIOClass.IntArray     => WdlTypes.T_Array(WdlTypes.T_Int, nonEmpty = false)
        case DxIOClass.FloatArray   => WdlTypes.T_Array(WdlTypes.T_Float, nonEmpty = false)
        case DxIOClass.StringArray  => WdlTypes.T_Array(WdlTypes.T_String, nonEmpty = false)
        case DxIOClass.FileArray    => WdlTypes.T_Array(WdlTypes.T_File, nonEmpty = false)
        case _ =>
          throw new Exception(s"""|Cannot call applet ${appletName} from WDL, argument ${argName}
                                  |has IO class ${ioClass}""".stripMargin.replaceAll("\n", " "))
      }
    } else {
      ioClass match {
        case DxIOClass.Boolean      => WdlTypes.T_Boolean
        case DxIOClass.Int          => WdlTypes.T_Int
        case DxIOClass.Float        => WdlTypes.T_Float
        case DxIOClass.String       => WdlTypes.T_String
        case DxIOClass.File         => WdlTypes.T_File
        case DxIOClass.BooleanArray => WdlTypes.T_Array(WdlTypes.T_Boolean, nonEmpty = true)
        case DxIOClass.IntArray     => WdlTypes.T_Array(WdlTypes.T_Int, nonEmpty = true)
        case DxIOClass.FloatArray   => WdlTypes.T_Array(WdlTypes.T_Float, nonEmpty = true)
        case DxIOClass.StringArray  => WdlTypes.T_Array(WdlTypes.T_String, nonEmpty = true)
        case DxIOClass.FileArray    => WdlTypes.T_Array(WdlTypes.T_File, nonEmpty = true)
        case _ =>
          throw new Exception(s"""|Cannot call applet ${appletName} from WDL, argument ${argName}
                                  |has IO class ${ioClass}""".stripMargin.replaceAll("\n", " "))
      }
    }
  }

  private def appToWdlInterface(dxAppDesc: DxAppDescribe): Option[TAT.Task] = {
    val appName = dxAppDesc.name
    try {
      val inputSpec: Map[String, WdlTypes.T] =
        dxAppDesc.inputSpec.get.map { ioSpec =>
          ioSpec.name -> wdlTypeFromDxClass(dxAppDesc.name,
                                            ioSpec.name,
                                            ioSpec.ioClass,
                                            ioSpec.optional)
        }.toMap
      val outputSpec: Map[String, WdlTypes.T] =
        dxAppDesc.outputSpec.get.map { ioSpec =>
          ioSpec.name -> wdlTypeFromDxClass(dxAppDesc.name,
                                            ioSpec.name,
                                            ioSpec.ioClass,
                                            ioSpec.optional)
        }.toMap
      // DNAnexus applets allow the same variable name to be used for inputs and outputs.
      // This is illegal in WDL.
      val allInputNames = inputSpec.keys.toSet
      val allOutputNames = outputSpec.keys.toSet
      val both = allInputNames.intersect(allOutputNames)
      if (both.nonEmpty) {
        val bothStr = "[" + both.mkString(", ") + "]"
        throw new Exception(
            s"Parameters ${bothStr} used as both input and output in applet ${dxAppDesc.name}"
        )
      }
      Some(createDnanexusAppletStub(dxAppDesc.id, dxAppDesc.name, inputSpec, outputSpec))
    } catch {
      case e: Throwable =>
        dxApi.logger.warning(
            s"Unable to construct a WDL interface for app ${appName}",
            exception = Some(e)
        )
        None
    }
  }

  // Convert an applet to a WDL task with an empty body
  //
  // We can translate with primitive types, and their arrays. Hashes cannot
  // be translated; applets that have them cannot be converted.
  private def wdlTypesOfDxApplet(
      aplName: String,
      desc: DxAppletDescribe
  ): (Map[String, WdlTypes.T], Map[String, WdlTypes.T]) = {
    dxApi.logger.trace(s"analyzing applet ${aplName}")
    val inputSpec: Map[String, WdlTypes.T] =
      desc.inputSpec.get.map { iSpec =>
        iSpec.name -> wdlTypeFromDxClass(aplName, iSpec.name, iSpec.ioClass, iSpec.optional)
      }.toMap
    val outputSpec: Map[String, WdlTypes.T] =
      desc.outputSpec.get.map { iSpec =>
        iSpec.name -> wdlTypeFromDxClass(aplName, iSpec.name, iSpec.ioClass, iSpec.optional)
      }.toMap
    (inputSpec, outputSpec)
  }

  // Create a small WDL snippet that is a header for this applet
  private def appletToWdlInterface(dxAppletDesc: DxAppletDescribe): Option[TAT.Task] = {
    val appletName = dxAppletDesc.name
    try {
      val (inputSpec, outputSpec) = wdlTypesOfDxApplet(appletName, dxAppletDesc)
      // DNAx applets allow the same variable name to be used for inputs and outputs.
      // This is illegal in WDL.
      val allInputNames = inputSpec.keys.toSet
      val allOutputNames = outputSpec.keys.toSet
      val both = allInputNames.intersect(allOutputNames)
      if (both.nonEmpty) {
        val bothStr = "[" + both.mkString(", ") + "]"
        throw new Exception(
            s"""Parameters ${bothStr} used as both input and output in applet ${appletName}"""
        )
      }
      Some(createDnanexusAppletStub(dxAppletDesc.id, appletName, inputSpec, outputSpec))
    } catch {
      case e: Throwable =>
        dxApi.logger.warning(
            s"Unable to construct a WDL interface for applet ${appletName}",
            exception = Some(e)
        )
        None
    }
  }

  private def documentFromTasks(tasks: Vector[TAT.Task]): TAT.Document = {
    def createDocument(docTasks: Vector[TAT.Task]): TAT.Document = {
      TAT.Document(
          StringFileSource.empty,
          TAT.Version(wdl.version, SourceLocation.empty),
          docTasks,
          None,
          SourceLocation.empty,
          CommentMap.empty
      )
    }

    // uniquify and sort tasks
    val sortedUniqueTasks =
      tasks.map(t => t.name -> t).toMap.values.toVector.sortWith(_.name < _.name)
    // validate each task and warn if it doesn't generate valid WDL
    val validTasks = sortedUniqueTasks.flatMap { task =>
      try {
        // TODO: currently we always generate WDL 1.0 - other versions of the code generator
        //  need to be implemented in wdlTools
        val taskDoc = createDocument(Vector(task))
        val sourceCode = wdl.codeGenerator.generateDocument(taskDoc).mkString("\n")
        logger.ignore(Utils.parseSourceString(sourceCode, fileResolver, regime, logger))
        Some(task)
      } catch {
        case e: Throwable =>
          dxApi.logger.warning(s"Unable to construct a WDL interface for applet ${task.name}")
          dxApi.logger.warning(e.getMessage)
          None
      }
    }
    createDocument(validTasks)
  }

  override def generate(apps: Vector[DxApp],
                        applets: Vector[DxApplet],
                        headerLines: Vector[String]): Vector[String] = {
    val appTasks: Vector[TAT.Task] = apps.map(_.describe()).flatMap(appToWdlInterface)
    val appletTasks: Vector[TAT.Task] = applets.map(_.describe()).flatMap(appletToWdlInterface)
    val tasks: Vector[TAT.Task] = appTasks ++ appletTasks
    if (tasks.nonEmpty) {
      val doc = documentFromTasks(tasks)
      wdl.codeGenerator.generateDocument(doc)
    } else {
      Vector.empty
    }
  }
}

case class WdlDxNativeInterfaceFactory(fileResolver: FileSourceResolver = FileSourceResolver.get,
                                       dxApi: DxApi = DxApi.get,
                                       logger: Logger = Logger.get)
    extends NativeInterfaceGeneratorFactory {
  private def create(wdlVersion: WdlVersion): WdlNativeInterfaceGenerator = {
    wdlVersion match {
      case WdlVersion.Draft_2 =>
        Logger.get.warning("Upgrading draft-2 input to verion 1.0")
        WdlNativeInterfaceGenerator(WdlVersion.V1,
                                    fileResolver = fileResolver,
                                    dxApi = dxApi,
                                    logger = logger)
      case WdlVersion.V1 =>
        WdlNativeInterfaceGenerator(WdlVersion.V1,
                                    fileResolver = fileResolver,
                                    dxApi = dxApi,
                                    logger = logger)
      case _ =>
        throw new Exception(s"DxNI not supported for WDL version ${wdlVersion}")
    }
  }

  override def create(language: Language): Option[WdlNativeInterfaceGenerator] = {
    try {
      val wdlVersion = Language.toWdlVersion(language)
      Some(create(wdlVersion))
    } catch {
      case _: Throwable => None
    }
  }
}
