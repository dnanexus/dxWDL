package dx.compiler.wdl

import dx.api.{DxApi, DxApp, DxAppDescribe, DxApplet, DxAppletDescribe, DxIOClass}
import dx.compiler.{DxNativeInterface, WdlCodeGen}
import dx.core.languages.Language
import dx.core.languages.wdl.ParseSource
import wdlTools.syntax.{CommentMap, SourceLocation}
import wdlTools.types.{WdlTypes, TypedAbstractSyntax => TAT}
import wdlTools.util.StringFileSource

class WdlDxNativeInterface(dxApi: DxApi, language: Language.Value)
    extends DxNativeInterface(dxApi) {
  private val codeGen = WdlCodeGen(dxApi.logger, Map.empty, language)

  private def documentFromTasks(tasks: Vector[TAT.Task]): TAT.Document = {
    def createDocument(docTasks: Vector[TAT.Task]): TAT.Document = {
      TAT.Document(
          StringFileSource.empty,
          TAT.Version(codeGen.wdlVersion, SourceLocation.empty),
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
    val parser = ParseSource(dxApi)
    val validTasks = sortedUniqueTasks.flatMap { task =>
      try {
        // TODO: currently we always generate WDL 1.0 - other versions of the code generator
        //  need to be implemented in wdlTools
        val taskDoc = createDocument(Vector(task))
        val sourceCode = codeGen.generateDocument(taskDoc)
        parser.validateWdlCode(sourceCode)
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

  private def wdlTypeOfIOClass(appletName: String,
                               argName: String,
                               ioClass: DxIOClass.Value,
                               isOptional: Boolean): WdlTypes.T = {
    if (isOptional) {
      ioClass match {
        case DxIOClass.BOOLEAN           => WdlTypes.T_Optional(WdlTypes.T_Boolean)
        case DxIOClass.INT               => WdlTypes.T_Optional(WdlTypes.T_Int)
        case DxIOClass.FLOAT             => WdlTypes.T_Optional(WdlTypes.T_Float)
        case DxIOClass.STRING            => WdlTypes.T_Optional(WdlTypes.T_String)
        case DxIOClass.FILE              => WdlTypes.T_Optional(WdlTypes.T_File)
        case DxIOClass.ARRAY_OF_BOOLEANS => WdlTypes.T_Array(WdlTypes.T_Boolean, nonEmpty = false)
        case DxIOClass.ARRAY_OF_INTS     => WdlTypes.T_Array(WdlTypes.T_Int, nonEmpty = false)
        case DxIOClass.ARRAY_OF_FLOATS   => WdlTypes.T_Array(WdlTypes.T_Float, nonEmpty = false)
        case DxIOClass.ARRAY_OF_STRINGS  => WdlTypes.T_Array(WdlTypes.T_String, nonEmpty = false)
        case DxIOClass.ARRAY_OF_FILES    => WdlTypes.T_Array(WdlTypes.T_File, nonEmpty = false)
        case _ =>
          throw new Exception(s"""|Cannot call applet ${appletName} from WDL, argument ${argName}
                                  |has IO class ${ioClass}""".stripMargin.replaceAll("\n", " "))
      }
    } else {
      ioClass match {
        case DxIOClass.BOOLEAN           => WdlTypes.T_Boolean
        case DxIOClass.INT               => WdlTypes.T_Int
        case DxIOClass.FLOAT             => WdlTypes.T_Float
        case DxIOClass.STRING            => WdlTypes.T_String
        case DxIOClass.FILE              => WdlTypes.T_File
        case DxIOClass.ARRAY_OF_BOOLEANS => WdlTypes.T_Array(WdlTypes.T_Boolean, nonEmpty = true)
        case DxIOClass.ARRAY_OF_INTS     => WdlTypes.T_Array(WdlTypes.T_Int, nonEmpty = true)
        case DxIOClass.ARRAY_OF_FLOATS   => WdlTypes.T_Array(WdlTypes.T_Float, nonEmpty = true)
        case DxIOClass.ARRAY_OF_STRINGS  => WdlTypes.T_Array(WdlTypes.T_String, nonEmpty = true)
        case DxIOClass.ARRAY_OF_FILES    => WdlTypes.T_Array(WdlTypes.T_File, nonEmpty = true)
        case _ =>
          throw new Exception(s"""|Cannot call applet ${appletName} from WDL, argument ${argName}
                                  |has IO class ${ioClass}""".stripMargin.replaceAll("\n", " "))
      }
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
        iSpec.name -> wdlTypeOfIOClass(aplName, iSpec.name, iSpec.ioClass, iSpec.optional)
      }.toMap
    val outputSpec: Map[String, WdlTypes.T] =
      desc.outputSpec.get.map { iSpec =>
        iSpec.name -> wdlTypeOfIOClass(aplName, iSpec.name, iSpec.ioClass, iSpec.optional)
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
      Some(codeGen.genDnanexusAppletStub(dxAppletDesc.id, appletName, inputSpec, outputSpec))
    } catch {
      case e: Throwable =>
        dxApi.logger.warning(
            s"Unable to construct a WDL interface for applet ${appletName}",
            exception = Some(e)
        )
        None
    }
  }

  override def generateApplets(applets: Vector[DxApplet], headerLines: Vector[String]): String = {
    val tasks = applets.map(_.describe()).flatMap(appletToWdlInterface)
    val doc = if (tasks.nonEmpty) {
      Some(documentFromTasks(tasks))
    } else {
      None
    }
  }

  private def appToWdlInterface(dxAppDesc: DxAppDescribe): Option[TAT.Task] = {
    val appName = dxAppDesc.name
    try {
      val inputSpec: Map[String, WdlTypes.T] =
        dxAppDesc.inputSpec.get.map { ioSpec =>
          ioSpec.name -> wdlTypeOfIOClass(dxAppDesc.name,
                                          ioSpec.name,
                                          ioSpec.ioClass,
                                          ioSpec.optional)
        }.toMap
      val outputSpec: Map[String, WdlTypes.T] =
        dxAppDesc.outputSpec.get.map { ioSpec =>
          ioSpec.name -> wdlTypeOfIOClass(dxAppDesc.name,
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
            s"""|Parameters ${bothStr} used as both input and
                |output in applet ${dxAppDesc.name}""".stripMargin
              .replaceAll("\n", " ")
        )
      }
      Some(codeGen.genDnanexusAppletStub(dxAppDesc.id, dxAppDesc.name, inputSpec, outputSpec))
    } catch {
      case e: Throwable =>
        dxApi.logger.warning(
            s"Unable to construct a WDL interface for app ${appName}",
            exception = Some(e)
        )
        None
    }
  }

  override def generateApps(apps: Vector[DxApp], headerLines: Vector[String]): String = {
    val tasks = apps.flatmap(appToWdlInterface)
    if (tasks.nonEmpty) {
      Some(documentFromTasks(tasks))
    } else {
      None
    }
  }
}
