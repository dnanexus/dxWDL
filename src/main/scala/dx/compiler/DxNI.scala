/**
 Enable calling native dx:applets. They are represented as empty
 tasks, and it is possible to call them at runtime from the WDL
 workflow. dx:workflows will be supported in the future.

 For example, an applet with a signature like:

{
  "name": "mk_int_list",
  "inputSpec": [
    {
      "name": "a",
      "class": "int"
    },
    {
      "name": "b",
      "class": "int"
    }
  ],
  "outputSpec": [
    {
      "name": "all",
      "class": "array:int"
      }
  ]
}

Is represented as:

task mk_int_list {
  Int a
  Int b
  command {}
  output {
    Array[Int] all = []
  }
  meta {
    type: native
    applet_id: applet-xxxx
  }
}

  */
package dx.compiler

import java.nio.file.{Files, Path}

import dx.api._
import dx.core.languages.Language
import dx.core.languages.wdl.ParseSource
import dx.core.getVersion
import spray.json._
import wdlTools.generators.code.WdlV1Generator
import wdlTools.syntax.{CommentMap, SourceLocation}
import wdlTools.types.{WdlTypes, TypedAbstractSyntax => TAT}
import wdlTools.util.{StringFileSource, Util}

import scala.util.matching.Regex

case class DxNI(dxApi: DxApi, language: Language.Value) {
  private val codeGen = WdlCodeGen(dxApi.logger, Map.empty, language)

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
  private def createAppletWdlHeader(desc: DxAppletDescribe): Option[TAT.Task] = {
    val aplName = desc.name
    try {
      val (inputSpec, outputSpec) = wdlTypesOfDxApplet(aplName, desc)
      // DNAx applets allow the same variable name to be used for inputs and outputs.
      // This is illegal in WDL.
      val allInputNames = inputSpec.keys.toSet
      val allOutputNames = outputSpec.keys.toSet
      val both = allInputNames.intersect(allOutputNames)
      if (both.nonEmpty) {
        val bothStr = "[" + both.mkString(", ") + "]"
        throw new Exception(
            s"""Parameters ${bothStr} used as both input and output in applet ${aplName}"""
        )
      }
      Some(codeGen.genDnanexusAppletStub(desc.id, aplName, inputSpec, outputSpec))
    } catch {
      case e: Throwable =>
        dxApi.logger.warning(s"Unable to construct a WDL interface for applet ${aplName}")
        dxApi.logger.warning(e.getMessage)
        None
    }
  }

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

  // Search a platform path for all applets in it. Use
  // one API call for efficiency. Return a list of tasks, and their
  // applet-ids.
  //
  // If the folder is not a valid path, an empty list will be returned.
  private def search(dxProject: DxProject,
                     folder: String,
                     recursive: Boolean): Option[TAT.Document] = {
    val dxObjectsInFolder: Map[DxDataObject, DxObjectDescribe] =
      DxFindDataObjects(dxApi, None)
        .apply(Some(dxProject),
               Some(folder),
               recursive,
               None,
               Vector.empty,
               Vector.empty,
               withInputOutputSpec = true,
               Vector.empty,
               Set.empty)

    // we just want the applets
    val dxAppletsInFolder: Map[DxApplet, DxAppletDescribe] = dxObjectsInFolder.collect {
      case (dxobj, desc) if dxobj.isInstanceOf[DxApplet] =>
        (dxobj.asInstanceOf[DxApplet], desc.asInstanceOf[DxAppletDescribe])
    }

    // Filter out applets that are WDL tasks
    val nativeApplets: Map[DxApplet, DxAppletDescribe] = dxAppletsInFolder.flatMap {
      case (apl, desc) =>
        desc.properties match {
          case None => None
          case Some(props) =>
            props.get(CHECKSUM_PROP) match {
              case Some(_) => None
              case None    => Some(apl -> desc)
            }
        }
    }

    val tasks = nativeApplets.values.flatMap(createAppletWdlHeader).toVector
    if (tasks.nonEmpty) {
      Some(documentFromTasks(tasks))
    } else {
      None
    }
  }

  private def isWdl(properties: Option[Map[String, String]]): Boolean = {
    properties match {
      case None => false
      case Some(props) =>
        props.get(CHECKSUM_PROP) match {
          case Some(_) => true
          case None    => false
        }
    }
  }

  private def path(dxProject: DxProject, path: String): Option[TAT.Document] = {
    val dxObj = path match {
      case id if path.startsWith("app-")  => dxApi.app(id)
      case id if id.startsWith("applet-") => dxApi.applet(id)
      case _ =>
        val fullPath = s"${DxPath.DX_URL_PREFIX}/${path}"
        dxApi.resolveOnePath(fullPath, dxProject)
    }
    val task: Option[TAT.Task] = dxObj match {
      // an applet
      case applet: DxApplet =>
        val desc = applet.describe(Set(Field.Properties))
        if (isWdl(desc.properties))
          None
        else
          createAppletWdlHeader(desc)

      case app: DxApp =>
        // an app
        val desc = app.describe(Set(Field.Properties))
        if (isWdl(desc.properties))
          None
        else
          Some(appToWdlInterface(desc))

      case _ => None
    }
    task.map(t => documentFromTasks(Vector(t)))
  }

  private def checkedGetJsString(jsv: JsValue, fieldName: String): String = {
    val fields = jsv.asJsObject.fields
    fields.get(fieldName) match {
      case Some(JsString(x)) => x
      case other             => throw new Exception(s"malformed field ${fieldName} (${other})")
    }
  }

  private def checkedGetJsObject(jsv: JsValue, fieldName: String): JsObject = {
    val fields = jsv.asJsObject.fields
    fields.get(fieldName) match {
      case Some(JsObject(x)) => JsObject(x)
      case other             => throw new Exception(s"malformed field ${fieldName} (${other})")
    }
  }

  private def checkedGetJsArray(jsv: JsValue, fieldName: String): Vector[JsValue] = {
    val fields = jsv.asJsObject.fields
    fields.get(fieldName) match {
      case Some(JsArray(x)) => x
      case other            => throw new Exception(s"malformed field ${fieldName} (${other})")
    }
  }

  private def checkedGetIoSpec(appName: String, jsv: JsValue): IOParameter = {
    val ioParam = IOParameter.parseIoParam(dxApi, jsv)
    if (ioParam.ioClass == DxIOClass.HASH)
      throw new Exception(
          s"""|app ${appName} has field ${ioParam.name}
              |with non WDL-native io class HASH""".stripMargin
            .replaceAll("\n", " ")
      )
    ioParam
  }

  // App names can have characters that are illegal in WDL.
  // 1) Leave only number, letters, and underscores
  // 2) If the name starts with a number, add the prefix "app_"
  private val taskNameRegex: Regex = raw"""[a-zA-Z][a-zA-Z0-9_.]*""".r
  private def normalizeAppName(name: String): String = {
    def sanitizeChar(ch: Char): String = ch match {
      case '_'                     => "_"
      case _ if ch.isLetterOrDigit => ch.toString
      case _                       => "_"
    }
    name match {
      case taskNameRegex(_*) =>
        // legal in WDL
        name
      case _ =>
        // remove all illegal characeters
        val nameClean = name.flatMap(sanitizeChar)

        // add a prefix if needed
        val prefix = nameClean(0) match {
          case x if x.isLetter => ""
          case '_'             => "app"
          case _               => "app_"
        }
        if (!prefix.isEmpty)
          dxApi.logger.warning(
              s"""|app ${nameClean} does not start
                  |with a letter, adding the prefix '${prefix}'""".stripMargin
                .replaceAll("\n", " ")
          )
        s"${prefix}${nameClean}"
    }
  }

  private def checkedGetApp(jsv: JsValue): DxAppDescribe = {
    val id: String = checkedGetJsString(jsv, "id")
    val desc: JsObject = checkedGetJsObject(jsv, "describe")
    val name: String = checkedGetJsString(desc, "name")
    val inputSpecJs = checkedGetJsArray(desc, "inputSpec")
    val outputSpecJs = checkedGetJsArray(desc, "outputSpec")

    val inputSpec = inputSpecJs.map { x =>
      checkedGetIoSpec(name, x)
    }
    val outputSpec = outputSpecJs.map { x =>
      checkedGetIoSpec(name, x)
    }
    val normName = normalizeAppName(name)
    DxAppDescribe(id, normName, 0, 0, None, None, Some(inputSpec), Some(outputSpec))
  }

  private def appToWdlInterface(dxApp: DxAppDescribe): TAT.Task = {
    val inputSpec: Map[String, WdlTypes.T] =
      dxApp.inputSpec.get.map { ioSpec =>
        ioSpec.name -> wdlTypeOfIOClass(dxApp.name, ioSpec.name, ioSpec.ioClass, ioSpec.optional)
      }.toMap
    val outputSpec: Map[String, WdlTypes.T] =
      dxApp.outputSpec.get.map { ioSpec =>
        ioSpec.name -> wdlTypeOfIOClass(dxApp.name, ioSpec.name, ioSpec.ioClass, ioSpec.optional)
      }.toMap

    // DNAx applets allow the same variable name to be used for inputs and outputs.
    // This is illegal in WDL.
    val allInputNames = inputSpec.keys.toSet
    val allOutputNames = outputSpec.keys.toSet
    val both = allInputNames.intersect(allOutputNames)
    if (both.nonEmpty) {
      val bothStr = "[" + both.mkString(", ") + "]"
      throw new Exception(
          s"""|Parameters ${bothStr} used as both input and
              |output in applet ${dxApp.name}""".stripMargin
            .replaceAll("\n", " ")
      )
    }
    codeGen.genDnanexusAppletStub(dxApp.id, dxApp.name, inputSpec, outputSpec)
  }

  // Search for global apps
  def searchApps: Option[TAT.Document] = {
    val req = Map(
        "published" -> JsBoolean(true),
        "describe" -> JsObject("inputSpec" -> JsBoolean(true), "outputSpec" -> JsBoolean(true)),
        "limit" -> JsNumber(1000)
    )
    val repJs = dxApi.findApps(req)
    val appsJs = repJs.fields.get("results") match {
      case Some(JsArray(apps)) => apps
      case _                   => throw new Exception(s"""|malformed reply to findApps
                                        |
                                        |${repJs}""".stripMargin)
    }
    if (appsJs.length == 1000)
      throw new Exception("There are probably more than 1000 accessible apps")

    val taskHeaders = appsJs.flatMap { jsv =>
      val desc: JsObject = checkedGetJsObject(jsv, "describe")
      val appName = checkedGetJsString(desc, "name")
      try {
        val dxApp = checkedGetApp(jsv)
        Some(appToWdlInterface(dxApp))
      } catch {
        case e: Throwable =>
          dxApi.logger.warning(s"Unable to construct a WDL interface for applet ${appName}")
          dxApi.logger.warning(e.getMessage)
          None
      }
    }

    if (taskHeaders.nonEmpty) {
      Some(documentFromTasks(taskHeaders))
    } else {
      None
    }
  }
}

object DxNI {
  private def writeHeadersToFile(header: Vector[String],
                                 element: TAT.Document,
                                 outputPath: Path,
                                 force: Boolean): Unit = {
    if (Files.exists(outputPath)) {
      if (!force) {
        throw new Exception(
            s"""|Output file ${outputPath.toString} already exists,
                |use -force to overwrite it""".stripMargin
              .replaceAll("\n", " ")
        )
      }
      outputPath.toFile.delete
    }
    val generator = WdlV1Generator()
    val lines = generator.generateDocument(element, header)
    Util.writeFileContent(outputPath, lines.mkString("\n"))
  }

  // create headers for calling dx:applets and dx:workflows
  // We assume the folder is valid.
  def apply(dxProject: DxProject,
            folderOrPath: Either[String, String],
            output: Path,
            recursive: Boolean,
            force: Boolean,
            language: Language.Value,
            dxApi: DxApi): Unit = {
    val dxni = new DxNI(dxApi, language)
    val dxNativeTasks: Option[TAT.Document] = folderOrPath match {
      case Left(folder) => dxni.search(dxProject, folder, recursive)
      case Right(path)  => dxni.path(dxProject, path)
    }
    val folderOrPathRepr = folderOrPath match {
      case Left(folder) => s"folder = ${folder}"
      case Right(path)  => s"path = ${path}"
    }
    if (dxNativeTasks.isEmpty) {
      dxApi.logger.warning(s"Found no DX native applets in ${folderOrPathRepr}")
      return
    }

    val projName = dxProject.describe().name

    // add comment describing how the file was created
    val headerLines = Vector(
        s"This file was generated by the Dx Native Interface (DxNI) tool ${getVersion}.",
        s"project name = ${projName}",
        s"project ID = ${dxProject.getId}",
        s"folder = ${folderOrPathRepr}"
    )

    writeHeadersToFile(headerLines, dxNativeTasks.get, output, force)
  }

  def applyApps(output: Path, force: Boolean, language: Language.Value, dxApi: DxApi): Unit = {
    val dxni = new DxNI(dxApi, language)
    val dxAppsAsTasks: Option[TAT.Document] = dxni.searchApps
    if (dxAppsAsTasks.isEmpty) {
      dxApi.logger.warning(s"Found no DX global apps")
      return
    }

    // add comment describing how the file was created
    val header = Vector(
        s"This file was generated by the Dx Native Interface (DxNI) tool ${getVersion}.",
        "These are interfaces to apps."
    )

    writeHeadersToFile(header, dxAppsAsTasks.get, output, force)
  }
}
