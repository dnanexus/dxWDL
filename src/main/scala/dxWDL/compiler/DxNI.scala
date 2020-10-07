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
package dxWDL.compiler

import com.dnanexus.DXAPI
import com.fasterxml.jackson.databind.JsonNode
import java.nio.file.{Files, Path}
import scala.util.matching.Regex
import spray.json._
import wom.types._

import dxWDL.base._
import dxWDL.dx._

case class DxNI(verbose: Verbose, language: Language.Value) {

  private def wdlTypeOfIOClass(appletName: String,
                               argName: String,
                               ioClass: DxIOClass.Value,
                               isOptional: Boolean): WomType = {
    if (isOptional) {
      ioClass match {
        case DxIOClass.BOOLEAN           => WomOptionalType(WomBooleanType)
        case DxIOClass.INT               => WomOptionalType(WomIntegerType)
        case DxIOClass.FLOAT             => WomOptionalType(WomFloatType)
        case DxIOClass.STRING            => WomOptionalType(WomStringType)
        case DxIOClass.FILE              => WomOptionalType(WomSingleFileType)
        case DxIOClass.ARRAY_OF_BOOLEANS => WomMaybeEmptyArrayType(WomBooleanType)
        case DxIOClass.ARRAY_OF_INTS     => WomMaybeEmptyArrayType(WomIntegerType)
        case DxIOClass.ARRAY_OF_FLOATS   => WomMaybeEmptyArrayType(WomFloatType)
        case DxIOClass.ARRAY_OF_STRINGS  => WomMaybeEmptyArrayType(WomStringType)
        case DxIOClass.ARRAY_OF_FILES    => WomMaybeEmptyArrayType(WomSingleFileType)
        case _ =>
          throw new Exception(s"""|Cannot call applet ${appletName} from WDL, argument ${argName}
                                  |has IO class ${ioClass}""".stripMargin.replaceAll("\n", " "))
      }
    } else {
      ioClass match {
        case DxIOClass.BOOLEAN           => WomBooleanType
        case DxIOClass.INT               => WomIntegerType
        case DxIOClass.FLOAT             => WomFloatType
        case DxIOClass.STRING            => WomStringType
        case DxIOClass.FILE              => WomSingleFileType
        case DxIOClass.ARRAY_OF_BOOLEANS => WomNonEmptyArrayType(WomBooleanType)
        case DxIOClass.ARRAY_OF_INTS     => WomNonEmptyArrayType(WomIntegerType)
        case DxIOClass.ARRAY_OF_FLOATS   => WomNonEmptyArrayType(WomFloatType)
        case DxIOClass.ARRAY_OF_STRINGS  => WomNonEmptyArrayType(WomStringType)
        case DxIOClass.ARRAY_OF_FILES    => WomNonEmptyArrayType(WomSingleFileType)
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
  ): (Map[String, WomType], Map[String, WomType]) = {
    Utils.trace(verbose.on, s"analyzing applet ${aplName}")
    val inputSpec: Map[String, WomType] =
      desc.inputSpec.get.map { iSpec =>
        iSpec.name -> wdlTypeOfIOClass(aplName, iSpec.name, iSpec.ioClass, iSpec.optional)
      }.toMap
    val outputSpec: Map[String, WomType] =
      desc.outputSpec.get.map { iSpec =>
        iSpec.name -> wdlTypeOfIOClass(aplName, iSpec.name, iSpec.ioClass, iSpec.optional)
      }.toMap
    (inputSpec, outputSpec)
  }

  // Create a small WDL snippet that is a header for this applet
  private def createAppletWdlHeader(desc: DxAppletDescribe): Option[String] = {
    val aplName = desc.name
    try {
      val (inputSpec, outputSpec) = wdlTypesOfDxApplet(aplName, desc)
      // DNAx applets allow the same variable name to be used for inputs and outputs.
      // This is illegal in WDL.
      val allInputNames = inputSpec.keys.toSet
      val allOutputNames = outputSpec.keys.toSet
      val both = allInputNames.intersect(allOutputNames)
      if (!both.isEmpty) {
        val bothStr = "[" + both.mkString(", ") + "]"
        throw new Exception(
            s"""Parameters ${bothStr} used as both input and output in applet ${aplName}"""
        )
      }
      val WdlCodeSnippet(taskCode) =
        WdlCodeGen(verbose, Map.empty, language)
          .genDnanexusAppletStub(desc.id, aplName, inputSpec, outputSpec)
      Some(taskCode)
    } catch {
      case e: Throwable =>
        Utils.warning(verbose, s"Unable to construct a WDL interface for applet ${aplName}")
        Utils.warning(verbose, e.getMessage)
        None
    }
  }

  // Search a platform path for all applets in it. Use
  // one API call for efficiency. Return a list of tasks, and their
  // applet-ids.
  //
  // If the folder is not a valid path, an empty list will be returned.
  private def search(dxProject: DxProject, folder: String, recursive: Boolean): Vector[String] = {
    val dxObjectsInFolder: Map[DxDataObject, DxObjectDescribe] =
      DxFindDataObjects(None, verbose)
        .apply(Some(dxProject),
               Some(folder),
               recursive,
               None,
               Vector.empty,
               Vector.empty,
               true,
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
            props.get(Utils.CHECKSUM_PROP) match {
              case Some(_) => None
              case None    => Some(apl -> desc)
            }
        }
    }.toMap

    nativeApplets
      .map { case (_, desc) => createAppletWdlHeader(desc) }
      .flatten
      .toVector
  }

  private def isWdl(properties: Option[Map[String, String]]): Boolean = {
    properties match {
      case None => false
      case Some(props) =>
        props.get(Utils.CHECKSUM_PROP) match {
          case Some(_) => true
          case None    => false
        }
    }
  }

  private def path(dxProject: DxProject, path: String): Option[String] = {
    val dxObj = path match {
      case id if path.startsWith("app-")  => DxApp.getInstance(id)
      case id if id.startsWith("applet-") => DxApplet.getInstance(id)
      case _ =>
        val fullPath = Utils.DX_URL_PREFIX + "/" + path
        DxPath.resolveOnePath(fullPath, dxProject)
    }
    dxObj match {
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
      case Some(JsArray(x)) => x.toVector
      case other            => throw new Exception(s"malformed field ${fieldName} (${other})")
    }
  }

  private def checkedGetIoSpec(appName: String, jsv: JsValue): IOParameter = {
    val ioParam = DxObject.parseIoParam(jsv)
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
      case '_'                       => "_"
      case _ if (ch.isLetterOrDigit) => ch.toString
      case _                         => "_"
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
          Utils.warning(
              verbose,
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
    }.toVector
    val outputSpec = outputSpecJs.map { x =>
      checkedGetIoSpec(name, x)
    }.toVector
    val normName = normalizeAppName(name)
    DxAppDescribe(id, normName, 0, 0, None, None, Some(inputSpec), Some(outputSpec))
  }

  private def appToWdlInterface(dxApp: DxAppDescribe): String = {
    val inputSpec: Map[String, WomType] =
      dxApp.inputSpec.get.map { ioSpec =>
        ioSpec.name -> wdlTypeOfIOClass(dxApp.name, ioSpec.name, ioSpec.ioClass, ioSpec.optional)
      }.toMap
    val outputSpec: Map[String, WomType] =
      dxApp.outputSpec.get.map { ioSpec =>
        ioSpec.name -> wdlTypeOfIOClass(dxApp.name, ioSpec.name, ioSpec.ioClass, ioSpec.optional)
      }.toMap

    // DNAx applets allow the same variable name to be used for inputs and outputs.
    // This is illegal in WDL.
    val allInputNames = inputSpec.keys.toSet
    val allOutputNames = outputSpec.keys.toSet
    val both = allInputNames.intersect(allOutputNames)
    if (!both.isEmpty) {
      val bothStr = "[" + both.mkString(", ") + "]"
      throw new Exception(
          s"""|Parameters ${bothStr} used as both input and
              |output in applet ${dxApp.name}""".stripMargin
            .replaceAll("\n", " ")
      )
    }
    val WdlCodeSnippet(taskCode) =
      WdlCodeGen(verbose, Map.empty, language)
        .genDnanexusAppletStub(dxApp.id, dxApp.name, inputSpec, outputSpec)
    taskCode
  }

  // Search for global apps
  def searchApps: Vector[String] = {
    val req = JsObject(
        "published" -> JsBoolean(true),
        "describe" -> JsObject(
            "fields" -> JsObject("name" -> JsTrue, "inputSpec" -> JsTrue, "outputSpec" -> JsTrue)
        ),
        "limit" -> JsNumber(1000)
    )
    val rep = DXAPI.systemFindApps(DxUtils.jsonNodeOfJsValue(req), classOf[JsonNode])
    val repJs: JsValue = DxUtils.jsValueOfJsonNode(rep)
    val appsJs = repJs.asJsObject.fields.get("results") match {
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
          Utils.warning(verbose, s"Unable to construct a WDL interface for applet ${appName}")
          Utils.warning(verbose, e.getMessage)
          None
      }
    }
    taskHeaders.toVector
  }
}

object DxNI {
  private def writeHeadersToFile(header: String,
                                 tasks: Vector[String],
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
      outputPath.toFile().delete
    }

    // pretty print into a buffer
    val lines = tasks.mkString("\n\n")
    val allLines = header + "\n" + lines
    Utils.writeFileContent(outputPath, allLines)
  }

  // create headers for calling dx:applets and dx:workflows
  // We assume the folder is valid.
  def apply(dxProject: DxProject,
            folderOrPath: Either[String, String],
            output: Path,
            recursive: Boolean,
            force: Boolean,
            language: Language.Value,
            verbose: Verbose): Unit = {
    val dxni = new DxNI(verbose, language)
    val dxNativeTasks: Vector[String] = folderOrPath match {
      case Left(folder) => dxni.search(dxProject, folder, recursive)
      case Right(path)  => dxni.path(dxProject, path).toVector
    }

    val folderOrPathRepr = folderOrPath match {
      case Left(folder) => s"folder = ${folder}"
      case Right(path)  => s"path = ${path}"
    }
    if (dxNativeTasks.isEmpty) {
      Utils.warning(verbose, s"Found no DX native applets in ${folderOrPathRepr}")
      return
    }
    val projName = dxProject.describe().name

    // add comment describing how the file was created
    val languageHeader = new WdlCodeGen(verbose, Map.empty, language).versionString()
    val header =
      s"""|# This file was generated by the Dx Native Interface (DxNI) tool.
          |# project name = ${projName}
          |# project ID = ${dxProject.getId}
          |# folder = ${folderOrPathRepr}
          |
          |${languageHeader}
          |""".stripMargin

    writeHeadersToFile(header, dxNativeTasks, output, force)
  }

  def applyApps(output: Path, force: Boolean, language: Language.Value, verbose: Verbose): Unit = {
    val dxni = new DxNI(verbose, language)
    val dxAppsAsTasks: Vector[String] = dxni.searchApps
    if (dxAppsAsTasks.isEmpty) {
      Utils.warning(verbose, s"Found no DX global apps")
      return
    }

    // If there are many apps, we might end up with multiple definitions of
    // the same task. This gets rid of duplicates.
    val uniqueTasks = dxAppsAsTasks.toSet.toVector

    // add comment describing how the file was created
    val languageHeader = new WdlCodeGen(verbose, Map.empty, language).versionString()
    val header =
      s"""|# This file was generated by the Dx Native Interface (DxNI) tool.
          |# These are interfaces to apps.
          |#
          |${languageHeader}
          |""".stripMargin

    writeHeadersToFile(header, uniqueTasks, output, force)
  }

}
