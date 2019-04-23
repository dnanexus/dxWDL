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

import com.dnanexus._
import com.fasterxml.jackson.databind.JsonNode
import java.nio.file.{Files, Path}
import scala.collection.JavaConverters._
import scala.util.matching.Regex
import spray.json._
import wom.types._

import dxWDL.util._

case class IoSpec(name: String,
                  ioClass: IOClass,
                  optional: Boolean)

case class DxApp(name: String,
                 id: String,
                 inputSpec: Map[String, IoSpec],
                 outputSpec: Map[String, IoSpec])

case class DxNI(verbose: Verbose,
                language: Language.Value) {

    private def wdlTypeOfIOClass(appletName:String,
                                 argName: String,
                                 ioClass: IOClass,
                                 isOptional: Boolean) : WomType = {
        if (isOptional) {
            ioClass match {
            case IOClass.BOOLEAN => WomOptionalType(WomBooleanType)
            case IOClass.INT => WomOptionalType(WomIntegerType)
            case IOClass.FLOAT => WomOptionalType(WomFloatType)
            case IOClass.STRING => WomOptionalType(WomStringType)
            case IOClass.FILE => WomOptionalType(WomSingleFileType)
            case IOClass.ARRAY_OF_BOOLEANS => WomMaybeEmptyArrayType(WomBooleanType)
            case IOClass.ARRAY_OF_INTS => WomMaybeEmptyArrayType(WomIntegerType)
            case IOClass.ARRAY_OF_FLOATS => WomMaybeEmptyArrayType(WomFloatType)
            case IOClass.ARRAY_OF_STRINGS => WomMaybeEmptyArrayType(WomStringType)
            case IOClass.ARRAY_OF_FILES => WomMaybeEmptyArrayType(WomSingleFileType)
            case _ => throw new Exception(
                s"""|Cannot call applet ${appletName} from WDL, argument ${argName}
                    |has IO class ${ioClass}""".stripMargin.replaceAll("\n", " "))
            }
        } else {
            ioClass match {
                case IOClass.BOOLEAN => WomBooleanType
                case IOClass.INT => WomIntegerType
                case IOClass.FLOAT => WomFloatType
                case IOClass.STRING => WomStringType
                case IOClass.FILE => WomSingleFileType
                case IOClass.ARRAY_OF_BOOLEANS => WomNonEmptyArrayType(WomBooleanType)
                case IOClass.ARRAY_OF_INTS => WomNonEmptyArrayType(WomIntegerType)
                case IOClass.ARRAY_OF_FLOATS => WomNonEmptyArrayType(WomFloatType)
                case IOClass.ARRAY_OF_STRINGS => WomNonEmptyArrayType(WomStringType)
                case IOClass.ARRAY_OF_FILES => WomNonEmptyArrayType(WomSingleFileType)
                case _ => throw new Exception(
                    s"""|Cannot call applet ${appletName} from WDL, argument ${argName}
                        |has IO class ${ioClass}""".stripMargin.replaceAll("\n", " "))
            }
        }
    }

    // Convert an applet to a WDL task with an empty body
    //
    // We can translate with primitive types, and their arrays. Hashes cannot
    // be translated; applets that have them cannot be converted.
    private def wdlTypesOfDxApplet(aplName: String,
                                   desc: DXApplet.Describe) :
            (Map[String, WomType], Map[String, WomType]) = {
        Utils.trace(verbose.on, s"analyzing applet ${aplName}")
        val inputSpecRaw: List[InputParameter] = desc.getInputSpecification().asScala.toList
        val inputSpec:Map[String, WomType] =
            inputSpecRaw.map{ iSpec =>
                iSpec.getName -> wdlTypeOfIOClass(aplName, iSpec.getName,
                                                  iSpec.getIOClass, iSpec.isOptional)
            }.toMap
        val outputSpecRaw: List[OutputParameter] = desc.getOutputSpecification().asScala.toList
        val outputSpec:Map[String, WomType] =
            outputSpecRaw.map{ iSpec =>
                iSpec.getName -> wdlTypeOfIOClass(aplName, iSpec.getName,
                                                  iSpec.getIOClass, iSpec.isOptional)
            }.toMap
        (inputSpec, outputSpec)
    }

    // Search a platform path for all applets in it. Use
    // one API call for efficiency. Return a list of tasks, and their
    // applet-ids.
    //
    // If the folder is not a valid path, an empty list will be returned.
    private def search(dxProject: DXProject,
                       folder: String,
                       recursive: Boolean) : Vector[String] = {
        val dxAppletsInFolder: Seq[DXApplet] =
            if (recursive) {
                DXSearch.findDataObjects()
                    .inFolderOrSubfolders(dxProject, folder)
                    .withClassApplet
                    .includeDescribeOutput(DXDataObject.DescribeOptions.get().withProperties())
                    .execute().asList().asScala.toVector
            } else {
                DXSearch.findDataObjects()
                    .inFolder(dxProject, folder)
                    .withClassApplet
                    .includeDescribeOutput(DXDataObject.DescribeOptions.get().withProperties())
                    .execute().asList().asScala.toVector
            }

        // Filter applets that are WDL tasks
        val nativeApplets: Seq[DXApplet] = dxAppletsInFolder.map{ apl =>
            val desc = apl.getCachedDescribe()
            val props: Map[String, String] = desc.getProperties().asScala.toMap
            props.get(Utils.CHECKSUM_PROP) match {
                case Some(_) => None
                case None => Some(apl)
            }
        }.flatten

        nativeApplets.map{ apl =>
            val desc = apl.getCachedDescribe()
            val aplName = desc.getName
            try {
                val (inputSpec, outputSpec) = wdlTypesOfDxApplet(aplName, desc)
                // DNAx applets allow the same variable name to be used for inputs and outputs.
                // This is illegal in WDL.
                val allInputNames = inputSpec.keys.toSet
                val allOutputNames = outputSpec.keys.toSet
                val both = allInputNames.intersect(allOutputNames)
                if (!both.isEmpty) {
                    val bothStr = "[" + both.mkString(", ") + "]"
                    throw new Exception(s"""Parameters ${bothStr} used as both input and output in applet ${aplName}""")
                }
                val WdlCodeSnippet(taskCode) = WdlCodeGen(verbose, Map.empty).genDnanexusAppletStub(
                    apl.getId, aplName,
                    inputSpec, outputSpec,
                    language)
//                val task = ParseWomSourceFile.parseWdlTask(taskCode)
//                Utils.ignore(task)
                Some(taskCode)
            } catch {
                case e : Throwable =>
                    Utils.warning(verbose, s"Unable to construct a WDL interface for applet ${aplName}")
                    Utils.warning(verbose, e.getMessage)
                    None
            }
        }.flatten.toVector
    }

    private def checkedGetJsBooleanOrFalse(jsv: JsValue,
                                           fieldName: String) : Boolean = {
        val fields = jsv.asJsObject.fields
        fields.get(fieldName) match {
            case Some(JsBoolean(x)) => x
            case other => false
        }
    }

    private def checkedGetJsString(jsv: JsValue,
                                   fieldName: String) : String = {
        val fields = jsv.asJsObject.fields
        fields.get(fieldName) match {
            case Some(JsString(x)) => x
            case other => throw new Exception(s"malformed field ${fieldName} (${other})")
        }
    }

    private def checkedGetJsObject(jsv: JsValue,
                                   fieldName: String) : JsObject = {
        val fields = jsv.asJsObject.fields
        fields.get(fieldName) match {
            case Some(JsObject(x)) => JsObject(x)
            case other => throw new Exception(s"malformed field ${fieldName} (${other})")
        }
    }

    private def checkedGetJsArray(jsv: JsValue,
                                  fieldName: String) : Vector[JsValue] = {
        val fields = jsv.asJsObject.fields
        fields.get(fieldName) match {
            case Some(JsArray(x)) => x.toVector
            case other => throw new Exception(s"malformed field ${fieldName} (${other})")
        }
    }

    private def checkedGetIoSpec(appName: String,
                                 jsv: JsValue) : IoSpec = {
        val name = checkedGetJsString(jsv, "name")
        val ioClassRaw = checkedGetJsString(jsv, "class")
        val ioClass = ioClassRaw match {
            case "boolean" => IOClass.BOOLEAN
            case "int" => IOClass.INT
            case "float" => IOClass.FLOAT
            case "string" => IOClass.STRING
            case "file" => IOClass.FILE
            case "array:boolean" => IOClass.ARRAY_OF_BOOLEANS
            case "array:int" => IOClass.ARRAY_OF_INTS
            case "array:float" => IOClass.ARRAY_OF_FLOATS
            case "array:string" => IOClass.ARRAY_OF_STRINGS
            case "array:file" => IOClass.ARRAY_OF_FILES
            case other => throw new Exception(s"app ${appName} has field ${name} with non WDL-native io class ${other}")
        }
        val optional = checkedGetJsBooleanOrFalse(jsv, "optional")
        IoSpec(name, ioClass, optional)
    }


    // App names can have characters that are illegal in WDL.
    // 1) Leave only number, letters, and underscores
    // 2) If the name starts with a number, add the prefix "app_"
    private val taskNameRegex:Regex = raw"""[a-zA-Z][a-zA-Z0-9_.]*""".r
    private def normalizeAppName(name: String) : String = {
        def sanitizeChar(ch: Char) : String = ch match {
            case '_' => "_"
            case _ if (ch.isLetterOrDigit) => ch.toString
            case _ => "_"
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
                    case '_' => "app"
                    case _ => "app_"
                }
                if (!prefix.isEmpty)
                    Utils.warning(verbose, s"""|app ${nameClean} does not start
                                               |with a letter, adding the prefix '${prefix}'"""
                                      .stripMargin.replaceAll("\n", " "))
                s"${prefix}${nameClean}"
        }
    }

    private def checkedGetApp(jsv: JsValue) : DxApp = {
        val id: String = checkedGetJsString(jsv, "id")
        val desc: JsObject = checkedGetJsObject(jsv, "describe")
        val name: String = checkedGetJsString(desc, "name")
        val inputSpecJs = checkedGetJsArray(desc, "inputSpec")
        val outputSpecJs = checkedGetJsArray(desc, "outputSpec")

        val inputSpec = inputSpecJs.map{ x =>
            val spec = checkedGetIoSpec(name, x)
            spec.name -> spec
        }.toMap
        val outputSpec = outputSpecJs.map{ x =>
            val spec = checkedGetIoSpec(name, x)
            spec.name -> spec
        }.toMap
        val normName = normalizeAppName(name)
        DxApp(normName, id, inputSpec, outputSpec)
    }

    private def appToWdlInterface(dxApp: DxApp) : String = {
        val inputSpec:Map[String, WomType] =
            dxApp.inputSpec.map{ case (_,ioSpec) =>
                ioSpec.name -> wdlTypeOfIOClass(dxApp.name, ioSpec.name,
                                                ioSpec.ioClass, ioSpec.optional)
            }.toMap
        val outputSpec:Map[String, WomType] =
            dxApp.outputSpec.map{ case (_,ioSpec) =>
                ioSpec.name -> wdlTypeOfIOClass(dxApp.name, ioSpec.name,
                                                ioSpec.ioClass, ioSpec.optional)
            }.toMap

        // DNAx applets allow the same variable name to be used for inputs and outputs.
        // This is illegal in WDL.
        val allInputNames = inputSpec.keys.toSet
        val allOutputNames = outputSpec.keys.toSet
        val both = allInputNames.intersect(allOutputNames)
        if (!both.isEmpty) {
            val bothStr = "[" + both.mkString(", ") + "]"
            throw new Exception(s"""|Parameters ${bothStr} used as both input and
                                    |output in applet ${dxApp.name}""".stripMargin.replaceAll("\n", " "))
        }
        val WdlCodeSnippet(taskCode) = WdlCodeGen(verbose, Map.empty).genDnanexusAppletStub(
            dxApp.id, dxApp.name,
            inputSpec, outputSpec,
            language)
//        val task = ParseWomSourceFile.parseWdlTask(taskCode)
//        Utils.ignore(task)
        taskCode
    }

    // Search for global apps
    def searchApps: Vector[String] = {
        val req = JsObject("published" -> JsBoolean(true),
                           "describe" -> JsObject("inputSpec" -> JsBoolean(true),
                                                  "outputSpec" -> JsBoolean(true)),
                           "limit" -> JsNumber(1000))
        val rep = DXAPI.systemFindApps(Utils.jsonNodeOfJsValue(req),
                                       classOf[JsonNode])
        val repJs:JsValue = Utils.jsValueOfJsonNode(rep)
        val appsJs = repJs.asJsObject.fields.get("results") match {
            case Some(JsArray(apps)) => apps
            case _ => throw new Exception(s"""|malformed reply to findApps
                                              |
                                              |${repJs}""".stripMargin)
        }
        if (appsJs.length == 1000)
            throw new Exception("There are probably more than 1000 accessible apps")

        val taskHeaders = appsJs.flatMap{ jsv =>
            val desc: JsObject = checkedGetJsObject(jsv, "describe")
            val appName = checkedGetJsString(desc, "name")
            try {
                val dxApp = checkedGetApp(jsv)
                Some(appToWdlInterface(dxApp))
            } catch {
                case e : Throwable =>
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
                                   tasks : Vector[String],
                                   output: Path,
                                   force: Boolean) : Unit = {
        if (Files.exists(output)) {
            if (!force) {
                throw new Exception(s"""|Output file ${output.toString} already exists,
                                        |use -force to overwrite it"""
                                        .stripMargin.replaceAll("\n", " "))
            }
            output.toFile().delete
        }

        // pretty print into a buffer
        val lines = tasks.mkString("\n\n")
        val allLines = header + "\n" + lines
        Utils.writeFileContent(output, allLines)
    }


    // create headers for calling dx:applets and dx:workflows
    // We assume the folder is valid.
    def apply(dxProject: DXProject,
              folder: String,
              output: Path,
              recursive: Boolean,
              force: Boolean,
              language: Language.Value,
              verbose: Verbose) : Unit = {
        val dxni = new DxNI(verbose, language)
        val dxNativeTasks: Vector[String] = dxni.search(dxProject, folder, recursive)
        if (dxNativeTasks.isEmpty) {
            Utils.warning(verbose, s"Found no DX native applets in ${folder}")
            return
        }
        val projName = dxProject.describe.getName

        // add comment describing how the file was created
        val languageHeader = new WdlCodeGen(verbose, Map.empty).versionString(language)
        val header =
            s"""|# This file was generated by the Dx Native Interface (DxNI) tool.
                |# project name = ${projName}
                |# project ID = ${dxProject.getId}
                |# folder = ${folder}
                |
                |${languageHeader}
                |""".stripMargin

        writeHeadersToFile(header, dxNativeTasks, output, force)
    }

    def applyApps(output: Path,
                  force: Boolean,
                  language: Language.Value,
                  verbose: Verbose) : Unit = {
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
        val header =
            s"""|# This file was generated by the Dx Native Interface (DxNI) tool.
                |# These are interfaces to apps.
                |#
                |""".stripMargin

        writeHeadersToFile(header, uniqueTasks, output, force)
    }

}
