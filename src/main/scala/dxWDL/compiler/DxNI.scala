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

import com.dnanexus.{DXApplet, DXDataObject, DXProject, DXSearch,
    IOClass, InputParameter, OutputParameter}
import dxWDL.{Utils, WdlPrettyPrinter}
import java.nio.file.{Files, Path}
import scala.collection.JavaConverters._
import scala.util.{Failure, Success}
import wdl.{WdlTask, WdlNamespace}
import wom.types._

case class DxNI(ns: WdlNamespace, verbose: Utils.Verbose) {

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
            case IOClass.FILE => WomOptionalType(WomFileType)
            case IOClass.ARRAY_OF_BOOLEANS => WomMaybeEmptyArrayType(WomBooleanType)
            case IOClass.ARRAY_OF_INTS => WomMaybeEmptyArrayType(WomIntegerType)
            case IOClass.ARRAY_OF_FLOATS => WomMaybeEmptyArrayType(WomFloatType)
            case IOClass.ARRAY_OF_STRINGS => WomMaybeEmptyArrayType(WomStringType)
            case IOClass.ARRAY_OF_FILES => WomMaybeEmptyArrayType(WomFileType)
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
                case IOClass.FILE => WomFileType
                case IOClass.ARRAY_OF_BOOLEANS => WomNonEmptyArrayType(WomBooleanType)
                case IOClass.ARRAY_OF_INTS => WomNonEmptyArrayType(WomIntegerType)
                case IOClass.ARRAY_OF_FLOATS => WomNonEmptyArrayType(WomFloatType)
                case IOClass.ARRAY_OF_STRINGS => WomNonEmptyArrayType(WomStringType)
                case IOClass.ARRAY_OF_FILES => WomNonEmptyArrayType(WomFileType)
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

    private def genAppletStub(dxApplet: DXApplet,
                              appletName: String,
                              inputSpec: Map[String, WomType],
                              outputSpec: Map[String, WomType]) : WdlTask = {
        val meta = Map("type" -> "native",
                       "id" -> dxApplet.getId)
        val task = WdlRewrite.taskGenEmpty(appletName, meta, ns)
        val inputs = inputSpec.map{ case (name, wdlType) =>
            WdlRewrite.declaration(wdlType, name, None)
        }.toVector
        val outputs = outputSpec.map{ case (name, wdlType) =>
            WdlRewrite.taskOutput(name, wdlType, task)
        }.toVector
        task.children = inputs ++ outputs
        task
    }


    // Search a platform path for all applets in it. Use
    // one API call for efficiency. Return a list of tasks, and their
    // applet-ids.
    //
    // If the folder is not a valid path, an empty list will be returned.
    def search(dxProject: DXProject,
               folder: String,
               recursive: Boolean) : Vector[WdlTask] = {
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
                val task = genAppletStub(apl, aplName, inputSpec, outputSpec)
                Some(task)
            } catch {
                case e : Throwable =>
                    System.err.println(s"Unable to construct a WDL interface for applet ${aplName}")
                    System.err.println(e.getMessage)
                    None
            }
        }.flatten.toVector
    }
}

object DxNI {
    // create headers for calling dx:applets and dx:workflows
    // We assume the folder is valid.
    def apply(dxProject: DXProject,
              folder: String,
              output: Path,
              recursive: Boolean,
              force: Boolean,
              verbose: Utils.Verbose) : Unit = {
        val nsEmpty = WdlRewrite.namespaceEmpty()
        val dxni = DxNI(nsEmpty, verbose)

        val dxNativeTasks: Vector[WdlTask] = dxni.search(dxProject, folder, recursive)
        if (dxNativeTasks.isEmpty) {
            System.err.println(s"Found no DX native applets in ${folder}")
            return
        }
        val ns = WdlRewrite.namespace(dxNativeTasks)
        val projName = dxProject.describe.getName

        // pretty print into a buffer
        val lines: String = WdlPrettyPrinter(false, None)
            .apply(ns, 0)
            .mkString("\n")
        // add comment describing how the file was created
        val header =
            s"""|# This file was generated by the Dx Native Interface (DxNI) tool.
                |# project name = ${projName}
                |# project ID = ${dxProject.getId}
                |# folder = ${folder}
                |""".stripMargin
        val allLines = header + "\n" + lines
        if (Files.exists(output)) {
            if (!force) {
                throw new Exception(s"""|Output file ${output.toString} already exists,
                                        |use -force to overwrite it"""
                                        .stripMargin.replaceAll("\n", " "))
            }
            output.toFile().delete
        }

        Utils.writeFileContent(output, allLines)

        // Validate the file
        WdlNamespace.loadUsingSource(allLines, None, None) match {
            case Success(_) => ()
            case Failure(f) =>
                System.err.println("DxNI generated WDL file contains errors")
                throw f
        }
    }
}
