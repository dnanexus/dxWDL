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
package dxWDL

import com.dnanexus.{DXApplet, DXDataObject, DXProject, DXSearch,
    IOClass, InputParameter, OutputParameter}
import java.nio.file.{Files, Path}
import scala.collection.JavaConverters._
import Utils.{CHECKSUM_PROP, trace}
import wdl4s.wdl.{WdlTask, WdlNamespace}
import wdl4s.wdl.types._

case class DxCI(ns: WdlNamespace, verbose: Utils.Verbose) {

    private def wdlTypeOfIOClass(appletName:String,
                                 argName: String,
                                 ioClass: IOClass,
                                 isOptional: Boolean) : WdlType = {
        val t:WdlType = ioClass match {
            case IOClass.BOOLEAN => WdlBooleanType
            case IOClass.INT => WdlIntegerType
            case IOClass.FLOAT => WdlFloatType
            case IOClass.STRING => WdlStringType
            case IOClass.FILE => WdlFileType
            case IOClass.ARRAY_OF_BOOLEANS => WdlArrayType(WdlBooleanType)
            case IOClass.ARRAY_OF_INTS => WdlArrayType(WdlIntegerType)
            case IOClass.ARRAY_OF_FLOATS => WdlArrayType(WdlFloatType)
            case IOClass.ARRAY_OF_STRINGS => WdlArrayType(WdlStringType)
            case IOClass.ARRAY_OF_FILES => WdlArrayType(WdlFileType)
            case _ => throw new Exception(
                s"""|Cannot call applet ${appletName} from WDL, argument ${argName}
                    |has IO class ${ioClass}""".stripMargin.replaceAll("\n", " "))
        }
        if (isOptional)
            WdlOptionalType(t)
        else
            t
    }

    // Convert an applet to a WDL task with an empty body
    //
    // We can deal only with primitive types, and their arrays. Hashes are
    // not supported; such applets are ignored.
    private def wdlTypesOfDxApplet(aplName: String,
                                   desc: DXApplet.Describe) :
            (Map[String, WdlType], Map[String, WdlType]) = {
        trace(verbose.on, s"analyzing applet ${aplName}")
        val inputSpecRaw: List[InputParameter] = desc.getInputSpecification().asScala.toList
        val inputSpec:Map[String, WdlType] =
            inputSpecRaw.map{ iSpec =>
                iSpec.getName -> wdlTypeOfIOClass(aplName, iSpec.getName, iSpec.getIOClass, iSpec.isOptional)
            }.toMap
        val outputSpecRaw: List[OutputParameter] = desc.getOutputSpecification().asScala.toList
        val outputSpec:Map[String, WdlType] =
            outputSpecRaw.map{ iSpec =>
                iSpec.getName -> wdlTypeOfIOClass(aplName, iSpec.getName, iSpec.getIOClass, iSpec.isOptional)
            }.toMap
        (inputSpec, outputSpec)
    }

    private def genAppletStub(dxApplet: DXApplet,
                              appletName: String,
                              inputSpec: Map[String, WdlType],
                              outputSpec: Map[String, WdlType]) : WdlTask = {
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


    // Search a platform path for all workflows and applets in
    // it. Return a list of tasks, and their applet-ids.
    //
    // We assume that [folder] is indeed a platform directory. This
    // needs to be checked.
    def search(dxProject: DXProject,
               folder: String) : Vector[WdlTask] = {
        // search the entire folder for dx:applets, use one API call
        // for efficiency.
        val dxAppletsInFolder: Seq[DXApplet] = DXSearch.findDataObjects()
            .inFolder(dxProject, folder)
            .withClassApplet
            .includeDescribeOutput(DXDataObject.DescribeOptions.get().withProperties())
            .execute().asList().asScala.toVector

        // Filter applets that are WDL tasks
        val nativeApplets: Seq[DXApplet] = dxAppletsInFolder.map{ apl =>
            val desc = apl.getCachedDescribe()
            val props: Map[String, String] = desc.getProperties().asScala.toMap
            props.get(CHECKSUM_PROP) match {
                case Some(_) => None
                case None => Some(apl)
            }
        }.flatten

        nativeApplets.map{ apl =>
            val desc = apl.getCachedDescribe()
            val aplName = desc.getName
            try {
                val (inputSpec, outputSpec) = wdlTypesOfDxApplet(aplName, desc)
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

object DxCI {
    // create headers for calling dx:applets and dx:workflows
    def apply(dxProject: DXProject,
              folder: String,
              output: Path,
              force: Boolean,
              verbose: Utils.Verbose) : Unit = {
        val nsEmpty = WdlRewrite.namespaceEmpty()
        val dxFfi = DxCI(nsEmpty, verbose)
        val dxNativeTasks: Vector[WdlTask] = dxFfi.search(dxProject, folder)
        val ns = WdlRewrite.namespace(dxNativeTasks)

        // pretty print into a buffer
        val lines: String = WdlPrettyPrinter(false, None)
            .apply(ns, 0)
            .mkString("\n")

        if (Files.exists(output)) {
            if (!force) {
                throw new Exception(s"""|Output file ${output.toString} already exists,
                                        |use -force to overwrite it"""
                                        .stripMargin.replaceAll("\n", " "))
            }
            output.toFile().delete
        }
        Utils.writeFileContent(output, lines)
    }
}
