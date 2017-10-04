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
    type: extern
    applet_id: applet-xxxx
  }
}

  */
package dxWDL

import com.dnanexus.{DXApplet, DXSearch, IOClass, InputParameter, OutputParameter}
import wdl4s.wdl.{WdlTask}
import wdl4s.wdl.types._
import wdl4s.wdl.values._

case class DxExtern(verbose: Utils.Verbose) {

    private def wdlTypeOfIOClass(appletName:String,
                                 argName: String,
                                 ioClass: IOClass) : WdlType = {
        ioClass match {
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
            case _ =>
                throw new Exception(s"""|Cannot call applet ${appletName} from WDL, argument ${name}
                                        |has IO class ${ioClass}""".stripMargin.replaceAll("\n", " "))
        }
    }

    // Convert an applet to a WDL task with an empty body
    //
    // We can deal only with primitive types, and their arrays. Hashes are
    // not supported; such applets are ignored.
    private def wdlTypesOfDxApplet(aplName: String,
                                   desc: DXApplet.Describe) :
            (Map[String, WdlType], Map[String, WdlType]) = {
        val inputSpecRaw: List[InputParameter] = desc.getInputSpecification().asScala.toList
        val inputSpec:Map[String, IOClass] =
            inputSpecRaw.map{ iSpec =>
                iSpec.getName -> wdlTypeOfIOClass(aplName, iSpec.getName, iSpec.getIOClass)
            }.toMap
        val outputSpecRaw: List[OutputParameter] = desc.getOutputSpecification().asScala.toList
        val outputSpec:Map[String, IOClass] =
            outputSpecRaw.map{ iSpec =>
                iSpec.getName -> wdlTypeOfIOClass(aplName, iSpec.getName, iSpec.getIOClass)
            }.toMap
        (inputSpec, outputSpec)
    }

    private def genAppletHeader(appletName: String,
                                inputSpec: Map[String, WdlType],
                                outputSpec: Map[String, WdlType]) : WdlTask = {
        val task = WdlRewrite.taskGenEmpty(appletName, scope)
        val inputs = inputSpec.map{ case (name, wdlType) =>
            WdlRewrite.declaration(name, wdlType, None)
        }.toVector
        val outputs = outputs.map{ case (name, wdlType) =>
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
    def apply(dxProject: DXProject,
              folder: String) : Vector[(WdlTask, DXApplet)] = {
        // search the entire folder for dx:applets, use one API call
        // for efficiency.
        val dxAppletsInFolder: List[DXApplet] = DXSearch.findDataObjects()
            .inFolder(dxProject, folder)
            .withClassApplet
            .includeDescribeOutput
            .execute().asList().asScala.toList

        dxAppletsInFolder.map{ apl =>
            val desc = apl.getCachedDescribe()
            val aplName = apl.getName
            val (inputSpec, outputSpec) = wdlTypesOfDxApplet(aplName, desc)
            val task = genAppletHeader(aplName, inputSpec, outputSpec)
            (task, apl)
        }.toVector
    }
}
