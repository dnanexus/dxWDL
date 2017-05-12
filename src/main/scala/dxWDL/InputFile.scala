/** Generate an input file for a dx:workflow based on the
  * WDL input file.
  *
  * In the documentation, we assume the workflow name is "myWorkflow"
  */
package dxWDL

import java.nio.file.{Path, Paths, Files}
import spray.json._
import spray.json.DefaultJsonProtocol

object InputFile {

    // 1. Convert fields of the form myWorkflow.xxxx to common.xxxx
    //
    private def dxTranslate(wf: IR.Workflow, wdlInputs: JsObject) : JsObject= {
        val m: Map[String, JsValue] = wdlInputs.fields.map{ case (key, v) =>
            val components = key.split("\\.")
            val dxKey =
                if (components.length == 0) {
                    throw new Exception(s"String ${key} cannot be a JSON field key")
                } else if (components.length == 1) {
                    key
                } else {
                    val call = components.head
                    val suffix = components.tail.mkString(".")
                    val stageName =
                        if (call == wf.name) "common"
                        else call
                    stageName + "." + suffix
                }
            dxKey -> v
        }.toMap
        JsObject(m)
    }

    // Build a dx input file, based on the wdl input file and the workflow
    def apply(wf: IR.Workflow,
              wdlInputFile: Path,
              dxInputFile: Path,
              verbose: Boolean) : Unit = {
        // read the input file
        val wdlInputs: JsObject = Utils.readFileContent(wdlInputFile).parseJson.asJsObject
        wdlInputs.fields.foreach{ case (key, v) =>
            System.err.println(s"${key} -> ${v}")
        }
        val dxInputs: JsObject = dxTranslate(wf, wdlInputs)
        Utils.writeFileContent(dxInputFile, dxInputs.prettyPrint)
    }
}
