/** Generate an input file for a dx:workflow based on the
  * WDL input file.
  */
package dxWDL

import java.nio.file.{Path, Paths, Files}
import spray.json._
import spray.json.DefaultJsonProtocol

object InputFile {

    private def convert(wdlInputs: JsObject) = {
        throw new Exception("TODO")
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
        val dxInputs: JsObject = convert(wdlInputs)
        Utils.writeFileContent(dxInputFile, dxInputs.prettyPrint)
    }
}
