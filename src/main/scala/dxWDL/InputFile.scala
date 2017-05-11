/** Generate an input file for a dx:workflow based on the
  * WDL input file.
  */
package dxWDL

import java.nio.file.{Path, Paths, Files}
import spray.json._
import spray.json.DefaultJsonProtocol

object InputFile {

    // Build a dx input file, based on the wdl input file and the workflow
    def apply(wdlSrcFile: Path,
              wdlInputFile: Path,
              verbose: Boolean) : Path = {
        throw new Exception("Not implemented yet")
    }
}
