// A dictionary of all WDL files that are also
// platform files. These are files that have been uploaded or
// downloaded. The WDL representation is (essentially) a local filesystem path.
package dxWDL.util

import com.dnanexus.DXFile
import java.nio.file.{Path}
import spray.json._
import wom.values._

object LocalDxFiles {
    def freeze() : Unit = ???

    def unfreeze() : Unit = ???

    def get(dxFile: DXFile) : Option[Path] = ???
    def get(path: Path) : Option[DXFile] = ???

    def upload(path: Path) : JsValue =  ???

    def delete(path: Path) : Unit = ???

    def download(jsValue: JsValue, ioMode: IOMode.Value) : WomValue = ???
}
