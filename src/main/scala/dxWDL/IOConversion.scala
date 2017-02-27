package dxWDL

// DX bindings
import com.dnanexus.{DXApplet, DXEnvironment, DXFile, DXProject, DXWorkflow, InputParameter}
import java.nio.file.{Path, Paths}
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer
import spray.json._
import spray.json.DefaultJsonProtocol
import spray.json.JsString
import wdl4s.types._
import wdl4s.values._
import wdl4s.WdlExpression.AstForExpressions
import WdlVarLinks._

// Convert between dxApplet input/output fields and WDL values.
object IOConversion {
}
