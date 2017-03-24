
package dxWDL

// DX bindings
import com.fasterxml.jackson.databind.JsonNode
import com.dnanexus.{DXWorkflow, DXApplet, DXProject, DXJSON, DXUtil, DXContainer, DXSearch, DXDataObject}
import java.nio.file.{Files, Paths, Path}
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}
import wdl4s.AstTools
import wdl4s.AstTools.EnhancedAstNode
import wdl4s.{Call, Declaration, Scatter, Scope, Task, TaskOutput, WdlExpression,
    WdlNamespace, WdlNamespaceWithWorkflow, WdlSource, Workflow}
import wdl4s.expression.{NoFunctions, WdlStandardLibraryFunctionsType}
//import wdl4s.parser.WdlParser._
import wdl4s.parser.WdlParser.{Ast, AstNode, Terminal}
import wdl4s.types._
import wdl4s.values._
import wdl4s.WdlExpression.AstForExpressions
import WdlVarLinks._

// Json stuff
import spray.json._
import DefaultJsonProtocol._

object CompilerBackEnd {

    def apply(wf: IR.Workflow,
              destination: String,
              dxWDLrtId: String,
              verbose: Boolean) : DXWorkflow = {
        throw new RuntimeException("Not implemented yet")
    }
}
