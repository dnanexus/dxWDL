// Declaration attributes, an experimental extension
package dxWDL

import spray.json._
import wdl4s.{Declaration, Task}
import wdl4s.parser.WdlParser.{Ast, AstNode, Terminal}
import wdl4s.types._
import wdl4s.values._

case class DeclAttrs(m: Map[String, JsValue])

object DeclAttrs {
    val empty = DeclAttrs(Map.empty)

    // Get the attributes from the parameter-meta
    // section. Currently, we only support a single attribute,
    // streaming, and it applies only to files. However, the
    // groundwork is being layed out to support more complex
    // annotations in the future.
    def get(task:Task,
            varName: String,
            ast: Ast,
            cef: CompilerErrorFormatter) : DeclAttrs = {
        val attr:Option[(String,String)] = task.parameterMeta.find{ case (k,v) =>  k == varName }
        val m:Map[String, JsValue] = attr match {
            case None => Map.empty
            case Some((_,"stream")) => Map("stream" -> JsBoolean(true))
            case Some((_,x)) =>
                throw new Exception(cef.notCurrentlySupported(ast, s"attribute ${x}"))
        }
        DeclAttrs(m)
    }
}
