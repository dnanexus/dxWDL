// Declaration attributes, an experimental extension
package dxWDL

import wdl4s.wdl.{Declaration, WdlTask}
import wdl4s.wdl.expression.NoFunctions
import wdl4s.wdl.types._
import wdl4s.wdl.values._

case class DeclAttrs(m: Map[String, WdlValue]) {
    lazy val stream : Boolean = {
        m.get("stream") match {
            case Some(WdlBoolean(true)) => true
            case _ => false
        }
    }
}

object DeclAttrs {
    val empty = DeclAttrs(Map.empty)

    private def process(decl: Declaration,
                        attrs: Map[String, WdlValue],
                        cef: CompilerErrorFormatter) : Map[String, WdlValue] = {
        attrs.foldLeft(Map.empty[String, WdlValue]) {
            case (accu, (attrName, attrVal)) =>
                attrName match {
                    case "stream" =>
                        if (Utils.stripOptional(decl.wdlType) != WdlFileType) {
                            val msg = cef.onlyFilesCanBeStreamed(decl.ast)
                            System.err.println(s"Warning: ${msg}")
                            accu
                        } else {
                            accu ++ Map("stream" -> WdlBoolean(true))
                        }
                    case _ =>
                        // ignoring other attributes
                        accu
                }
        }
    }

    // Get the attributes from the parameter-meta
    // section. Currently, we only support a single attribute,
    // streaming, and it applies only to files. However, the
    // groundwork is being layed to support more complex
    // annotations.
    def get(task:WdlTask,
            varName: String,
            cef: CompilerErrorFormatter) : DeclAttrs = {
        val declOpt = task.declarations.find(decl => decl.unqualifiedName == varName)
        val m: Map[String, WdlValue] = declOpt match {
            case None => Map.empty
            case Some(decl) =>
                // Evaluate the expressions. We currently support only
                // constants, and very simple expressions.
                val attrs = decl.attributes.map{case (varName, expr) =>
                    def nullLookup(varName : String) : WdlValue = {
                        throw new Exception(cef.expressionMustBeConst(expr))
                    }
                    val wdlValue = expr.evaluate(nullLookup, NoFunctions).get
                    varName -> wdlValue
                }.toMap
                process(decl, attrs, cef)
        }
        DeclAttrs(m)
    }
}
