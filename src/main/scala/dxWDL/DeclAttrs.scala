// Declaration attributes, an experimental extension
package dxWDL

import spray.json._
import wdl4s.wdl.{Declaration, WdlTask}
import wdl4s.wdl.types._
import wdl4s.wdl.values._

case class DeclAttrs(m: Map[String, WdlValue]) {
    lazy val stream : Boolean = {
        m.get("stream") match {
            case Some(JsBoolean(true)) => true
            case _ => false
        }
    }

    def getDefault: Option[WdlValue] = {
        m.get("default")
    }

    // add another attribute
    def add(key:String, value:WdlValue) : DeclAttrs = {
        DeclAttrs(m + (key -> value))
    }

    def setDefault(value:WdlValue) : DeclAttrs = {
        add("default", value)
    }

    def isEmpty : Boolean = m.isEmpty

    def merge(attrs: DeclAttrs) : DeclAttrs = {
        DeclAttrs(attrs.m ++ m)
    }
}

object DeclAttrs {
    val empty = DeclAttrs(Map.empty)

    // Get the attributes from the parameter-meta
    // section. Currently, we only support a single attribute,
    // streaming, and it applies only to files. However, the
    // groundwork is being layed to support more complex
    // annotations.
    def get(task:WdlTask,
            varName: String,
            cefOpt: Option[CompilerErrorFormatter]) : DeclAttrs = {
        val attr:Option[(String,String)] = task.parameterMeta.find{ case (k,v) =>  k == varName }
        val m:Map[String, WdlValue] = attr match {
            case None => Map.empty
            case Some((_,"stream")) =>
                // Only files can be streamed
                val declOpt:Option[Declaration] =
                    task.declarations.find{ decl => decl.unqualifiedName == varName }
                val decl = declOpt match {
                    case None => throw new Exception(s"No variable ${varName}")
                    case Some(x) => x
                }
                if (Utils.stripOptional(decl.wdlType) != WdlFileType) {
                    cefOpt match {
                        case Some(cef) =>
                            val msg = cef.onlyFilesCanBeStreamed(decl.ast)
                            System.err.println(s"Warning: ${msg}")
                            Map.empty
                        case None =>
                            System.err.println(s"Warning: only files can be streamed ${varName} type=${decl.wdlType}")
                            Map.empty
                    }
                } else {
                    Map("stream" -> WdlBoolean(true))
                }
            case Some((_,x)) =>
                // ignoring other attributes
                Map.empty
        }
        DeclAttrs(m)
    }
}
