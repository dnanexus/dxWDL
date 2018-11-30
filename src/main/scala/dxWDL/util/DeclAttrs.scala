// Declaration attributes, an experimental extension
package dxWDL.util

import wdl.draft2.model.{Declaration, WdlTask}
import wom.values._
import wom.types._

case class DeclAttrs(m: Map[String, WomValue]) {
    lazy val stream : Boolean = {
        m.get("stream") match {
            case Some(WomBoolean(true)) => true
            case _ => false
        }
    }

    // add another attribute
    def add(key:String, value:WomValue) : DeclAttrs = {
        DeclAttrs(m + (key -> value))
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
    def get(task:WdlTask, varName: String) : DeclAttrs = {
        val attr:Option[(String,String)] = task.parameterMeta.find{ case (k,v) =>  k == varName }
        val m:Map[String, WomValue] = attr match {
            case None => Map.empty
            case Some((_,"stream")) =>
                // Only files can be streamed
                val declOpt:Option[Declaration] =
                    task.declarations.find{ decl => decl.unqualifiedName == varName }
                val decl = declOpt match {
                    case None => throw new Exception(s"No variable ${varName}")
                    case Some(x) => x
                }
                if (!Utils.stripOptional(decl.womType).isInstanceOf[WomFileType]) {
                    System.err.println(s"Warning: only files can be streamed ${varName} type=${decl.womType.toDisplayString}")
                    Map.empty
                } else {
                    Map("stream" -> WomBoolean(true))
                }
            case Some((_,x)) =>
                // ignoring other attributes
                Map.empty
        }
        DeclAttrs(m)
    }
}
