// Emit a YAML structure reflecting a wdl4s object tree. This is mainly an exercise to
// familiarize ourselves with the WDL syntax and wdl4s object hierarchy.
package dxWDL
import wdl4s._
import net.jcazevedo.moultingyaml._
import net.jcazevedo.moultingyaml.DefaultYamlProtocol._

// the applicable WdlYamlTree() object recursively maps the main wdl4s objects into
// YamlValues.
object WdlYamlTree {

    def apply(expr: WdlExpression): YamlValue = {
        // not decomposing individual expressions for now
        YamlString(expr.toWdlString)
    }

    def apply(decl: Declaration): YamlObject = {
        val base: Map[YamlValue, YamlValue] = Map(
            YamlString("type") -> YamlString(decl.wdlType.toWdlString),
            YamlString("name") -> YamlString(decl.unqualifiedName)
        )
        // TODO: Figure out how to get the quantifier information
        // for this declaration from the AST
        /*    var pq = decl.postfixQuantifier match {
         case Some("?") => Map(YamlString("optional") -> YamlBoolean(true))
         case Some("+") => Map(YamlString("nonempty") -> YamlBoolean(true))
         case None => Map()
         }*/
        val expr = decl.expression match {
            case Some(ex) => Map(YamlString("expression") -> apply(ex))
            case None => Map()
        }
        //YamlObject(base ++ pq ++ expr)
        YamlObject(base ++ expr)
    }

    def apply(tso: TaskOutput): YamlObject = {
        val base : Map[YamlValue, YamlValue] = Map(
            YamlString("type") -> YamlString(tso.wdlType.toWdlString),
            YamlString("name") -> YamlString(tso.unqualifiedName)
        )
        val expr = tso.expression match {
            case Some(ex) => Map(YamlString("expression") -> YamlString(ex.toWdlString))
            case None => Map()
        }
        YamlObject(base ++ expr)
    }

    def apply(task: Task): YamlObject = {
        val decls = task.declarations.map(apply)
        val outputs = task.outputs.map(apply)
        val runtime: Map[YamlValue, YamlValue] = task.runtimeAttributes.attrs.map { case (nm,ex) =>
            YamlString(nm) -> apply(ex)
        }
        YamlObject(
            YamlString("name") -> YamlString(task.name),
            YamlString("declarations") -> YamlArray(decls.toVector),
            YamlString("commandTemplate") -> YamlString(task.commandTemplateString),
            YamlString("outputs") -> YamlArray(outputs.toVector),
            YamlString("runtime") -> YamlObject(runtime)
        )
    }

    def apply(call: Call): YamlObject = {
        val task = Utils.taskOfCall(call)
        val nm = call.alias match {
            case Some(x) => x
            case None => task.name
        }
        val im: Map[YamlValue, YamlValue] = call.inputMappings.map { case (k,v) =>
            YamlString(k) -> apply(v)
        }
        YamlObject(
            YamlString("call") -> YamlObject(
                YamlString("name") -> YamlString(nm),
                YamlString("task") -> YamlString(task.name),
                YamlString("inputMappings") -> YamlObject(im)
            )
        )
    }

    def apply(sc: Scatter): YamlObject = {
        // Although the WDL syntax can have declarations scoped within a scatter, oddly
        // there is no wdl4s.Scatter.declarations. Instead, wdl4s seems to "lift" such
        // declarations into the containing Workflow.declarations.
        //val decls = sc.declarations.map(apply)
        val children = sc.children.map {
            case call: Call => apply(call)
            case ssc: Scatter => apply(ssc)
            case swf: Workflow => apply(swf)
        }
        YamlObject(
            YamlString("scatter") -> YamlObject(
                YamlString("item") -> YamlString(sc.item),
                YamlString("collection") -> apply(sc.collection),
                //YamlString("declarations") -> YamlArray(decls.toVector),
                YamlString("children") -> YamlArray(children.toVector)
            )
        )
    }

    def apply(wf: Workflow): YamlObject = {
        val decls = wf.declarations.map(apply)
        val children = wf.children.map {
            case call: Call => Some(apply(call))
            case sc: Scatter => Some(apply(sc))
            case swf: Workflow => Some(apply(swf))
            case decl: Declaration => None
        }.flatten
        val outputs = wf.outputs.map { wod =>
            // TODO: figure out how to check for wildcards
/*            if (wod.wildcard) {
                YamlObject(YamlString("fqn") -> YamlString(wod.fullyQualifiedName), YamlString("wildcard") -> YamlBoolean(true))
            } else {*/
                YamlObject(YamlString("fqn") -> YamlString(wod.fullyQualifiedName))
//            }
        }
        YamlObject(
            YamlString("workflow") -> YamlObject(
                YamlString("name") -> YamlString(wf.unqualifiedName),
                YamlString("declarations") -> YamlArray(decls.toVector),
                YamlString("children") -> YamlArray(children.toVector),
                YamlString("output") -> YamlArray(outputs.toVector)
            )
        )
    }

    def apply(ns : WdlNamespace): YamlObject = {
        val doc: Map[YamlValue, YamlValue] = Map(
            YamlString("tasks") -> YamlArray(ns.tasks.map(apply).toVector)
        )
        val docwf: Map[YamlValue, YamlValue] = ns match {
            case nswf : WdlNamespaceWithWorkflow => doc ++ apply(nswf.workflow).fields
            case _ => doc
        }
        YamlObject(docwf)
    }
}
