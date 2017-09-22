/** Generate an input file for a dx:workflow based on the
  WDL input file.

  In the documentation, we assume the workflow name is "myWorkflow"

For example, this is a Cromwell input file for workflow optionals:
{
  "optionals.arg1": 10,
  "optionals.mul2.i": 5,
  "optionals.add.a" : 1,
  "optionals.add.b" : 3
}

This is the dx JSON input:
{
  "0.arg1": 10,
  "stage-xxxx.i": 5,
  "stage-yyyy.a": 1,
  "stage-yyyy.b": 3
}
  */
package dxWDL

import com.dnanexus.{DXFile, DXProject, DXSearch}
import scala.collection.mutable.HashMap
import IR._
import java.nio.file.{Path, Paths}
import scala.collection.JavaConverters._
import spray.json._
import Utils.{UNIVERSAL_FILE_PREFIX}

case class InputFile(verbose: Utils.Verbose) {
    private def lookupFile(dxProject: Option[DXProject], fileName: String): DXFile = {
        if (fileName.startsWith("file-")) {
            // A file ID
            DXFile.getInstance(fileName)
        } else {
            val fullPath = Paths.get(fileName)
            var folder = fullPath.getParent.toString
            if (!folder.startsWith("/"))
                folder = "/" + folder
            val baseName = fullPath.getFileName.toString

            val found:List[DXFile] = dxProject match  {
                case Some(x) =>
                    DXSearch.findDataObjects().nameMatchesExactly(baseName)
                        .inFolder(x, folder).withClassFile().execute().asList().asScala.toList
                case None =>
                    throw new Exception("File lookup requires project context")
            }
            if (found.length == 0)
                throw new Exception(s"File ${fileName} not found in project ${dxProject}")
            if (found.length > 1)
                throw new Exception(s"Found more than one file named ${fileName} in project ${dxProject}")
            found(0)
        }
    }

    private def lookupDxPath(dxPath: String) : DXFile = {
        val components = dxPath.split(":/")
        if (components.length > 2) {
            throw new Exception(s"Path ${dxPath} cannot more than two components")
        } else if (components.length == 2) {
            val projName = components(0)
            val fileName = components(1)
            val dxProject = Utils.lookupProject(projName)
            lookupFile(Some(dxProject), fileName)
        } else if (components.length == 1) {
            val fileName = components(0)
            lookupFile(None, fileName)
        } else {
            throw new Exception(s"Path ${dxPath} is invalid")
        }
    }

    private def translateValue(v: JsValue) : JsValue= {
        v match {
            case JsString(s) if s.startsWith(UNIVERSAL_FILE_PREFIX) =>
                // Identify platform file paths by their prefix,
                // do a lookup, and create a dxlink
                val dxFile: DXFile = lookupDxPath(s.substring(UNIVERSAL_FILE_PREFIX.length))
                Utils.jsValueOfJsonNode(dxFile.getLinkAsJson)
            case JsArray(a) =>
                JsArray(a.map(x => translateValue(x)))
            case _ => v
        }
    }

    // Embed default values into the IR
    //
    // Make a sequential pass on the IR, figure out the fully qualified names
    // of all CVar and SArgs. If they have a default value, add it as an attribute
    // (DeclAttrs).
    def embedDefaults(ns: Namespace,
                      defaultInputs: Path) : Namespace = {
        Utils.trace(verbose.on, s"Embedding defaults into the IR")

        // read the default inputs file (xxxx.json)
        val defaults:JsObject = Utils.readFileContent(defaultInputs).parseJson.asJsObject

        // if applet inputs have defaults, set them as attributes
        def addDefaultsToApplet(apl:Applet, wdlNameTrail:String) : Applet = {
            val inputsWithDefaults = apl.inputs.map{ cVar =>
                val fqn = s"${wdlNameTrail}.${cVar.name}"
                defaults.fields.get(fqn) match {
                    case None => cVar
                    case Some(dflt:JsValue) =>
                        val attrs = cVar.attrs.add("default", dflt)
                        cVar.copy(attrs = attrs)
                }
            }
            apl.copy(inputs = inputsWithDefaults)
        }

        // If a stage has defaults, set the SArg to a constant. The user
        // can override it at runtime.
        def addDefaultsToStage(stg:Stage, wdlNameTrail:String) : Stage = {
            val inputsWithDefaults:Vector[SArg] = stg.inputs.zipWithIndex.map{
                case (sArg,idx) =>
                    val callee:Applet = ns.applets(stg.appletName)
                    val cVar = callee.inputs(idx)
                    val fqn = s"${wdlNameTrail}.${cVar.name}"
                    defaults.fields.get(fqn) match {
                        case None => sArg
                        case Some(dflt:JsValue) =>
                            val wvl = WdlVarLinks(cVar.wdlType, cVar.attrs, DxlValue(dflt))
                            val w = WdlVarLinks.eval(wvl, false)
                            SArgConst(w)
                    }
            }
            stg.copy(inputs = inputsWithDefaults)
        }

        // figure out the WDL ancestry of an applet. What piece
        // of WDL code is the source for this applet?
        ns.workflow match {
            case Some(wf)  =>
                val applets = ns.applets.map{ case (name, applet) =>
                    val nameTrail = applet.kind match {
                        case AppletKindTask => applet.name
                        case _ => wf.name
                    }
                    val apl = addDefaultsToApplet(applet, nameTrail)
                    apl.name -> apl
                }.toMap
                // add defaults to workflow stages
                val stages = wf.stages.map{ stg =>
                    val nameTrail = s"${wf.name}.${stg.name}"
                    addDefaultsToStage(stg, nameTrail)
                }
                val wfWithDefaults = wf.copy(stages = stages)
                Namespace(Some(wfWithDefaults), applets)

            case None =>
                // The namespace comprises tasks only
                val applets = ns.applets.map{ case (name, applet) =>
                    val nameTrail = applet.kind match {
                        case AppletKindTask => applet.name
                        case _ => throw new Exception("sanity")
                    }
                    val apl = addDefaultsToApplet(applet, nameTrail)
                    apl.name -> apl
                }.toMap
                Namespace(None, applets)
        }
    }

    // Build a dx input file, based on the wdl input file and the workflow
    def dxFromCromwell(ns: Namespace,
                       inputPath: Path) : JsObject = {
        Utils.trace(verbose.on, s"Translating WDL input file ${inputPath}")


        // read the input file xxxx.json
        val wdlInputs: JsObject = Utils.readFileContent(inputPath).parseJson.asJsObject

        // The general idea here is to figure out the ancestry of each
        // applet/call/workflow input. This provides the fully-qualified-name (fqn)
        // of each IR variable. Then we check if the fqn is defined in
        // the input file.

        // make a pass on all applets in the namespace
        val appletBindings = HashMap.empty[String, JsValue]
        ns.applets.foreach{ case (name, applet) =>
            val nameTrail = applet.kind match {
                case AppletKindTask => applet.name
                case other =>
                    ns.workflow match {
                        case Some(wf) =>
                            // An applet generated from a piece of the workflow
                            wf.name
                        case None =>
                            // A namespace with no workflow, it cannot have generated applets.
                            throw new Exception(s"Sanity, bad applet type ${other}")
                    }
            }
            // search for all the applet inputs
            applet.inputs.foreach{ cVar =>
                val fqn = s"${nameTrail}.${cVar.name}"
                wdlInputs.fields.get(fqn).map{ jsv =>
                    appletBindings(fqn) = translateValue(jsv)
                }
            }
        }

        // make a pass on the workflow, if there is one
        val workflowBindings = HashMap.empty[String, JsValue]
        ns.workflow.map{ wf =>
            wf.stages.foreach{ stg =>
                val nameTrail = s"${wf.name}.${stg.name}"

                // make a pass on all call inputs
                stg.inputs.zipWithIndex.foreach{
                    case (sArg,idx) =>
                        val callee:Applet = ns.applets(stg.appletName)
                        val cVar = callee.inputs(idx)
                        val fqn = s"${nameTrail}.${cVar.name}"
                        wdlInputs.fields.get(fqn).map{ jsv =>
                            appletBindings(fqn) = translateValue(jsv)
                        }
                }
            }
        }

        val m:Map[String, JsValue] = appletBindings.toMap ++ workflowBindings.toMap
        JsObject(m)
    }
}
