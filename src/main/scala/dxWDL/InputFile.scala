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
import java.nio.file.{Path, Paths}
import scala.collection.JavaConverters._
import spray.json._
import Utils.{trace, UNIVERSAL_FILE_PREFIX}

case class InputFile(verbose: Utils.Verbose) {
    private def lookupFile(dxProject: Option[DXProject], fileName: String): DXFile = {
        if (fileName.startsWith("file-")) {
            // A file ID
            DXFile.getInstance(fileName)
        } else {
            val fullPath = Paths.get(fileName)
            trace(verbose.on, s"lookupFile: ${fullPath.toString}")
            val parent = fullPath.getParent
            var folder = "/"
            if (parent != null) {
                folder = parent.toString
                if (!folder.startsWith("/"))
                    folder = "/" + folder
            }
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
    def embedDefaults(ns: IR.Namespace,
                      defaultInputs: Path) : IR.Namespace = {
        trace(verbose.on, s"Embedding defaults into the IR")

        // read the default inputs file (xxxx.json)
        val defaults:JsObject = Utils.readFileContent(defaultInputs).parseJson.asJsObject

        // if applet inputs have defaults, set them as attributes
        def addDefaultsToApplet(apl:IR.Applet, wdlNameTrail:String) : IR.Applet = {
            val inputsWithDefaults = apl.inputs.map{ cVar =>
                val fqn = s"${wdlNameTrail}.${cVar.name}"
                defaults.fields.get(fqn) match {
                    case None => cVar
                    case Some(dflt:JsValue) =>
                        val jsv = translateValue(dflt)
                        val attrs = cVar.attrs.add("default", jsv)
                        cVar.copy(attrs = attrs)
                }
            }
            apl.copy(inputs = inputsWithDefaults)
        }

        // If a stage has defaults, set the SArg to a constant. The user
        // can override it at runtime.
        def addDefaultsToStage(stg:IR.Stage, wdlNameTrail:String) : IR.Stage = {
            val inputsWithDefaults:Vector[IR.SArg] = stg.inputs.zipWithIndex.map{
                case (sArg,idx) =>
                    val callee:IR.Applet = ns.applets(stg.appletName)
                    val cVar = callee.inputs(idx)
                    val fqn = s"${wdlNameTrail}.${cVar.name}"
                    defaults.fields.get(fqn) match {
                        case None => sArg
                        case Some(dflt:JsValue) =>
                            val jsv = translateValue(dflt)
                            val wvl = WdlVarLinks(cVar.wdlType, cVar.attrs, DxlValue(jsv))
                            val w = WdlVarLinks.eval(wvl, false)
                            IR.SArgConst(w)
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
                        case IR.AppletKindTask => applet.name
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
                IR.Namespace(Some(wfWithDefaults), applets)

            case None =>
                // The namespace comprises tasks only
                val applets = ns.applets.map{ case (name, applet) =>
                    val nameTrail = applet.kind match {
                        case IR.AppletKindTask => applet.name
                        case _ => throw new Exception("sanity")
                    }
                    val apl = addDefaultsToApplet(applet, nameTrail)
                    apl.name -> apl
                }.toMap
                IR.Namespace(None, applets)
        }
    }

    // Build a dx input file, based on the wdl input file and the workflow
    def dxFromCromwell(ns: IR.Namespace,
                       wf: IR.Workflow,
                       inputPath: Path) : JsObject = {
        trace(verbose.on, s"Translating WDL input file ${inputPath}")

        // read the input file xxxx.json
        // skip comment lines, these start with ##.
        val wdlInputs: JsObject = Utils.readFileContent(inputPath).parseJson.asJsObject
        val inputFields = HashMap.empty[String, JsValue]
        wdlInputs.fields.foreach{ case (k,v) =>
            if (!k.startsWith("##"))
                inputFields(k) = v
        }

        // The general idea here is to figure out the ancestry of each
        // applet/call/workflow input. This provides the fully-qualified-name (fqn)
        // of each IR variable. Then we check if the fqn is defined in
        // the input file.
        val workflowBindings = HashMap.empty[String, JsValue]

        // If WDL variable fully qualified name [fqn] was provided in the
        // input file, set [stage.cvar] to its JSON value
        def checkAndBind(fqn:String, dxName:String) : Unit = {
            inputFields.get(fqn) match {
                case None => ()
                case Some(jsv) =>
                    trace(verbose.on, s"${fqn} -> ${dxName}")
                    workflowBindings(dxName) = translateValue(jsv)
                    // Do not assign the value to any later stages.
                    // We found the variable declaration, the others
                    // are variable uses.
                    inputFields -= fqn
            }
        }
        // This works for calls inside an if/scatter block. The naming is:
        //  STAGE_ID.CALL_VARNAME
        def compoundCalls(stage:IR.Stage,
                          calls:Map[String, String]) : Unit = {
            calls.foreach{ case (callName, appletName) =>
                val callee:IR.Applet = ns.applets(appletName)
                callee.inputs.zipWithIndex.foreach{
                    case (cVar,idx) =>
                        val fqn = s"${wf.name}.${callName}.${cVar.name}"
                        val dxName = s"${stage.id.getId}.${callName}_${cVar.name}"
                        checkAndBind(fqn, dxName)
                }
            }
        }

        // make a pass on all the stages
        wf.stages.foreach{ stage =>
            val callee:IR.Applet = ns.applets(stage.appletName)

            // make a pass on all call inputs
            stage.inputs.zipWithIndex.foreach{
                case (_,idx) =>
                    val cVar = callee.inputs(idx)
                    val fqn = s"${wf.name}.${stage.name}.${cVar.name}"
                    val dxName = s"${stage.id.getId}.${cVar.name}"
                    checkAndBind(fqn, dxName)
            }
            // check if the applet called from this stage has bindings
            callee.kind match {
                case IR.AppletKindTask =>
                    // We aren't handling applet settings currently
                    ()
                case other =>
                    // An applet generated from a piece of the workflow.
                    // search for all the applet inputs
                    callee.inputs.foreach{ cVar =>
                        val fqn = s"${wf.name}.${cVar.name}"
                        val dxName = s"${stage.id.getId}.${cVar.name}"
                        checkAndBind(fqn, dxName)
                    }
            }
            // compound stages, for example if blocks, or scatters.
            // They contain calls to applets.
            callee.kind match {
                case IR.AppletKindIf(calls) => compoundCalls(stage, calls)
                case IR.AppletKindScatter(calls) => compoundCalls(stage, calls)
                case IR.AppletKindScatterCollect(calls) => compoundCalls(stage, calls)
                case _ => ()
            }
        }

        if (!inputFields.isEmpty) {
            System.err.println("Could not map all the input fields. These were left:")
            System.err.println(s"${inputFields}")
            throw new Exception("Failed to map all input fields")
        }
        JsObject(workflowBindings.toMap)
    }
}
