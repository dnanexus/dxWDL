/** Generate an input file for a dx:workflow based on the
  JSON input file. Also deal with setting default values for
a workflow and tasks.

For example, this is a input file for workflow optionals:
{
  "optionals.arg1": 10,
  "optionals.mul2.i": 5,
  "optionals.add.a" : 1,
  "optionals.add.b" : 3
}

This is the dx JSON input:
{
  "arg1": 10,
  "stage-xxxx.i": 5,
  "stage-yyyy.a": 1,
  "stage-yyyy.b": 3
}
  */
package dxWDL

import com.dnanexus.{DXDataObject, DXFile}
import scala.collection.mutable.HashMap
import IR.{CVar, SArg}
import java.nio.file.Path
import spray.json._
import Utils.{COMMON, DX_URL_PREFIX, FLAT_FILES_SUFFIX, readFileContent, trace}

case class InputFile(verbose: Utils.Verbose) {
    // traverse the JSON structure, and replace file URLs with
    // dx-links.
    private def replaceURLsWithLinks(jsv: JsValue) : JsValue = {
        jsv match {
            case JsString(s) if s.startsWith(DX_URL_PREFIX) =>
                // Identify platform file paths by their prefix,
                // do a lookup, and create a dxlink
                val dxFile: DXDataObject = DxPath.lookupDxURLFile(s)
                Utils.dxFileToJsValue(dxFile.asInstanceOf[DXFile])

            case JsBoolean(_) | JsNull | JsNumber(_) | JsString(_) => jsv
            case JsObject(fields) =>
                JsObject(fields.map{
                             case(k,v) => k -> replaceURLsWithLinks(v)
                         }.toMap)
            case JsArray(elems) =>
                JsArray(elems.map(e => replaceURLsWithLinks(e)))
            case _ =>
                throw new Exception(s"unrecognized JSON value=${jsv}")
        }
    }

    // Import into a WDL value, and convert back to JSON.
    private def translateValue(cVar: IR.CVar,
                               jsv: JsValue) : WdlVarLinks = {
        val jsWithDxLinks = replaceURLsWithLinks(jsv)
        WdlVarLinks.importFromCromwellJSON(cVar.wdlType, cVar.attrs, jsWithDxLinks)
    }

    private def preprocessInputs(obj: JsObject) : HashMap[String, JsValue] = {
        val inputFields = HashMap.empty[String, JsValue]
        obj.fields.foreach{ case (k,v) =>
            if (!k.startsWith("##"))
                inputFields(k) = v
        }
        inputFields
    }

    private def getExactlyOnce(fields: HashMap[String, JsValue],
                               fqn: String) : Option[JsValue] = {
        fields.get(fqn) match {
            case None => None
            case Some(v:JsValue) =>
                fields -= fqn
                Some(v)
        }
    }

        // if applet inputs have defaults, set them as attributes
    private def addDefaultsToApplet(apl: IR.Applet,
                                    wdlNameTrail: String,
                                    defaultFields: HashMap[String, JsValue]) : IR.Applet = {
        val inputsWithDefaults = apl.inputs.map{ cVar =>
            val fqn = s"${wdlNameTrail}.${cVar.name}"
            getExactlyOnce(defaultFields, fqn) match {
                case None => cVar
                case Some(dflt:JsValue) =>
                    val wvl = translateValue(cVar, dflt)

                    // We want a single JSON value representing
                    // the variable. Discard the flat-files companion array.
                    val jsValues:Seq[JsValue] =
                        WdlVarLinks.genFields( wvl, "__dummy__")
                            .filter { case (name, jsv) => !name.endsWith(FLAT_FILES_SUFFIX) }
                            .map{ case (_, jsv) => jsv}
                    assert (jsValues.size == 1)
                    val attrs = cVar.attrs.add("default", jsValues.head)
                    cVar.copy(attrs = attrs)
            }
        }
        apl.copy(inputs = inputsWithDefaults)
    }

        // If a stage has defaults, set the SArg to a constant. The user
        // can override it at runtime.
    private def addDefaultsToStage(ns: IR.Namespace,
                                   stg:IR.Stage,
                                   wdlNameTrail:String,
                                   defaultFields: HashMap[String, JsValue]) : IR.Stage = {
        val inputsWithDefaults:Vector[IR.SArg] = stg.inputs.zipWithIndex.map{
            case (sArg,idx) =>
                val callee:IR.Applet = ns.applets(stg.appletName)
                val cVar = callee.inputs(idx)
                val fqn = s"${wdlNameTrail}.${cVar.name}"
                getExactlyOnce(defaultFields, fqn) match {
                    case None => sArg
                    case Some(dflt:JsValue) =>
                        val wvl = translateValue(cVar, dflt)
                        val w = WdlVarLinks.eval(wvl, IOMode.Remote, IODirection.Zero)
                        IR.SArgConst(w)
                }
        }
        stg.copy(inputs = inputsWithDefaults)
    }

        // Set defaults for workflow inputs.
    private def addDefaultsToWorkflowInputs(inputs:Vector[(CVar, SArg)],
                                            wfName:String,
                                            defaultFields: HashMap[String, JsValue])
            : Vector[(CVar, SArg)] = {
        inputs.map { case (cVar, sArg) =>
            val fqn = sArg match {
                case IR.SArgWorkflowInput(_,Some(orgName)) => s"${wfName}.${orgName}"
                case _ =>  s"${wfName}.${cVar.name}"
            }
            val sArgDflt = getExactlyOnce(defaultFields, fqn) match {
                case None => sArg
                case Some(dflt:JsValue) =>
                    val wvl = translateValue(cVar, dflt)
                    val w = WdlVarLinks.eval(wvl, IOMode.Remote, IODirection.Zero)
                    IR.SArgConst(w)
            }
            (cVar, sArgDflt)
        }.toVector
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
        val wdlDefaults: JsObject = readFileContent(defaultInputs).parseJson.asJsObject
        val defaultFields:HashMap[String,JsValue] = preprocessInputs(wdlDefaults)

        // figure out the WDL ancestry of an applet. What piece
        // of WDL code is the source for this applet?
        val irNs = ns.workflow match {
            case Some(wf)  =>
                // add defaults to workflow inputs
                val wfWithDefaults =
                    if (wf.locked) {
                        // Locked workflows, we have workflow level inputs
                        val inputs = addDefaultsToWorkflowInputs(wf.inputs, wf.name, defaultFields)
                        val stagesWithDefaults = wf.stages.map{ stg =>
                            val nameTrail = s"${wf.name}.${stg.name}"
                            addDefaultsToStage(ns, stg, nameTrail, defaultFields)
                        }
                        wf.copy(inputs = inputs, stages = stagesWithDefaults)
                    } else {
                        // Unlocked workflow, the common stage
                        // replaces the workflow inputs stage. We
                        // need maintain the stage order, and keep the common
                        // stage first
                        val commonStage:Option[IR.Stage] =
                            wf.stages.find(_.name == COMMON).map { stg =>
                                addDefaultsToStage(ns, stg, wf.name, defaultFields)
                            }
                        val midStagesWithDefaults = wf.stages.filter(_.name != COMMON).map {
                            case stg =>
                                val nameTrail = s"${wf.name}.${stg.name}"
                                addDefaultsToStage(ns, stg, nameTrail, defaultFields)
                        }
                        val allStages = commonStage match {
                            case None => midStagesWithDefaults
                            case Some(cmn) => cmn +: midStagesWithDefaults
                        }
                        wf.copy(stages = allStages)
                    }
                val appletsWithDefaults = ns.applets.map{ case (name, applet) =>
                    val nameTrail = applet.kind match {
                        case IR.AppletKindTask =>
                            // A WDL task implemented as an applet
                            applet.name
                        case _ =>
                            // An auxiliary applet, for example, implementing a scatter/if block.
                            wf.name
                    }
                    val apl = addDefaultsToApplet(applet, nameTrail, defaultFields)
                    apl.name -> apl
                }.toMap
                IR.Namespace(Some(wfWithDefaults), appletsWithDefaults)

            case None =>
                // The namespace comprises tasks only
                val applets = ns.applets.map{ case (name, applet) =>
                    val nameTrail = applet.kind match {
                        case IR.AppletKindTask => applet.name
                        case _ => throw new Exception("sanity")
                    }
                    val apl = addDefaultsToApplet(applet, nameTrail, defaultFields)
                    apl.name -> apl
                }.toMap
                IR.Namespace(None, applets)
        }

        if (!defaultFields.isEmpty) {
            System.err.println("Could not map all default fields. These were left:")
            System.err.println(s"${defaultFields}")
            throw new Exception("Failed to map all default fields")
        }
        irNs
    }

    // Build a dx input file, based on the JSON input file and the workflow
    def dxFromCromwell(ns: IR.Namespace,
                       wf: IR.Workflow,
                       inputPath: Path) : JsObject = {
        trace(verbose.on, s"Translating WDL input file ${inputPath}")

        // read the input file xxxx.json
        // skip comment lines, these start with ##.
        val wdlInputs: JsObject = readFileContent(inputPath).parseJson.asJsObject
        val inputFields:HashMap[String,JsValue] = preprocessInputs(wdlInputs)

        // The general idea here is to figure out the ancestry of each
        // applet/call/workflow input. This provides the fully-qualified-name (fqn)
        // of each IR variable. Then we check if the fqn is defined in
        // the input file.
        val workflowBindings = HashMap.empty[String, JsValue]

        // If WDL variable fully qualified name [fqn] was provided in the
        // input file, set [stage.cvar] to its JSON value
        def checkAndBind(fqn:String, dxName:String, cVar:IR.CVar) : Unit = {
            getExactlyOnce(inputFields, fqn) match {
                case None => ()
                case Some(jsv) =>
                    // Do not assign the value to any later stages.
                    // We found the variable declaration, the others
                    // are variable uses.
                    trace(verbose.on, s"${fqn} -> ${dxName}")
                    val wvl = translateValue(cVar, jsv)
                    WdlVarLinks.genFields(wvl, dxName, encodeDots=false)
                        .foreach{ case (name, jsv) => workflowBindings(name) = jsv }
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
                        checkAndBind(fqn, dxName, cVar)
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
                    checkAndBind(fqn, dxName, cVar)
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
                        checkAndBind(fqn, dxName, cVar)
                    }
            }

            if (wf.locked) {
                // Locked workflow. A user can set workflow level
                // inputs; nothing else.
                wf.inputs.foreach { case (cVar, sArg) =>
                    val fqn = sArg match {
                        case IR.SArgWorkflowInput(_,Some(orgName)) => s"${wf.name}.${orgName}"
                        case _ =>  s"${wf.name}.${cVar.name}"
                    }
                    val dxName = s"${cVar.name}"
                    checkAndBind(fqn, dxName, cVar)
                }
            } else {
                // Unlocked workflow.
                //
                // compound stages, for example if blocks, or scatters.
                // They contain calls to applets.
                callee.kind match {
                    case IR.AppletKindIf(calls) => compoundCalls(stage, calls)
                    case IR.AppletKindScatter(calls) => compoundCalls(stage, calls)
                    case IR.AppletKindScatterCollect(calls) => compoundCalls(stage, calls)
                    case _ => ()
                }
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
