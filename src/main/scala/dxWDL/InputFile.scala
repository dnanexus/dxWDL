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
import Utils.{DX_URL_PREFIX, readFileContent, isGeneratedVar, OUTPUT_SECTION, trace}

case class InputFile(verbose: Utils.Verbose) {
    val verbose2:Boolean = verbose.keywords contains "InputFile"

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
    private def translateValue(cVar: CVar,
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
            case None =>
                trace(verbose2, s"getExactlyOnce ${fqn} => None")
                None
            case Some(v:JsValue) =>
                trace(verbose2, s"getExactlyOnce ${fqn} => Some(${v})")
                fields -= fqn
                Some(v)
        }
    }

    // If a stage has defaults, set the SArg to a constant. The user
    // can override it at runtime.
    private def addDefaultsToStage(wf: IR.Workflow,
                                   stg:IR.Stage,
                                   callee: IR.Applet,
                                   defaultFields: HashMap[String, JsValue]) : IR.Stage = {
        trace(verbose2, s"addDefaultToStage ${stg.name}")
        val inputsFull:Vector[(SArg,CVar)] = stg.inputs.zipWithIndex.map{
            case (sArg,idx) =>
                val cVar = callee.inputs(idx)
                (sArg, cVar)
        }
        val nameTrail = callee.kind match {
            case IR.AppletKindNative(_) => s"${wf.name}.${stg.name}"
            case IR.AppletKindTask => s"${wf.name}.${stg.name}"
            case _ => wf.name
        }
        val inputNames = inputsFull.map{ case (_,cVar) => cVar.name }
        trace(verbose2, s"inputNames=${inputNames}  trail=${nameTrail}")
        val inputsWithDefaults: Vector[SArg] = inputsFull.map{ case (sArg, cVar) =>
            if (isGeneratedVar(cVar.name)) {
                // generated variables are not accessible to the user
                sArg
            } else {
                val fqn = cVar.originalFqn match {
                    case None => s"${nameTrail}.${cVar.name}"
                    case Some(fqn) =>
                        trace(verbose2, s"original fqn=${fqn} cVar=${cVar.name}")
                        s"${wf.name}.${fqn}"
                }
                getExactlyOnce(defaultFields, fqn) match {
                    case None => sArg
                    case Some(dflt:JsValue) =>
                        val wvl = translateValue(cVar, dflt)
                        val w = WdlVarLinks.eval(wvl, IOMode.Remote, IODirection.Zero)
                        IR.SArgConst(w)
                }
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
            val fqn = s"${wfName}.${cVar.name}"
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
                      wf: IR.Workflow,
                      defaultInputs: Path) : IR.Namespace = {
        trace(verbose.on, s"Embedding defaults into the IR")

        // read the default inputs file (xxxx.json)
        val wdlDefaults: JsObject = readFileContent(defaultInputs).parseJson.asJsObject
        val defaultFields:HashMap[String,JsValue] = preprocessInputs(wdlDefaults)

        val stagesWithDefaults = wf.stages.map{ stage =>
            val callee:IR.Applet = ns.applets(stage.appletName)
            val visible = callee.kind match {
                case IR.AppletKindWorkflowOutputReorg => false
                case _ if (stage.name == OUTPUT_SECTION) => false
                case _ => true
            }
            if (visible) {
                // user can see these stages, and set defaults
                addDefaultsToStage(wf, stage, callee, defaultFields)
            } else {
                stage
            }
        }
        val wfInputsWithDefaults =
            if (wf.locked) {
                // Locked workflows, we have workflow level inputs
                addDefaultsToWorkflowInputs(wf.inputs, wf.name, defaultFields)
            } else {
                wf.inputs
            }
        val wf2 = wf.copy(inputs = wfInputsWithDefaults,
                          stages = stagesWithDefaults)
        val irNs = IR.Namespace(Some(wf2), ns.applets)

        if (!defaultFields.isEmpty) {
            System.err.println("Could not map all default fields. These were left:")
            System.err.println(s"${defaultFields}")
            throw new Exception("Failed to map all default fields")
        }
        irNs
    }

    // Build a dx input file, based on the JSON input file and the workflow
    def dxFromCromwell(ns: IR.Namespace,
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
        def compoundCalls(wfName: String,
                          stage:IR.Stage,
                          calls:Map[String, String]) : Unit = {
            calls.foreach{ case (callName, appletName) =>
                val callee:IR.Applet = ns.applets(appletName)
                callee.inputs.zipWithIndex.foreach{
                    case (cVar,idx) =>
                        val fqn = s"${wfName}.${callName}.${cVar.name}"
                        val dxName = s"${stage.id.getId}.${callName}_${cVar.name}"
                        checkAndBind(fqn, dxName, cVar)
                }
            }
        }

        def handleWorkflow(wf: IR.Workflow) : Unit = {
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
                        val fqn = s"${wf.name}.${cVar.name}"
                        val dxName = s"${cVar.name}"
                        checkAndBind(fqn, dxName, cVar)
                    }
                } else {
                    // Unlocked workflow.
                    //
                    // compound stages, for example if blocks, or scatters.
                    // They contain calls to applets.
                    callee.kind match {
                        case IR.AppletKindIf(calls) => compoundCalls(wf.name, stage, calls)
                        case IR.AppletKindScatter(calls) => compoundCalls(wf.name, stage, calls)
                        case IR.AppletKindScatterCollect(calls) => compoundCalls(wf.name, stage, calls)
                        case _ => ()
                    }
                }
            }
        }
        ns.applets.map{ case (aplName, applet) =>
            applet.inputs.foreach { cVar =>
                val fqn = s"${aplName}.${cVar.name}"
                val dxName = s"${cVar.name}"
                checkAndBind(fqn, dxName, cVar)
            }
        }
        ns.workflow match {
            case None => ()
            case Some(wf) => handleWorkflow(wf)
        }

        if (!inputFields.isEmpty) {
            System.err.println("Could not map all the input fields. These were left:")
            System.err.println(s"${inputFields}")
            throw new Exception("Failed to map all input fields")
        }
        JsObject(workflowBindings.toMap)
    }
}
