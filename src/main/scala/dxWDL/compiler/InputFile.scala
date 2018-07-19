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
package dxWDL.compiler

import com.dnanexus.{DXDataObject, DXFile}
import dxWDL._
import IR.{CVar, SArg}
import scala.collection.mutable.HashMap
import java.nio.file.Path
import spray.json._


case class InputFile(verbose: Verbose) {
    val verbose2:Boolean = verbose.keywords contains "InputFile"

    // traverse the JSON structure, and replace file URLs with
    // dx-links.
    private def replaceURLsWithLinks(jsv: JsValue) : JsValue = {
        jsv match {
            case JsString(s) if s.startsWith(Utils.DX_URL_PREFIX) =>
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
        WdlVarLinks.importFromCromwellJSON(cVar.womType, cVar.attrs, jsWithDxLinks)
    }

    // skip comment lines, these start with ##.
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
                Utils.trace(verbose2, s"getExactlyOnce ${fqn} => None")
                None
            case Some(v:JsValue) =>
                Utils.trace(verbose2, s"getExactlyOnce ${fqn} => Some(${v})")
                fields -= fqn
                Some(v)
        }
    }

    // If a stage has defaults, set the SArg to a constant. The user
    // can override it at runtime.
    private def addDefaultsToStage(stg: IR.Stage,
                                   prefix: String,
                                   callee: IR.Callable,
                                   defaultFields: HashMap[String, JsValue]) : IR.Stage = {
        Utils.trace(verbose2, s"addDefaultToStage ${stg.stageName}")
        val inputsFull:Vector[(SArg,CVar)] = stg.inputs.zipWithIndex.map{
            case (sArg,idx) =>
                val cVar = callee.inputVars(idx)
                (sArg, cVar)
        }
        val inputsWithDefaults: Vector[SArg] = inputsFull.map{ case (sArg, cVar) =>
            val fqn = s"${prefix}.${cVar.name}"
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

    // set defaults for a task
    private def embedDefaultsIntoTask(applet: IR.Applet,
                                      defaultFields:HashMap[String,JsValue]) : IR.Applet = {
        val inputsWithDefaults: Vector[CVar] = applet.inputs.map {
            case cVar =>
                val fqn = s"${applet.name}.${cVar.name}"
                getExactlyOnce(defaultFields, fqn) match {
                    case None =>
                        cVar
                    case Some(dflt:JsValue) =>
                        val wvl = translateValue(cVar, dflt)
                        val w = WdlVarLinks.eval(wvl, IOMode.Remote, IODirection.Zero)
                        cVar.copy(attrs = cVar.attrs.setDefault(w))
                }
        }.toVector
        applet.copy(inputs = inputsWithDefaults)
    }

    // Embed default values into the workflow IR
    //
    // Make a sequential pass on the IR, figure out the fully qualified names
    // of all CVar and SArgs. If they have a default value, add it as an attribute
    // (DeclAttrs).
    private def embedDefaultsIntoWorkflow(wf: IR.Workflow,
                                          callables: Map[String, IR.Callable],
                                          defaultFields:HashMap[String,JsValue]) : IR.Workflow = {
        val wfWithDefaults =
            if (wf.locked) {
                // Locked workflows, we have workflow level inputs
                val wfInputsWithDefaults = addDefaultsToWorkflowInputs(wf.inputs, wf.name,
                                                                       defaultFields)
                wf.copy(inputs = wfInputsWithDefaults)
            } else {
                // Workflow is unlocked, we don't have proper
                // workflow level inputs. Instead, set the defaults in the COMMON stage

                val stagesWithDefaults = wf.stages.map{ stg =>
                    val callee:IR.Callable = callables(stg.calleeName)
                    if (stg.stageName == Utils.COMMON) {
                        addDefaultsToStage(stg, wf.name, callee, defaultFields)
                    } else {
                        addDefaultsToStage(stg, s"${wf.name}.${stg.stageName}", callee, defaultFields)
                    }
                }
                wf.copy(stages = stagesWithDefaults)
            }

        // check that the stage order hasn't changed
        val allStageNames = wf.stages.map{ stg => stg.stageName }.toVector
        val embedAllStageNames = wfWithDefaults.stages.map{ stg => stg.stageName }.toVector
        assert(allStageNames == embedAllStageNames)

        wfWithDefaults
    }

    // Embed default values into the IR
    //
    // Make a sequential pass on the IR, figure out the fully qualified names
    // of all CVar and SArgs. If they have a default value, add it as an attribute
    // (DeclAttrs).
    def embedDefaults(ns: IR.Namespace,
                      defaultInputs: Path) : IR.Namespace = {
        Utils.trace(verbose.on, s"Embedding defaults into the IR")

        // read the default inputs file (xxxx.json)
        val wdlDefaults: JsObject = Utils.readFileContent(defaultInputs).parseJson.asJsObject
        val defaultFields:HashMap[String,JsValue] = preprocessInputs(wdlDefaults)
        val callables = ns.buildCallables

        val appletsWithDefaults = ns.applets.map{ case (name, apl) =>
            val apl2 = apl.kind match {
                case IR.AppletKindTask => embedDefaultsIntoTask(apl, defaultFields)
                case _ => apl
            }
            name -> apl2
        }.toMap
        val subWorkflowsWithDefaults = ns.subWorkflows.map{ case (name, subwf) =>
            name -> embedDefaultsIntoWorkflow(subwf, callables, defaultFields)
        }.toMap
        val entryPoint = ns.entrypoint match {
            case None => None
            case Some(wf) => Some(embedDefaultsIntoWorkflow(wf, callables, defaultFields))
        }
        if (!defaultFields.isEmpty) {
            throw new Exception(s"""|Could not map all default fields.
                                    |These were left: ${defaultFields}""".stripMargin)
        }
        ns.copy(entrypoint = entryPoint,
                subWorkflows = subWorkflowsWithDefaults,
                applets = appletsWithDefaults)
    }

    // Converting a Cromwell style input JSON file, into a valid DNAx input file
    //
    case class CromwellInputFileState(inputFields: HashMap[String,JsValue],
                                      workflowBindings: HashMap[String, JsValue]) {
        // If WDL variable fully qualified name [fqn] was provided in the
        // input file, set [stage.cvar] to its JSON value
        def checkAndBind(fqn:String, dxName:String, cVar:IR.CVar) : Unit = {
            getExactlyOnce(inputFields, fqn) match {
                case None => ()
                case Some(jsv) =>
                    // Do not assign the value to any later stages.
                    // We found the variable declaration, the others
                     // are variable uses.
                    Utils.trace(verbose.on, s"${fqn} -> ${dxName}")
                    val wvl = translateValue(cVar, jsv)
                    WdlVarLinks.genFields(wvl, dxName, encodeDots=false)
                        .foreach{ case (name, jsv) => workflowBindings(name) = jsv }
            }
        }
    }


    // Build a dx input file, based on the JSON input file and the workflow
    //
    // The general idea here is to figure out the ancestry of each
    // applet/call/workflow input. This provides the fully-qualified-name (fqn)
    // of each IR variable. Then we check if the fqn is defined in
    // the input file.
    def dxFromCromwell(ns: IR.Namespace,
                       inputPath: Path) : JsObject = {
        Utils.trace(verbose.on, s"Translating WDL input file ${inputPath}")
        Utils.traceLevelInc()

        // read the input file xxxx.json
        val wdlInputs: JsObject = Utils.readFileContent(inputPath).parseJson.asJsObject
        val inputFields:HashMap[String,JsValue] = preprocessInputs(wdlInputs)
        val cif = CromwellInputFileState(inputFields, HashMap.empty)

        ns.entrypoint match {
            case None if ns.applets.size == 0 =>
                ()
            case None if ns.applets.size == 1 =>
                // There is one task, we can generate one input file for it.
                val (aplName, applet) = ns.applets.head
                applet.inputs.foreach { cVar =>
                    val fqn = s"${aplName}.${cVar.name}"
                    val dxName = s"${cVar.name}"
                    cif.checkAndBind(fqn, dxName, cVar)
                }
            case None =>
                // File with tasks only, where there are two or more tasks.
                throw new Exception(s"Cannot generate one input file for ${ns.applets.size} tasks")
            case Some(wf) if wf.locked =>
                // Locked workflow. A user can set workflow level
                // inputs; nothing else.
                wf.inputs.foreach { case (cVar, sArg) =>
                    val fqn = s"${wf.name}.${cVar.name}"
                    val dxName = s"${cVar.name}"
                    cif.checkAndBind(fqn, dxName, cVar)
                }
            case Some(wf) if wf.stages.isEmpty =>
                // unlocked workflow, no stages
                ()
            case Some(wf) =>
                // unlocked workflow with at least one stage
                // Workflow inputs go into the common stage
                val commonStage = wf.stages.head.id.getId
                wf.inputs.foreach { case (cVar, _) =>
                    val fqn = s"${wf.name}.${cVar.name}"
                    val dxName = s"${commonStage}.${cVar.name}"
                    cif.checkAndBind(fqn, dxName, cVar)
                }

                // Inputs for top level calls
                val callables: Map[String, IR.Callable] = ns.buildCallables
                val middleStages = wf.stages.filter{ stg =>
                    stg.stageName != Utils.COMMON && stg.stageName != Utils.OUTPUT_SECTION
                }
                middleStages.foreach{ stg =>
                    // Find the input definitions for the stage, by locating the callee
                    val callee = callables(stg.calleeName)
                    callee.inputVars.foreach { cVar =>
                        val fqn = s"${wf.name}.${stg.stageName}.${cVar.name}"
                        val dxName = s"${stg.id.getId}.${cVar.name}"
                        cif.checkAndBind(fqn, dxName, cVar)
                    }
                }
        }
        if (!inputFields.isEmpty) {
            throw new Exception(s"""|Could not map all default fields.
                                    |These were left: ${inputFields}""".stripMargin)
        }
        Utils.traceLevelDec()

        JsObject(cif.workflowBindings.toMap)
    }
}
