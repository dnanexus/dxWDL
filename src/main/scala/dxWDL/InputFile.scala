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

import com.dnanexus.DXDataObject
import scala.collection.mutable.HashMap
import IR.{CVar, SArg}
import java.nio.file.Path
import spray.json._
import Utils.{trace, DX_URL_PREFIX, FLAT_FILES_SUFFIX}

case class InputFile(verbose: Utils.Verbose) {
    // traverse the JSON structure, and replace file URLs with
    // dx-links.
    private def replaceURLsWithLinks(jsv: JsValue) : JsValue = {
        jsv match {
            case JsString(s) if s.startsWith(DX_URL_PREFIX) =>
                // Identify platform file paths by their prefix,
                // do a lookup, and create a dxlink
                val dxFile: DXDataObject = DxPath.lookupDxURLFile(s)
                Utils.jsValueOfJsonNode(dxFile.getLinkAsJson)

            case JsBoolean(_) | JsNull | JsNumber(_) | JsString(_) => jsv
            case JsObject(fields) =>
                JsObject(fields.map{
                             case(k,v) => k -> replaceURLsWithLinks(v)
                         }.toMap)
            case JsArray(elems) =>
                JsArray(elems.map(e => replaceURLsWithLinks(e)))
        }
    }

    // Import into a WDL value, and convert back to JSON.
    private def translateValue(cVar: CVar,
                               jsv: JsValue) : WdlVarLinks = {
        val jsWithDxLinks = replaceURLsWithLinks(jsv)
        WdlVarLinks.importFromCromwellJSON(cVar.wdlType, cVar.attrs, jsWithDxLinks)
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
        def addDefaultsToStage(stg:IR.Stage, wdlNameTrail:String) : IR.Stage = {
            val inputsWithDefaults:Vector[SArg] = stg.inputs.zipWithIndex.map{
                case (sArg,idx) =>
                    val callee:IR.Applet = ns.applets(stg.appletName)
                    val cVar = callee.inputs(idx)
                    val fqn = s"${wdlNameTrail}.${cVar.name}"
                    defaults.fields.get(fqn) match {
                        case None => sArg
                        case Some(dflt:JsValue) =>
                            val wvl = translateValue(cVar, dflt)
                            val w = WdlVarLinks.eval(wvl, false, IODirection.Zero)
                            IR.SArgConst(w)
                    }
            }
            stg.copy(inputs = inputsWithDefaults)
        }

        // Set defaults for workflow inputs.
        def addDefaultsToWorkflowInputs(inputs:Vector[(CVar, SArg)],
                                        wfName:String) : Vector[(CVar, SArg)] = {
            inputs.map { case (cVar, sArg) =>
                val fqn = s"${wfName}.${cVar.name}"
                val sArgDflt = defaults.fields.get(fqn) match {
                    case None => sArg
                    case Some(dflt:JsValue) =>
                        val wvl = translateValue(cVar, dflt)
                        val w = WdlVarLinks.eval(wvl, false, IODirection.Zero)
                        IR.SArgConst(w)
                }
                (cVar, sArgDflt)
            }.toVector
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
                // add defaults to workflow inputs
                val inputs = addDefaultsToWorkflowInputs(wf.inputs, wf.name)

                val wfWithDefaults = wf.copy(inputs = inputs, stages = stages)
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

    // Build a dx input file, based on the JSON input file and the workflow
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
        def checkAndBind(fqn:String, dxName:String, cVar:CVar) : Unit = {
            inputFields.get(fqn) match {
                case None => ()
                case Some(jsv) =>
                    trace(verbose.on, s"${fqn} -> ${dxName}")
                    val wvl = translateValue(cVar, jsv)
                    WdlVarLinks.genFields(wvl, dxName, encodeDots=false)
                        .foreach{ case (name, jsv) => workflowBindings(name) = jsv }

                    // Do not assign the value to any later stages.
                    // We found the variable declaration, the others
                    // are variable uses.
                    inputFields -= fqn
            }
        }

        wf.inputs.foreach { case (cVar, sArg) =>
            val fqn = s"${wf.name}.${cVar.name}"
            val dxName = s"${cVar.name}"
            checkAndBind(fqn, dxName, cVar)
        }

        if (!inputFields.isEmpty) {
            System.err.println("Could not map all the input fields. These were left:")
            System.err.println(s"${inputFields}")
            throw new Exception("Failed to map all input fields")
        }
        JsObject(workflowBindings.toMap)
    }
}
