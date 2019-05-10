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
import IR.{CVar, SArg, COMMON, OUTPUT_SECTION, REORG}
import scala.collection.mutable.HashMap
import java.nio.file.Path
import spray.json._
import wom.types._
import wom.values._

import dxWDL.util._

// scan a Cromwell style JSON input file, and return all the dx:files in it
case class InputFileScan(bundle: IR.Bundle,
                         verbose: Verbose) {

    private def findDxFiles(womType: WomType,
                            jsValue: JsValue) : Vector[DXFile] = {
        (womType, jsValue)  match {
            // base case: primitive types
            case (WomBooleanType, _) => Vector.empty
            case (WomIntegerType, _) => Vector.empty
            case (WomFloatType, _) => Vector.empty
            case (WomStringType, _) => Vector.empty
            case (WomSingleFileType, JsString(s)) =>
                Furl.parse(s) match {
                    case FurlDx(_, _, dxFile) => Vector(dxFile)
                    case _ => Vector.empty
                }
            case (WomSingleFileType, JsObject(_)) =>
                Vector(Utils.dxFileFromJsValue(jsValue))

            // Maps. These are serialized as an object with a keys array and
            // a values array.
            case (WomMapType(keyType, valueType), _) =>
                val (keysJs, valuesJs) = jsValue.asJsObject.getFields("keys", "values") match {
                    case Seq(JsArray(keys), JsArray(values)) => (keys, values)
                    case _ => throw new Exception("malformed JSON")
                }
                val kFiles = keysJs.flatMap(findDxFiles(keyType, _))
                val vFiles = valuesJs.flatMap(findDxFiles(valueType, _))
                kFiles.toVector ++ vFiles.toVector

            case (WomPairType(lType, rType), JsObject(fields))
                    if (List("left", "right").forall(fields contains _)) =>
                val lFiles = findDxFiles(lType, fields("left"))
                val rFiles = findDxFiles(rType, fields("right"))
                lFiles ++ rFiles

            case (WomArrayType(t), JsArray(vec)) =>
                vec.flatMap( findDxFiles(t, _ ))

            case (WomOptionalType(t), JsNull) =>
                Vector.empty

            case (WomOptionalType(t), jsv) =>
                findDxFiles(t, jsv)

            // structs
            case (WomCompositeType(typeMap, Some(structName)), JsObject(fields)) =>
                fields.flatMap {
                    case (key, value) =>
                        val t : WomType = typeMap(key)
                        findDxFiles(t, value)
                }.toVector

            case _ =>
                throw new AppInternalException(
                    s"Unsupported combination ${womType} ${jsValue.prettyPrint}")
        }
    }

    private def findFilesForApplet(inputs: Map[String, JsValue],
                                   applet: IR.Applet) : Vector[DXFile] = {
        applet.inputs.flatMap{
            case cVar =>
                val fqn = s"${applet.name}.${cVar.name}"
                inputs.get(fqn) match {
                    case None => None
                    case Some(jsValue) =>
                        Some(findDxFiles(cVar.womType, jsValue))
                }
        }.flatten.toVector
    }

    private def findFilesForWorkflow(inputs: Map[String, JsValue],
                                     wf: IR.Workflow) : Vector[DXFile] = {
        wf.inputs.flatMap {
            case (cVar, _) =>
                val fqn = s"${wf.name}.${cVar.name}"
                inputs.get(fqn) match {
                    case None => None
                    case Some(jsValue) =>
                        Some(findDxFiles(cVar.womType, jsValue))
                }
        }.flatten.toVector
    }

    def apply(inputPath: Path) : Vector[DXFile] = {
        // Read the JSON values from the file
        val content = Utils.readFileContent(inputPath).parseJson
        val inputs: Map[String, JsValue] = content.asJsObject.fields

        val allCallables: Vector[IR.Callable] =
            bundle.primaryCallable.toVector ++ bundle.allCallables.map(_._2)

        // Match each field with its WOM type, this allows
        // accurately finding dx-files.
        val files = allCallables.map {
            case applet :IR.Applet =>
                findFilesForApplet(inputs, applet)
            case wf :IR.Workflow =>
                findFilesForWorkflow(inputs, wf)
        }.flatten.toVector

        // make files unique
        files.toSet.toVector
    }
}

case class InputFile(fileInfoDir: Map[DXFile, DxBulkDescribe.MiniDescribe],
                     typeAliases: Map[String, WomType],
                     verbose: Verbose) {
    val verbose2:Boolean = verbose.containsKey("InputFile")
    private val wdlVarLinksConverter = WdlVarLinksConverter(fileInfoDir, typeAliases)

    // Convert a job input to a WomValue. Do not download any files, convert them
    // to a string representation. For example: dx://proj-xxxx:file-yyyy::/A/B/C.txt
    //
    private def womValueFromCromwellJSON(womType: WomType,
                                         jsValue: JsValue) : WomValue = {
        (womType, jsValue)  match {
            // base case: primitive types
            case (WomBooleanType, JsBoolean(b)) => WomBoolean(b.booleanValue)
            case (WomIntegerType, JsNumber(bnm)) => WomInteger(bnm.intValue)
            case (WomFloatType, JsNumber(bnm)) => WomFloat(bnm.doubleValue)
            case (WomStringType, JsString(s)) => WomString(s)
            case (WomSingleFileType, JsString(s)) => WomSingleFile(s)
            case (WomSingleFileType, JsObject(_)) =>
                // Convert the path in DNAx to a string. We can later
                // decide if we want to download it or not
                val dxFile = Utils.dxFileFromJsValue(jsValue)
                val FurlDx(s, _, _) = Furl.dxFileToFurl(dxFile, fileInfoDir)
                WomSingleFile(s)

            // Maps. These are serialized as an object with a keys array and
            // a values array.
            case (WomMapType(keyType, valueType), _) =>
                val fields = jsValue.asJsObject.fields
                // [mJs] is a map from json key to json value
                val mJs: Map[JsValue, JsValue] =
                    (fields("keys"), fields("values")) match {
                        case (JsArray(x), JsArray(y)) =>
                            assert(x.length == y.length)
                            (x zip y).toMap
                        case _ => throw new Exception("Malformed JSON")
                    }
                val m: Map[WomValue, WomValue] = mJs.map {
                    case (k:JsValue, v:JsValue) =>
                        val kWom = womValueFromCromwellJSON(keyType, k)
                        val vWom = womValueFromCromwellJSON(valueType, v)
                        kWom -> vWom
                }.toMap
                WomMap(WomMapType(keyType, valueType), m)

            case (WomPairType(lType, rType), JsObject(fields))
                    if (List("left", "right").forall(fields contains _)) =>
                val left = womValueFromCromwellJSON(lType, fields("left"))
                val right = womValueFromCromwellJSON(rType, fields("right"))
                WomPair(left, right)

            // empty array
            case (WomArrayType(t), JsNull) =>
                WomArray(WomArrayType(t), List.empty[WomValue])

            // array
            case (WomArrayType(t), JsArray(vec)) =>
                val wVec: Seq[WomValue] = vec.map{
                    elem:JsValue => womValueFromCromwellJSON(t, elem)
                }
                WomArray(WomArrayType(t), wVec)

            case (WomOptionalType(t), JsNull) =>
                WomOptionalValue(t, None)
            case (WomOptionalType(t), jsv) =>
                val value = womValueFromCromwellJSON(t, jsv)
                WomOptionalValue(t, Some(value))

            // structs
            case (WomCompositeType(typeMap, Some(structName)), JsObject(fields)) =>
                // convert each field
                val m = fields.map {
                    case (key, value) =>
                        val t : WomType = typeMap(key)
                        val elem: WomValue = womValueFromCromwellJSON(t, value)
                        key -> elem
                }.toMap
                WomObject(m, WomCompositeType(typeMap, Some(structName)))

            case _ =>
                throw new AppInternalException(
                    s"Unsupported combination ${womType} ${jsValue.prettyPrint}"
                )
        }
    }

    // Import a value specified in a Cromwell style JSON input
    // file. Assume that all the platform files have already been
    // converted into dx:links.
    //
    // Challenges:
    // 1) avoiding an intermediate conversion into a WDL value. Most
    // types pose no issues. However, dx:files cannot be converted
    // into WDL files in all cases.
    // 2) JSON maps and WDL maps are slighly different. WDL maps can have
    // keys of any type, where JSON maps can only have string keys.
    private def jsValueFromCromwellJSON(womType: WomType,
                                        jsv: JsValue) : JsValue = {
        (womType, jsv) match {
            // base case: primitive types
            case (WomBooleanType, JsBoolean(_)) => jsv
            case (WomIntegerType, JsNumber(_)) => jsv
            case (WomFloatType, JsNumber(_)) => jsv
            case (WomStringType, JsString(_)) => jsv
            case (WomSingleFileType, JsObject(_)) => jsv

            // Maps. Since these have string values, they are, essentially,
            // mapped to WDL maps of type Map[String, T].
            case (WomMapType(keyType, valueType), JsObject(fields)) =>
                if (keyType != WomStringType)
                    throw new Exception("Importing a JSON object to a WDL map requires string keys")
                JsObject(fields.map{ case (k,v) =>
                             k -> jsValueFromCromwellJSON(valueType, v)
                         })

            case (WomPairType(lType, rType), JsArray(Vector(l,r))) =>
                val lJs = jsValueFromCromwellJSON(lType, l)
                val rJs = jsValueFromCromwellJSON(rType, r)
                JsObject("left" -> lJs, "right" -> rJs)

            case (WomArrayType(t), JsArray(vec)) =>
                JsArray(vec.map{
                    elem => jsValueFromCromwellJSON(t, elem)
                })

            case (WomOptionalType(t), (null|JsNull)) => JsNull
            case (WomOptionalType(t), _) =>  jsValueFromCromwellJSON(t, jsv)

            // structs
            case (WomCompositeType(typeMap, Some(structName)), JsObject(fields)) =>
                // convert each field
                val m = fields.map {
                    case (key, value) =>
                        val t : WomType = typeMap(key)
                        val elem: JsValue = jsValueFromCromwellJSON(t, value)
                        key -> elem
                }.toMap
                JsObject(m)

            case _ =>
                throw new Exception(
                    s"""|Unsupported/Invalid type/JSON combination in input file
                        |  womType= ${womType}
                        |  JSON= ${jsv.prettyPrint}""".stripMargin.trim)
        }
    }

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
        val jsWithDxLinks: JsValue = replaceURLsWithLinks(jsv)
        val js2 = jsValueFromCromwellJSON(cVar.womType, jsWithDxLinks)
        wdlVarLinksConverter.importFromCromwellJSON(cVar.womType, js2)
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
        Utils.trace(verbose2, s"addDefaultToStage ${stg.id.getId}, ${stg.description}")
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
                    val w : WomValue = womValueFromCromwellJSON(cVar.womType, dflt)
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
                    val w : WomValue = womValueFromCromwellJSON(cVar.womType, dflt)
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
                        val w : WomValue = womValueFromCromwellJSON(cVar.womType, dflt)
                        cVar.copy(default = Some(w))
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
                    if (stg.id.getId == s"stage-${COMMON}") {
                        addDefaultsToStage(stg, wf.name, callee, defaultFields)
                    } else {
                        addDefaultsToStage(stg, s"${wf.name}.${stg.id.getId}", callee, defaultFields)
                    }
                }
                wf.copy(stages = stagesWithDefaults)
            }

        // check that the stage order hasn't changed
        val allStageNames = wf.stages.map{ _.id }.toVector
        val embedAllStageNames = wfWithDefaults.stages.map{ _.id }.toVector
        assert(allStageNames == embedAllStageNames)

        wfWithDefaults
    }

    // Embed default values into the IR
    //
    // Make a sequential pass on the IR, figure out the fully qualified names
    // of all CVar and SArgs. If they have a default value, embed it into the IR.
    def embedDefaults(bundle: IR.Bundle,
                      defaultInputs: Path) : IR.Bundle = {
        Utils.trace(verbose.on, s"Embedding defaults into the IR")

        // read the default inputs file (xxxx.json)
        val wdlDefaults: JsObject = Utils.readFileContent(defaultInputs).parseJson.asJsObject
        val defaultFields:HashMap[String,JsValue] = preprocessInputs(wdlDefaults)

        val callablesWithDefaults = bundle.allCallables.map {
            case (name, callable) =>
                val callableWithDefaults = callable match {
                    case applet : IR.Applet =>
                        embedDefaultsIntoTask(applet, defaultFields)
                    case subwf : IR.Workflow =>
                        embedDefaultsIntoWorkflow(subwf, bundle.allCallables, defaultFields)
                }
                name -> callableWithDefaults
        }.toMap
        val primaryCallable = bundle.primaryCallable match {
            case None => None
            case Some(applet : IR.Applet) =>
                Some(embedDefaultsIntoTask(applet, defaultFields))
            case Some(wf : IR.Workflow) =>
                Some(embedDefaultsIntoWorkflow(wf, bundle.allCallables, defaultFields))
        }
        if (!defaultFields.isEmpty) {
            throw new Exception(s"""|Could not map all default fields.
                                    |These were left: ${defaultFields}""".stripMargin)
        }
        bundle.copy(primaryCallable = primaryCallable,
                    allCallables = callablesWithDefaults)
    }

    // Converting a Cromwell style input JSON file, into a valid DNAx input file
    //
    case class CromwellInputFileState(inputFields: HashMap[String,JsValue],
                                      dxKeyValues: HashMap[String, JsValue]) {
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
                    wdlVarLinksConverter.genFields(wvl, dxName, encodeDots=false)
                        .foreach{ case (name, jsv) => dxKeyValues(name) = jsv }
            }
        }

        // Check if all the input fields were actually used. Otherwise, there are some
        // key/value pairs that were not translated to DNAx.
        def checkAllUsed() : Unit = {
            if (inputFields.isEmpty)
                return
            throw new Exception(s"""|Could not map all default fields.
                                    |These were left: ${inputFields}""".stripMargin)
        }
    }


    // Build a dx input file, based on the JSON input file and the workflow
    //
    // The general idea here is to figure out the ancestry of each
    // applet/call/workflow input. This provides the fully-qualified-name (fqn)
    // of each IR variable. Then we check if the fqn is defined in
    // the input file.
    def dxFromCromwell(bundle: IR.Bundle,
                       inputPath: Path) : JsObject = {
        Utils.trace(verbose.on, s"Translating WDL input file ${inputPath}")
        Utils.traceLevelInc()

        // read the input file xxxx.json
        val wdlInputs: JsObject = Utils.readFileContent(inputPath).parseJson.asJsObject
        val inputFields:HashMap[String,JsValue] = preprocessInputs(wdlInputs)
        val cif = CromwellInputFileState(inputFields, HashMap.empty)

        def handleTask(applet: IR.Applet) : Unit = {
            applet.inputs.foreach { cVar =>
                val fqn = s"${applet.name}.${cVar.name}"
                val dxName = s"${cVar.name}"
                cif.checkAndBind(fqn, dxName, cVar)
            }
        }

        // If there is one task, we can generate one input file for it.
        val tasks : Vector[IR.Applet] = bundle.allCallables.collect{
            case (_, callable : IR.Applet) => callable
        }.toVector

        bundle.primaryCallable match {
            // File with WDL tasks only, no workflows
            case None if tasks.size == 0 =>
                ()
            case None if tasks.size == 1 =>
                handleTask(tasks.head)
            case None =>
                throw new Exception(s"Cannot generate one input file for ${tasks.size} tasks")
            case Some(task: IR.Applet) =>
                handleTask(task)

            case Some(wf : IR.Workflow) if wf.locked =>
                // Locked workflow. A user can set workflow level
                // inputs; nothing else.
                wf.inputs.foreach { case (cVar, sArg) =>
                    val fqn = s"${wf.name}.${cVar.name}"
                    val dxName = s"${cVar.name}"
                    cif.checkAndBind(fqn, dxName, cVar)
                }
            case Some(wf : IR.Workflow) if wf.stages.isEmpty =>
                // edge case: workflow, with zero stages
                ()

            case Some(wf : IR.Workflow) =>
                // unlocked workflow with at least one stage.
                // Workflow inputs go into the common stage
                val commonStage = wf.stages.head.id.getId
                wf.inputs.foreach { case (cVar, _) =>
                    val fqn = s"${wf.name}.${cVar.name}"
                    val dxName = s"${commonStage}.${cVar.name}"
                    cif.checkAndBind(fqn, dxName, cVar)
                }

                // Inputs for top level calls
                val auxStages = Set(s"stage-${COMMON}",
                                    s"stage-${OUTPUT_SECTION}",
                                    s"stage-${REORG}")
                val middleStages = wf.stages.filter{ stg =>
                    !(auxStages contains stg.id.getId)
                }
                middleStages.foreach{ stg =>
                    // Find the input definitions for the stage, by locating the callee
                    val callee : IR.Callable = bundle.allCallables.get(stg.calleeName) match {
                        case None =>
                            throw new Exception(s"callable ${stg.calleeName} is missing")
                        case Some(x) => x
                    }
                    callee.inputVars.foreach { cVar =>
                        val fqn = s"${wf.name}.${stg.id.getId}.${cVar.name}"
                        val dxName = s"${stg.id.getId}.${cVar.name}"
                        cif.checkAndBind(fqn, dxName, cVar)
                    }
                }

            case other =>
                throw new Exception(s"Unknown case ${other.getClass}")
        }

        cif.checkAllUsed()

        Utils.traceLevelDec()

        JsObject(cif.dxKeyValues.toMap)
    }
}
