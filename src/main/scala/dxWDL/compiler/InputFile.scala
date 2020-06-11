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

import java.nio.file.Path

import spray.json._
import wdlTools.eval.WdlValues
import wdlTools.types.WdlTypes
import dxWDL.base._
import dxWDL.dx._
import dxWDL.util._
import IR.{COMMON, CVar, OUTPUT_SECTION, REORG, SArg}

import scala.collection.mutable

// scan a Cromwell style JSON input file, and return all the dx:files in it.
// An input file for workflow foo could look like this:
// {
//   "foo.f": "dx://dxWDL_playground:/test_data/fileB",
//   "foo.f1": "dx://dxWDL_playground:/test_data/fileC",
//   "foo.f2": "dx://dxWDL_playground:/test_data/1/fileC",
//   "foo.fruit_list": "dx://dxWDL_playground:/test_data/fruit_list.txt"
// }
//

case class InputFileScanResults(path2file: Map[String, DxFile], dxFiles: Vector[DxFile])

case class InputFileScan(bundle: IR.Bundle, dxProject: DxProject, verbose: Verbose) {

  private def findDxFiles(womType: WdlTypes.T, jsValue: JsValue): Vector[JsValue] = {
    (womType, jsValue) match {
      // base case: primitive types
      case (WdlTypes.T_Boolean, _) => Vector.empty
      case (WdlTypes.T_Int, _)     => Vector.empty
      case (WdlTypes.T_Float, _)   => Vector.empty
      case (WdlTypes.T_String, _)  => Vector.empty
      case (WdlTypes.T_File, jsv)  => Vector(jsv)

      // Maps. These are serialized as an object with a keys array and
      // a values array.
      case (WdlTypes.T_Map(keyType, valueType), _) =>
        val keysJs = jsValue.asJsObject.fields.keys.map { k =>
          JsString(k)
        }.toVector
        val valuesJs = jsValue.asJsObject.fields.values.toVector
        val kFiles = keysJs.flatMap(findDxFiles(keyType, _))
        val vFiles = valuesJs.flatMap(findDxFiles(valueType, _))
        kFiles ++ vFiles

      // Two ways of writing pairs: an object with left/right fields, or an array
      // with two elements.
      case (WdlTypes.T_Pair(lType, rType), JsObject(fields))
          if List("left", "right").forall(fields.contains) =>
        val lFiles = findDxFiles(lType, fields("left"))
        val rFiles = findDxFiles(rType, fields("right"))
        lFiles ++ rFiles

      case (WdlTypes.T_Pair(lType, rType), JsArray(Vector(l, r))) =>
        val lFiles = findDxFiles(lType, l)
        val rFiles = findDxFiles(rType, r)
        lFiles ++ rFiles

      case (WdlTypes.T_Array(t, _), JsArray(vec)) =>
        vec.flatMap(findDxFiles(t, _))

      case (WdlTypes.T_Optional(_), JsNull) =>
        Vector.empty

      case (WdlTypes.T_Optional(t), jsv) =>
        findDxFiles(t, jsv)

      // structs
      case (WdlTypes.T_Struct(_, typeMap), JsObject(fields)) =>
        fields.flatMap {
          case (key, value) =>
            val t: WdlTypes.T = typeMap(key)
            findDxFiles(t, value)
        }.toVector

      case _ =>
        throw new AppInternalException(s"Unsupported combination ${womType} ${jsValue.prettyPrint}")
    }
  }

  private def findFilesForApplet(inputs: Map[String, JsValue],
                                 applet: IR.Applet): Vector[JsValue] = {
    applet.inputs.flatMap { cVar =>
      val fqn = s"${applet.name}.${cVar.name}"
      inputs.get(fqn) match {
        case None => None
        case Some(jsValue) =>
          Some(findDxFiles(cVar.womType, jsValue))
      }
    }.flatten
  }

  private def findFilesForWorkflow(inputs: Map[String, JsValue],
                                   wf: IR.Workflow): Vector[JsValue] = {
    wf.inputs.flatMap {
      case (cVar, _) =>
        val fqn = s"${wf.name}.${cVar.name}"
        inputs.get(fqn) match {
          case None => None
          case Some(jsValue) =>
            Some(findDxFiles(cVar.womType, jsValue))
        }
    }.flatten
  }

  def apply(inputPath: Path): InputFileScanResults = {
    // Read the JSON values from the file
    val content = Utils.readFileContent(inputPath).parseJson
    val inputs: Map[String, JsValue] = content.asJsObject.fields

    val allCallables: Vector[IR.Callable] =
      bundle.primaryCallable.toVector ++ bundle.allCallables.values

    // Match each field with its WOM type, this allows
    // accurately finding dx-files.
    val jsFileDesc: Vector[JsValue] = allCallables.flatMap {
      case applet: IR.Applet =>
        findFilesForApplet(inputs, applet)
      case wf: IR.Workflow =>
        findFilesForWorkflow(inputs, wf)
    }

    // files that have already been resolved
    val dxFiles: Vector[DxFile] = jsFileDesc.collect {
      case jsv: JsObject => DxUtils.dxFileFromJsValue(jsv)
    }

    // Paths that look like this: "dx://dxWDL_playground:/test_data/fileB".
    // These need to be resolved.
    val dxPaths: Vector[String] = jsFileDesc.collect {
      case JsString(x) => x
    }
    val resolvedPaths = DxPath
      .resolveBulk(dxPaths, dxProject)
      .map {
        case (key, dxobj) if dxobj.isInstanceOf[DxFile] =>
          key -> dxobj.asInstanceOf[DxFile]
        case (_, dxobj) =>
          throw new Exception(s"Scanning the input file produced ${dxobj} which is not a file")
      }

    InputFileScanResults(resolvedPaths, dxFiles ++ resolvedPaths.values)
  }
}

case class InputFile(fileInfoDir: Map[String, (DxFile, DxFileDescribe)],
                     path2file: Map[String, DxFile],
                     typeAliases: Map[String, WdlTypes.T],
                     verbose: Verbose) {
  val verbose2: Boolean = verbose.containsKey("InputFile")
  private val wdlVarLinksConverter = WdlVarLinksConverter(verbose, fileInfoDir, typeAliases)

  // Convert a job input to a WdlValues.V. Do not download any files, convert them
  // to a string representation. For example: dx://proj-xxxx:file-yyyy::/A/B/C.txt
  //
  private def womValueFromCromwellJSON(womType: WdlTypes.T, jsValue: JsValue): WdlValues.V = {
    (womType, jsValue) match {
      // base case: primitive types
      case (WdlTypes.T_Boolean, JsBoolean(b)) => WdlValues.V_Boolean(b.booleanValue)
      case (WdlTypes.T_Int, JsNumber(bnm))    => WdlValues.V_Int(bnm.intValue)
      case (WdlTypes.T_Float, JsNumber(bnm))  => WdlValues.V_Float(bnm.doubleValue)
      case (WdlTypes.T_String, JsString(s))   => WdlValues.V_String(s)
      case (WdlTypes.T_File, JsString(s))     => WdlValues.V_File(s)
      case (WdlTypes.T_File, JsObject(_))     =>
        // Convert the path in DNAx to a string. We can later
        // decide if we want to download it or not
        val dxFile = DxUtils.dxFileFromJsValue(jsValue)
        val FurlDx(s, _, _) = Furl.dxFileToFurl(dxFile, fileInfoDir)
        WdlValues.V_File(s)

      // Maps. These are serialized as an object with a keys array and
      // a values array.
      case (WdlTypes.T_Map(keyType, valueType), _) =>
        val fields = jsValue.asJsObject.fields
        val m: Map[WdlValues.V, WdlValues.V] = fields.map {
          case (k: String, v: JsValue) =>
            val kWom = womValueFromCromwellJSON(keyType, JsString(k))
            val vWom = womValueFromCromwellJSON(valueType, v)
            kWom -> vWom
        }
        WdlValues.V_Map(m)

      // a few ways of writing a pair: an object, or an array
      case (WdlTypes.T_Pair(lType, rType), JsObject(fields))
          if List("left", "right").forall(fields.contains) =>
        val left = womValueFromCromwellJSON(lType, fields("left"))
        val right = womValueFromCromwellJSON(rType, fields("right"))
        WdlValues.V_Pair(left, right)

      case (WdlTypes.T_Pair(lType, rType), JsArray(Vector(l, r))) =>
        val left = womValueFromCromwellJSON(lType, l)
        val right = womValueFromCromwellJSON(rType, r)
        WdlValues.V_Pair(left, right)

      // empty array
      case (WdlTypes.T_Array(_, _), JsNull) =>
        WdlValues.V_Array(Vector.empty[WdlValues.V])

      // array
      case (WdlTypes.T_Array(t, _), JsArray(vec)) =>
        val wVec: Vector[WdlValues.V] = vec.map { elem: JsValue =>
          womValueFromCromwellJSON(t, elem)
        }
        WdlValues.V_Array(wVec)

      case (WdlTypes.T_Optional(_), JsNull) =>
        WdlValues.V_Null
      case (WdlTypes.T_Optional(t), jsv) =>
        val value = womValueFromCromwellJSON(t, jsv)
        WdlValues.V_Optional(value)

      // structs
      case (WdlTypes.T_Struct(structName, typeMap), JsObject(fields)) =>
        // convert each field
        val m = fields.map {
          case (key, value) =>
            val t: WdlTypes.T = typeMap(key)
            val elem: WdlValues.V = womValueFromCromwellJSON(t, value)
            key -> elem
        }
        WdlValues.V_Struct(structName, m)

      case _ =>
        throw new AppInternalException(
            s"Unsupported combination ${womType} ${jsValue.prettyPrint}"
        )
    }
  }

  // Import into a WDL value, and convert back to JSON.
  private def translateValue(cVar: CVar, jsv: JsValue): WdlVarLinks = {
    val womValue = womValueFromCromwellJSON(cVar.womType, jsv)
    wdlVarLinksConverter.importFromWDL(cVar.womType, womValue)
  }

  // skip comment lines, these start with ##.
  private def preprocessInputs(obj: JsObject): mutable.HashMap[String, JsValue] = {
    val inputFields = mutable.HashMap.empty[String, JsValue]
    obj.fields.foreach {
      case (k, v) =>
        if (!k.startsWith("##"))
          inputFields(k) = v
    }
    inputFields
  }

  private def getExactlyOnce(fields: mutable.HashMap[String, JsValue],
                             fqn: String): Option[JsValue] = {
    fields.get(fqn) match {
      case None =>
        Utils.trace(verbose2, s"getExactlyOnce ${fqn} => None")
        None
      case Some(v: JsValue) =>
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
                                 defaultFields: mutable.HashMap[String, JsValue]): IR.Stage = {
    Utils.trace(verbose2, s"addDefaultToStage ${stg.id.getId()}, ${stg.description}")
    val inputsFull: Vector[(SArg, CVar)] = stg.inputs.zipWithIndex.map {
      case (sArg, idx) =>
        val cVar = callee.inputVars(idx)
        (sArg, cVar)
    }
    val inputsWithDefaults: Vector[SArg] = inputsFull.map {
      case (sArg, cVar) =>
        val fqn = s"${prefix}.${cVar.name}"
        getExactlyOnce(defaultFields, fqn) match {
          case None => sArg
          case Some(dflt: JsValue) =>
            val w: WdlValues.V = womValueFromCromwellJSON(cVar.womType, dflt)
            IR.SArgConst(w)
        }
    }
    stg.copy(inputs = inputsWithDefaults)
  }

  // Set defaults for workflow inputs.
  private def addDefaultsToWorkflowInputs(
      inputs: Vector[(CVar, SArg)],
      wfName: String,
      defaultFields: mutable.HashMap[String, JsValue]
  ): Vector[(CVar, SArg)] = {
    inputs.map {
      case (cVar, sArg) =>
        val fqn = s"${wfName}.${cVar.name}"
        val sArgDflt = getExactlyOnce(defaultFields, fqn) match {
          case None => sArg
          case Some(dflt: JsValue) =>
            val w: WdlValues.V = womValueFromCromwellJSON(cVar.womType, dflt)
            IR.SArgConst(w)
        }
        (cVar, sArgDflt)
    }
  }

  // set defaults for a task
  private def embedDefaultsIntoTask(applet: IR.Applet,
                                    defaultFields: mutable.HashMap[String, JsValue]): IR.Applet = {
    val inputsWithDefaults: Vector[CVar] = applet.inputs.map { cVar =>
      val fqn = s"${applet.name}.${cVar.name}"
      getExactlyOnce(defaultFields, fqn) match {
        case None =>
          cVar
        case Some(dflt: JsValue) =>
          val w: WdlValues.V = womValueFromCromwellJSON(cVar.womType, dflt)
          cVar.copy(default = Some(w))
      }
    }
    applet.copy(inputs = inputsWithDefaults)
  }

  // Embed default values into the workflow IR
  //
  // Make a sequential pass on the IR, figure out the fully qualified names
  // of all CVar and SArgs. If they have a default value, add it as an attribute
  // (DeclAttrs).
  private def embedDefaultsIntoWorkflow(
      wf: IR.Workflow,
      callables: Map[String, IR.Callable],
      defaultFields: mutable.HashMap[String, JsValue]
  ): IR.Workflow = {
    val wfWithDefaults =
      if (wf.locked) {
        // Locked workflows, we have workflow level inputs
        val wfInputsWithDefaults = addDefaultsToWorkflowInputs(wf.inputs, wf.name, defaultFields)
        wf.copy(inputs = wfInputsWithDefaults)
      } else {
        // Workflow is unlocked, we don't have proper
        // workflow level inputs. Instead, set the defaults in the COMMON stage

        val stagesWithDefaults = wf.stages.map { stg =>
          val callee: IR.Callable = callables(stg.calleeName)
          if (stg.id.getId == s"stage-${COMMON}") {
            addDefaultsToStage(stg, wf.name, callee, defaultFields)
          } else {
            addDefaultsToStage(stg, s"${wf.name}.${stg.description}", callee, defaultFields)
          }
        }
        wf.copy(stages = stagesWithDefaults)
      }

    // check that the stage order hasn't changed
    val allStageNames = wf.stages.map {
      _.id
    }
    val embedAllStageNames = wfWithDefaults.stages.map {
      _.id
    }
    assert(allStageNames == embedAllStageNames)

    wfWithDefaults
  }

  // Embed default values into the IR
  //
  // Make a sequential pass on the IR, figure out the fully qualified names
  // of all CVar and SArgs. If they have a default value, embed it into the IR.
  def embedDefaults(bundle: IR.Bundle, defaultInputs: Path): IR.Bundle = {
    Utils.trace(verbose.on, s"Embedding defaults into the IR")

    // read the default inputs file (xxxx.json)
    val wdlDefaults: JsObject = Utils.readFileContent(defaultInputs).parseJson.asJsObject
    val defaultFields: mutable.HashMap[String, JsValue] = preprocessInputs(wdlDefaults)

    val callablesWithDefaults = bundle.allCallables.map {
      case (name, callable) =>
        val callableWithDefaults = callable match {
          case applet: IR.Applet =>
            embedDefaultsIntoTask(applet, defaultFields)
          case subwf: IR.Workflow =>
            embedDefaultsIntoWorkflow(subwf, bundle.allCallables, defaultFields)
        }
        name -> callableWithDefaults
    }
    val primaryCallable = bundle.primaryCallable match {
      case None => None
      case Some(applet: IR.Applet) =>
        Some(embedDefaultsIntoTask(applet, defaultFields))
      case Some(wf: IR.Workflow) =>
        Some(embedDefaultsIntoWorkflow(wf, bundle.allCallables, defaultFields))
    }
    if (defaultFields.nonEmpty) {
      throw new Exception(s"""|Could not map all default fields.
                              |These were left: ${defaultFields}""".stripMargin)
    }
    bundle.copy(primaryCallable = primaryCallable, allCallables = callablesWithDefaults)
  }

  // Converting a Cromwell style input JSON file, into a valid DNAx input file
  //
  case class CromwellInputFileState(inputFields: mutable.HashMap[String, JsValue],
                                    dxKeyValues: mutable.HashMap[String, JsValue]) {
    // If WDL variable fully qualified name [fqn] was provided in the
    // input file, set [stage.cvar] to its JSON value
    def checkAndBind(fqn: String, dxName: String, cVar: IR.CVar): Unit = {
      getExactlyOnce(inputFields, fqn) match {
        case None      => ()
        case Some(jsv) =>
          // Do not assign the value to any later stages.
          // We found the variable declaration, the others
          // are variable uses.
          Utils.trace(verbose.on, s"checkAndBind, found: ${fqn} -> ${dxName}")
          val wvl = translateValue(cVar, jsv)
          wdlVarLinksConverter
            .genFields(wvl, dxName, encodeDots = false)
            .foreach { case (name, jsv) => dxKeyValues(name) = jsv }
      }
    }

    // Check if all the input fields were actually used. Otherwise, there are some
    // key/value pairs that were not translated to DNAx.
    def checkAllUsed(): Unit = {
      if (inputFields.isEmpty)
        return
      throw new Exception(s"""|Could not map all input fields.
                              |These were left: ${inputFields}""".stripMargin)
    }
  }

  // Build a dx input file, based on the JSON input file and the workflow
  //
  // The general idea here is to figure out the ancestry of each
  // applet/call/workflow input. This provides the fully-qualified-name (fqn)
  // of each IR variable. Then we check if the fqn is defined in
  // the input file.
  def dxFromCromwell(bundle: IR.Bundle, inputPath: Path): JsObject = {
    Utils.trace(verbose.on, s"Translating WDL input file ${inputPath}")
    Utils.traceLevelInc()

    // read the input file xxxx.json
    val wdlInputs: JsObject = Utils.readFileContent(inputPath).parseJson.asJsObject
    val inputFields: mutable.HashMap[String, JsValue] = preprocessInputs(wdlInputs)
    val cif = CromwellInputFileState(inputFields, mutable.HashMap.empty)

    def handleTask(applet: IR.Applet): Unit = {
      applet.inputs.foreach { cVar =>
        val fqn = s"${applet.name}.${cVar.name}"
        val dxName = s"${cVar.name}"
        cif.checkAndBind(fqn, dxName, cVar)
      }
    }

    // If there is one task, we can generate one input file for it.
    val tasks: Vector[IR.Applet] = bundle.allCallables.collect {
      case (_, callable: IR.Applet) => callable
    }.toVector

    bundle.primaryCallable match {
      // File with WDL tasks only, no workflows
      case None if tasks.isEmpty =>
        ()
      case None if tasks.size == 1 =>
        handleTask(tasks.head)
      case None =>
        throw new Exception(s"Cannot generate one input file for ${tasks.size} tasks")
      case Some(task: IR.Applet) =>
        handleTask(task)

      case Some(wf: IR.Workflow) if wf.locked =>
        // Locked workflow. A user can set workflow level
        // inputs; nothing else.
        wf.inputs.foreach {
          case (cVar, _) =>
            val fqn = s"${wf.name}.${cVar.name}"
            val dxName = s"${cVar.name}"
            cif.checkAndBind(fqn, dxName, cVar)
        }
      case Some(wf: IR.Workflow) if wf.stages.isEmpty =>
        // edge case: workflow, with zero stages
        ()

      case Some(wf: IR.Workflow) =>
        // unlocked workflow with at least one stage.
        // Workflow inputs go into the common stage
        val commonStage = wf.stages.head.id.getId()
        wf.inputs.foreach {
          case (cVar, _) =>
            val fqn = s"${wf.name}.${cVar.name}"
            val dxName = s"${commonStage}.${cVar.name}"
            cif.checkAndBind(fqn, dxName, cVar)
        }

        // filter out auxiliary stages
        val auxStages = Set(s"stage-${COMMON}", s"stage-${OUTPUT_SECTION}", s"stage-${REORG}")
        val middleStages = wf.stages.filter { stg =>
          !(auxStages contains stg.id.getId)
        }

        // Inputs for top level calls
        middleStages.foreach { stage =>
          // Find the input definitions for the stage, by locating the callee
          val callee: IR.Callable = bundle.allCallables.get(stage.calleeName) match {
            case None =>
              throw new Exception(s"callable ${stage.calleeName} is missing")
            case Some(x) => x
          }
          callee.inputVars.foreach { cVar =>
            val fqn = s"${wf.name}.${stage.description}.${cVar.name}"
            val dxName = s"${stage.description}.${cVar.name}"
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
