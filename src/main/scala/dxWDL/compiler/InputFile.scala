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
import dxWDL.util.{DxPath, Utils, Verbose, WdlVarLinks}
import IR.{CVar}
import scala.collection.mutable.HashMap
import java.nio.file.Path
import spray.json._
import wom.types._

case class InputFile(verbose: Verbose) {
    val verbose2:Boolean = verbose.keywords contains "InputFile"


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
    private def importFromCromwell(womType: WomType,
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
                             k -> importFromCromwell(valueType, v)
                         })

            case (WomPairType(lType, rType), JsArray(Vector(l,r))) =>
                val lJs = importFromCromwell(lType, l)
                val rJs = importFromCromwell(rType, r)
                JsObject("left" -> lJs, "right" -> rJs)

            case (WomObjectType, _) =>
                throw new Exception(
                    s"""|WDL Objects are not supported when converting from JSON inputs
                        |type = ${womType.toDisplayString}
                        |value = ${jsv.prettyPrint}
                        |""".stripMargin.trim)

            case (WomArrayType(t), JsArray(vec)) =>
                JsArray(vec.map{
                    elem => importFromCromwell(t, elem)
                })

            case (WomOptionalType(t), (null|JsNull)) => JsNull
            case (WomOptionalType(t), _) =>  importFromCromwell(t, jsv)

            case _ =>
                throw new Exception(
                    s"""|Unsupported/Invalid type/JSON combination in input file
                        |  womType= ${womType.toDisplayString}
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
        val js2 = importFromCromwell(cVar.womType, jsWithDxLinks)
        WdlVarLinks.importFromCromwellJSON(cVar.womType, cVar.attrs, js2)
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

    // Converting a Cromwell style input JSON file, into a valid DNAx input file
    //
    case class CromwellInputFileState(inputFields: HashMap[String,JsValue],
                                      dxKeyValues: HashMap[String, JsValue]) {
        private def getExactlyOnce(fqn: String) : Option[JsValue] = {
            inputFields.get(fqn) match {
                case None =>
                    Utils.trace(verbose2, s"getExactlyOnce ${fqn} => None")
                    None
                case Some(v:JsValue) =>
                    Utils.trace(verbose2, s"getExactlyOnce ${fqn} => Some(${v})")
                    inputFields -= fqn
                    Some(v)
            }
        }

        // If WDL variable fully qualified name [fqn] was provided in the
        // input file, set [stage.cvar] to its JSON value
        def checkAndBind(fqn:String, dxName:String, cVar:IR.CVar) : Unit = {
            getExactlyOnce(fqn) match {
                case None => ()
                case Some(jsv) =>
                    // Do not assign the value to any later stages.
                    // We found the variable declaration, the others
                     // are variable uses.
                    Utils.trace(verbose.on, s"${fqn} -> ${dxName}")
                    val wvl = translateValue(cVar, jsv)
                    WdlVarLinks.genFields(wvl, dxName, encodeDots=false)
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

        bundle.primaryCallable match {
            case None => ()
            case Some(applet: IR.Applet) =>
                // There is one task, we can generate one input file for it.
                applet.inputs.foreach { cVar =>
                    val fqn = s"${applet.name}.${cVar.name}"
                    val dxName = s"${cVar.name}"
                    cif.checkAndBind(fqn, dxName, cVar)
                }
            case _ =>
                throw new Exception(s"Workflows aren't handled yet")
        }
        cif.checkAllUsed()

        Utils.traceLevelDec()

        JsObject(cif.dxKeyValues.toMap)
    }
}
