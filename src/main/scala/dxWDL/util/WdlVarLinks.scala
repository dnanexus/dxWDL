/**

Conversions from WDL types and data structures to DNAx JSON
representations. There are two difficulties this module needs to deal
with: (1) WDL has high order types which DNAx does not, and (2) the
file type is very different between WDL and DNAx.

  */
package dxWDL.util

import com.dnanexus.{DXFile, DXExecution}
import spray.json._
import wom.types._
import wom.values._

import dxWDL.base._

// A union of all the different ways of building a value
// from JSON passed by the platform.
//
// A complex values is a WDL values that does not map to a native dx:type. Such
// values may also have files embedded in them. For example:
//  - Ragged file array:  Array[Array[File]]
//  - Object with file elements
//  - Map of files:     Map[String, File]
// A complex value is implemented as a json structure, and an array of
// all the files it references.
sealed trait DxLink
case class DxlValue(jsn: JsValue) extends DxLink  // This may contain dx-files
case class DxlStage(dxStage: DXWorkflowStage, ioRef: IORef.Value, varName: String) extends DxLink
case class DxlWorkflowInput(varName: String) extends DxLink
case class DxlExec(dxExec: DXExecution, varName: String) extends DxLink

case class WdlVarLinks(womType: WomType, dxlink: DxLink)

case class WdlVarLinksConverter(typeAliases: Map[String, WomType]) {

    private def getRawJsValue(wvl: WdlVarLinks) : JsValue = {
        wvl.dxlink match {
            case DxlValue(jsn) => jsn
            case _ =>
                throw new AppInternalException(
                    s"Unsupported conversion from ${wvl.dxlink} to WomValue")
        }
    }

    private def isDxFile(jsValue: JsValue): Boolean = {
        jsValue match {
            case JsObject(fields) =>
                fields.get("$dnanexus_link") match {
                    case Some(JsString(s)) if s.startsWith("file-") => true
                    case Some(JsObject(linkFields)) =>
                        linkFields.get("id") match {
                            case Some(JsString(s)) if s.startsWith("file-") => true
                            case _ => false
                        }
                    case _ => false
                }
            case  _ => false
        }
    }

    // Search through a JSON value for all the dx:file links inside it. Returns
    // those as a vector.
    def findDxFiles(jsValue: JsValue) : Vector[DXFile] = {
        jsValue match {
            case JsBoolean(_) | JsNumber(_) | JsString(_) | JsNull =>
                Vector.empty[DXFile]
            case JsObject(_) if isDxFile(jsValue) =>
                Vector(Utils.dxFileFromJsValue(jsValue))
            case JsObject(fields) =>
                fields.map{ case(_,v) => findDxFiles(v) }.toVector.flatten
            case JsArray(elems) =>
                elems.map(e => findDxFiles(e)).flatten
        }
    }

    // find all dx:files referenced from the variable
    def findDxFiles(wvl: WdlVarLinks) : Vector[DXFile] = {
        findDxFiles(getRawJsValue(wvl))
    }

    // Get the file-id
    def getDxFile(wvl: WdlVarLinks) : DXFile = {
        assert(Utils.stripOptional(wvl.womType) == WomSingleFileType)
        wvl.dxlink match {
            case DxlValue(jsn) =>
                val dxFiles = findDxFiles(jsn)
                assert(dxFiles.length == 1)
                dxFiles.head
            case _ =>
                throw new Exception("cannot get file-id from non-JSON")
        }
    }

    private def isDoubleOptional(t: WomType) : Boolean = {
        t match {
            case WomOptionalType(WomOptionalType(_)) => true
            case _ => false
        }
    }


    // Serialize a complex WDL value into a JSON value. The value could potentially point
    // to many files. The assumption is that files are already in the format of dxWDLs,
    // requiring not upload/download or any special conversion.
    private def jsFromWomValue(womType: WomType, womValue: WomValue) : JsValue = {
        if (isDoubleOptional(womType) ||
                isDoubleOptional(womValue.womType)) {
            System.err.println(s"""|jsFromWomValue
                                   |    type=${womType}
                                   |    val=${womValue.toWomString}
                                   |    val.type=${womValue.womType}
                                   |""".stripMargin)
            throw new Exception("a double optional type")
        }
        def handleFile(path:String) : JsValue =  {
            Furl.parse(path) match {
                case FurlDx(path) =>
                    val dxFile = DxPath.lookupDxURLFile(path)
                    Utils.dxFileToJsValue(dxFile)
                case FurlLocal(path) =>
                    // A local file.
                    JsString(path)
            }
        }
        (womType, womValue) match {
            // Base case: primitive types
            case (WomSingleFileType, WomString(path)) => handleFile(path)
            case (WomSingleFileType, WomSingleFile(path)) => handleFile(path)
            case (WomStringType, WomSingleFile(path)) => JsString(path)
            case (WomStringType, WomString(buf)) =>
                if (buf.length > Utils.MAX_STRING_LEN)
                    throw new AppInternalException(s"string is longer than ${Utils.MAX_STRING_LEN}")
                JsString(buf)
            case (WomBooleanType,WomBoolean(b)) => JsBoolean(b)
            case (WomBooleanType,WomString("true")) => JsBoolean(true)
            case (WomBooleanType,WomString("false")) => JsBoolean(false)

            // Integer conversions
            case (WomIntegerType,WomInteger(n)) => JsNumber(n)
            case (WomIntegerType,WomString(s)) => JsNumber(s.toInt)
            case (WomIntegerType,WomFloat(x)) => JsNumber(x.toInt)

            // Float conversions
            case (WomFloatType, WomFloat(x)) => JsNumber(x)
            case (WomFloatType, WomInteger(n)) => JsNumber(n.toFloat)
            case (WomFloatType, WomString(s)) => JsNumber(s.toFloat)

            case (WomPairType(lType, rType), WomPair(l,r)) =>
                val lJs = jsFromWomValue(lType, l)
                val rJs = jsFromWomValue(rType, r)
                JsObject("left" -> lJs, "right" -> rJs)

            case (WomMapType(WomStringType, valueType), WomMap(_, m)) =>
                // keys are strings
                JsObject(m.map{
                             case (WomString(k), v) =>
                                 k -> jsFromWomValue(valueType, v)
                             case (k,_) =>
                                 throw new Exception(s"key ${k.toWomString} should be a WomString")
                         }.toMap)

            // Maps. These are projections from a key to value, where
            // the key and value types are statically known. We
            // represent them in JSON as an array of keys, followed by
            // an array of values.
            case (WomMapType(keyType, valueType), WomMap(_, m)) =>
                // general case
                val keys:WomValue = WomArray(WomArrayType(keyType), m.keys.toVector)
                val kJs = jsFromWomValue(keys.womType, keys)
                val values:WomValue = WomArray(WomArrayType(valueType), m.values.toVector)
                val vJs = jsFromWomValue(values.womType, values)
                JsObject("keys" -> kJs, "values" -> vJs)

            // Arrays: these come after maps, because there is an automatic coercion from
            // a map to an array.
            //
            // Base case: empty array
            case (_, WomArray(_, ar)) if ar.length == 0 =>
                JsArray(Vector.empty)
            case (WomArrayType(t), null) =>
                JsArray(Vector.empty)

            // Non empty array
            case (WomArrayType(t), WomArray(_, elems)) =>
                val jsVals = elems.map{ x => jsFromWomValue(t, x) }
                JsArray(jsVals.toVector)

            // Strip optional type
            case (WomOptionalType(t), WomOptionalValue(_,Some(w))) =>
                jsFromWomValue(t, w)
            case (WomOptionalType(t), WomOptionalValue(_,None)) =>
                JsNull
            case (WomOptionalType(t), w) =>
                jsFromWomValue(t, w)
            case (t, WomOptionalValue(_,Some(w))) =>
                jsFromWomValue(t, w)

            // structs
            case (WomCompositeType(typeMap, None), _) =>
                throw new Exception("struct without a name")

            case (WomCompositeType(typeMap, Some(structName)),
                  WomObject(m: Map[String, WomValue], _)) =>
                // Convert each of the elements
                val mJs = m.map{ case (key, womValue) =>
                    val elemType = typeMap(key)
                    key -> jsFromWomValue(elemType, womValue)
                }.toMap
                // retain the struct name
                JsObject("type" -> JsString(WomTypeSerialization(typeAliases).toString(womType)),
                         "value" -> JsObject(mJs))

            case (_,_) =>
                val womTypeStr =
                    if (womType == null)
                        "null"
                    else
                        WomPrettyPrintApproxWdl.typeName(womType)
                val womValueStr =
                    if (womValue == null)
                        "null"
                    else
                        s"(${womValue.toWomString}, ${womValue.womType})"
                throw new Exception(s"""|Unsupported combination:
                                        |    womType:  ${womTypeStr}
                                        |    womValue: ${womValueStr}""".stripMargin)
        }
    }

    // import a WDL value
    def importFromWDL(womType: WomType,
                      womValue: WomValue) : WdlVarLinks = {
        val jsValue = jsFromWomValue(womType, womValue)
        WdlVarLinks(womType, DxlValue(jsValue))
    }


    // Dx allows hashes as an input/output type. If the JSON value is
    // not a hash (js-object), we need to add an outer layer to it.
    private def jsValueToDxHash(womType: WomType, jsVal: JsValue) : JsValue = {
        val m:Map[String, JsValue] = jsVal match {
            case JsObject(fields) =>
                assert(!(fields contains "womType"))
                fields
            case _ =>
                // Embed the value into a JSON object
                Map("value" -> jsVal)
        }
        val typeStr = WomTypeSerialization(typeAliases).toString(womType)
        val mWithType = m + ("womType" -> JsString(typeStr))
        JsObject(mWithType)
    }

    private def unmarshalHash(jsv:JsValue) : (WomType, JsValue) = {
        jsv match {
            case JsObject(fields) =>
                // An object, the type is embedded as a 'womType' field
                fields.get("womType") match {
                    case Some(JsString(s)) =>
                        val t = WomTypeSerialization(typeAliases).fromString(s)
                        if (fields contains "value") {
                            // the value is encapsulated in the "value" field
                            (t, fields("value"))
                        } else {
                            // strip the womType field
                            (t, JsObject(fields - "womType"))
                        }
                    case _ => throw new Exception(
                        s"missing or malformed womType field in ${jsv}")
                }
            case other =>
                throw new Exception(s"JSON ${jsv} does not match the marshalled WDL value")
        }
    }

    // Import a value we got as an output from a dx:executable. In this case,
    // we don't have the dx:specification, but we do have the WomType.
    def importFromDxExec(womType:WomType,
                         jsValue: JsValue) : WdlVarLinks = {
        if (Utils.isNativeDxType(womType)) {
            WdlVarLinks(womType, DxlValue(jsValue))
        } else {
            val (womType2, v) = unmarshalHash(jsValue)
            assert(womType2 == womType)
            WdlVarLinks(womType, DxlValue(v))
        }
    }

    def importFromCromwellJSON(womType: WomType,
                               jsv: JsValue) : WdlVarLinks = {
        WdlVarLinks(womType, DxlValue(jsv))
    }

    // create input/output fields that bind the variable name [bindName] to
    // this WdlVar
    def genFields(wvl : WdlVarLinks,
                  bindName: String,
                  encodeDots: Boolean = true) : List[(String, JsValue)] = {
        def nodots(s: String) : String =
            Utils.encodeAppletVarName(Utils.transformVarName(s))
        val bindEncName =
            if (encodeDots) nodots(bindName)
            else bindName
        def mkSimple() : (String, JsValue) = {
            val jsv : JsValue = wvl.dxlink match {
                case DxlValue(jsn) => jsn
                case DxlStage(dxStage, ioRef, varEncName) =>
                    ioRef match {
                        case IORef.Input => dxStage.getInputReference(nodots(varEncName))
                        case IORef.Output => dxStage.getOutputReference(nodots(varEncName))
                    }
                case DxlWorkflowInput(varEncName) =>
                    JsObject("$dnanexus_link" -> JsObject(
                                 "workflowInputField" -> JsString(nodots(varEncName))))
                case DxlExec(dxJob, varEncName) =>
                    Utils.makeEBOR(dxJob, nodots(varEncName))
            }
            (bindEncName, jsv)
        }
        def mkComplex(womType: WomType) : Map[String, JsValue] = {
            val bindEncName_F = bindEncName + Utils.FLAT_FILES_SUFFIX
            wvl.dxlink match {
                case DxlValue(jsn) =>
                    // files that are embedded in the structure
                    val dxFiles = findDxFiles(jsn)
                    val jsFiles = dxFiles.map(x => Utils.jsValueOfJsonNode(x.getLinkAsJson))
                    // convert the top level structure into a hash
                    val hash = jsValueToDxHash(womType, jsn)
                    Map(bindEncName -> hash,
                        bindEncName_F -> JsArray(jsFiles))
                case DxlStage(dxStage, ioRef, varEncName) =>
                    val varEncName_F = varEncName + Utils.FLAT_FILES_SUFFIX
                    ioRef match {
                        case IORef.Input => Map(
                            bindEncName -> dxStage.getInputReference(varEncName),
                            bindEncName_F -> dxStage.getInputReference(varEncName_F)
                        )
                        case IORef.Output => Map(
                            bindEncName -> dxStage.getOutputReference(varEncName),
                            bindEncName_F -> dxStage.getOutputReference(varEncName_F)
                        )
                    }
                case DxlWorkflowInput(varEncName) =>
                    val varEncName_F = varEncName + Utils.FLAT_FILES_SUFFIX
                    Map( bindEncName ->
                            JsObject("$dnanexus_link" -> JsObject(
                                         "workflowInputField" -> JsString(varEncName))),
                         bindEncName_F ->
                            JsObject("$dnanexus_link" -> JsObject(
                                         "workflowInputField" -> JsString(varEncName_F)))
                    )
                case DxlExec(dxJob, varEncName) =>
                    val varEncName_F = varEncName + Utils.FLAT_FILES_SUFFIX
                    Map(bindEncName -> Utils.makeEBOR(dxJob, nodots(varEncName)),
                        bindEncName_F -> Utils.makeEBOR(dxJob, nodots(varEncName_F)))
            }
        }

        val womType = Utils.stripOptional(wvl.womType)
        if (Utils.isNativeDxType(womType)) {
            // Types that are supported natively in DX
            List(mkSimple())
        } else {
            // General complex type requiring two fields: a JSON
            // structure, and a flat array of files.
            mkComplex(womType).toList
        }
    }
}
