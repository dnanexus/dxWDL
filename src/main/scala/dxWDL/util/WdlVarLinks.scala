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
import dxWDL.dx.{DxDescribe, DXWorkflowStage, DxUtils}

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

case class WdlVarLinksConverter(fileInfoDir: Map[DXFile, DxDescribe],
                                typeAliases: Map[String, WomType]) {
    val womTypeSerializer = WomTypeSerialization(typeAliases)

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
                case FurlDx(path, _, dxFile) =>
                    DxUtils.dxFileToJsValue(dxFile)
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
                    val elemType = typeMap.get(key) match {
                        case None =>
                            throw new Exception(s"""|ERROR
                                                    |WomCompositeType
                                                    |  structName=${structName}
                                                    |  typeMap=${typeMap}
                                                    |  wom-object=${m}
                                                    |typeMap is missing key=${key}
                                                    |""".stripMargin)
                        case Some(t) => t
                    }
                    key -> jsFromWomValue(elemType, womValue)
                }.toMap
                // retain the struct name
                JsObject("type" -> JsString(womTypeSerializer.toString(womType)),
                         "value" -> JsObject(mJs))

            case (_,_) =>
                val womTypeStr =
                    if (womType == null)
                        "null"
                    else
                        WomTypeSerialization.typeName(womType)
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

    def importFromCromwellJSON(womType: WomType,
                               jsv: JsValue) : WdlVarLinks = {
        WdlVarLinks(womType, DxlValue(jsv))
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
        val typeStr = womTypeSerializer.toString(womType)
        val mWithType = m + ("womType" -> JsString(typeStr))
        JsObject(mWithType)
    }

    // Convert a job input to a WomValue. Do not download any files, convert them
    // to a string representation. For example: dx://proj-xxxx:file-yyyy::/A/B/C.txt
    //
    private def jobInputToWomValue(name: String,
                                   womType: WomType,
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
                val dxFile = DxUtils.dxFileFromJsValue(jsValue)
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
                        val kWom = jobInputToWomValue(name, keyType, k)
                        val vWom = jobInputToWomValue(name, valueType, v)
                        kWom -> vWom
                }.toMap
                WomMap(WomMapType(keyType, valueType), m)

            case (WomPairType(lType, rType), JsObject(fields))
                    if (List("left", "right").forall(fields contains _)) =>
                val left = jobInputToWomValue(name, lType, fields("left"))
                val right = jobInputToWomValue(name, rType, fields("right"))
                WomPair(left, right)

            // empty array
            case (WomArrayType(t), JsNull) =>
                WomArray(WomArrayType(t), List.empty[WomValue])

            // array
            case (WomArrayType(t), JsArray(vec)) =>
                val wVec: Seq[WomValue] = vec.map{
                    elem:JsValue => jobInputToWomValue(name, t, elem)
                }
                WomArray(WomArrayType(t), wVec)

            case (WomOptionalType(t), JsNull) =>
                WomOptionalValue(t, None)
            case (WomOptionalType(t), jsv) =>
                val value = jobInputToWomValue(name, t, jsv)
                WomOptionalValue(t, Some(value))

            // structs
            case (WomCompositeType(typeMap, Some(structName)), JsObject(fields)) =>
                val m : Map[String, WomValue] = fields.map{
                    case (key, jsValue) =>
                        val t = typeMap.get(key) match {
                            case None =>
                                throw new Exception(s"""|ERROR
                                                        |WomCompositeType
                                                        |  structName=${structName}
                                                        |  typeMap=${typeMap}
                                                        |  fields=${fields}
                                                        |typeMap is missing key=${key}
                                                        |""".stripMargin)
                            case Some(t) => t
                        }
                        key -> jobInputToWomValue(key, t, jsValue)
                }.toMap
                WomObject(m, WomCompositeType(typeMap, Some(structName)))

            case _ =>
                throw new AppInternalException(
                    s"""|Unsupported combination
                        |  name:    ${name}
                        |  womType: ${womType}
                        |  JSON:    ${jsValue.prettyPrint}
                        |""".stripMargin)
        }
    }

    def unpackJobInput(name: String, womType: WomType, jsv: JsValue) : (WomValue,
                                                                        Vector[DXFile]) = {
        if (DxUtils.isNativeDxType(womType)) {
            // no unpacking is needed, this is a primitive, or an array of primitives.
            // it is directly mapped to dnanexus types.
            val womValue = jobInputToWomValue(name, womType, jsv)
            val dxFiles = DxUtils.findDxFiles(jsv)
            return (womValue, dxFiles)
        }

        // unpack the hash with which complex JSON values are
        // wrapped in dnanexus.
        val fields = jsv match {
            case JsObject(fields) => fields
            case other =>
                throw new Exception(s"JSON ${jsv} does not match the marshalled WDL value")
        }

        // An object, the type is embedded as a 'womType' field
        fields.get("womType") match {
            case Some(JsString(s)) =>
                val t = womTypeSerializer.fromString(s)
                assert(t == womType)
            case _ => throw new Exception(s"missing or malformed womType field in ${jsv}")
        }

        val jsv1 =
            if (fields contains "value") {
                    // the value is encapsulated in the "value" field
                fields("value")
            } else {
                // strip the womType field
                JsObject(fields - "womType")
            }
        val womValue = jobInputToWomValue(name, womType, jsv1)
        val dxFiles = DxUtils.findDxFiles(jsv1)
        (womValue, dxFiles)
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
                    DxUtils.makeEBOR(dxJob, nodots(varEncName))
            }
            (bindEncName, jsv)
        }
        def mkComplex(womType: WomType) : Map[String, JsValue] = {
            val bindEncName_F = bindEncName + Utils.FLAT_FILES_SUFFIX
            wvl.dxlink match {
                case DxlValue(jsn) =>
                    // files that are embedded in the structure
                    val dxFiles = DxUtils.findDxFiles(jsn)
                    val jsFiles = dxFiles.map(x => DxUtils.jsValueOfJsonNode(x.getLinkAsJson))
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
                    Map(bindEncName -> DxUtils.makeEBOR(dxJob, nodots(varEncName)),
                        bindEncName_F -> DxUtils.makeEBOR(dxJob, nodots(varEncName_F)))
            }
        }

        val womType = Utils.stripOptional(wvl.womType)
        if (DxUtils.isNativeDxType(womType)) {
            // Types that are supported natively in DX
            List(mkSimple())
        } else {
            // General complex type requiring two fields: a JSON
            // structure, and a flat array of files.
            mkComplex(womType).toList
        }
    }
}
