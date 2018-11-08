package dxWDL.runner

import com.dnanexus.{DXFile}
import dxWDL.util.WomTypeSerialization
import java.nio.file.{Files, Paths}
import spray.json._
import wom.callable.Callable.{InputDefinitionWithDefault, FixedInputDefinition}
import wom.types._
import wom.values._

object JobInputOutput {

    // Convert a job input to a WomValue. Do not download any files, convert them
    // to a string representation. For example: dx://proj-xxxx:file-yyyy::/A/B/C.txt
    //
    private def jobInputToWomValue(womType: WomType,
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
                val dxFile = Utils.dxFileFromJsValue(jsv)
                val s = DxPath.dxFileToURL(dxFile)
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
                        val kWom = jobInputToWomValue(keyType, k)
                        val vWom = jobInputToWomValue(valueType, v)
                        kWom -> vWom
                }.toMap
                WomMap(WomMapType(keyType, valueType), m)

            case (WomPairType(lType, rType), JsObject(fields))
                    if (List("left", "right").forall(fields contains _)) =>
                val left = jobInputToWomValue(lType, fields("left"), ioMode, ioDir)
                val right = jobInputToWomValue(rType, fields("right"), ioMode, ioDir)
                WomPair(left, right)

            case (WomObjectType, JsObject(fields)) =>
                throw new Exception("WOM objects not supported")

            // empty array
            case (WomArrayType(t), JsNull) =>
                WomArray(WomArrayType(t), List.empty[WomValue])

            // array
            case (WomArrayType(t), JsArray(vec)) =>
                val wVec: Seq[WomValue] = vec.map{
                    elem:JsValue => jobInputToWomValue(t, elem, ioMode, ioDir)
                }
                WomArray(WomArrayType(t), wVec)

            case (WomOptionalType(t), JsNull) =>
                WomOptionalValue(t, None)
            case (WomOptionalType(t), jsv) =>
                val value = jobInputToWomValue(t, jsv, ioMode, ioDir)
                WomOptionalValue(t, Some(value))

            case _ =>
                throw new AppInternalException(
                    s"Unsupported combination ${womType} ${jsValue.prettyPrint}"
                )
        }
    }


    private def unmarshalHash(jsv:JsValue) : (WomType, JsValue) = {
        jsv match {
            case JsObject(fields) =>
                // An object, the type is embedded as a 'womType' field
                fields.get("womType") match {
                    case Some(JsString(s)) =>
                        val t = WomTypeSerialization.fromString(s)
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

    // Read the job-inputs JSON file, and convert the variables
    // from JSON to WOM values. Delay downloading the files.
    def loadInputs(inputLines: String,
                   callable: wom.callable.Callable): Map[String, WomValue] = {
        // Discard auxiliary fields
        val jsonAst : JsValue = inputLines.parseJson
        val fields : Map[String, JsValue] = jsonAst
            .asJsObject.fields
            .filter{ case (fieldName,_) => !fieldName.endsWith(Utils.FLAT_FILES_SUFFIX) }

        // Get the declarations matching the input fields.
        // Create a mapping from each key to its WDL value
        callable.inputs.map { inpDfn =>
            val defaultValue: Option[WomValue] = inpDfn match {
                case d: InputDefinitionWithDefault =>
                    Utils.ifConstEval(d.default)
                case d: FixedInputDefinition =>
                    Utils.ifConstEval(d.default)
                case _ => None
            }

            // There are several distinct cases
            //
            //   default   input           result   comments
            //   -------   -----           ------   --------
            //   d         not-specified   d
            //   _         null            None     override
            //   _         Some(v)         Some(v)
            //
            val wv: WomValue = fields.get(inpDfn.name) match {
                case None =>
                    // this key is not specified in the input. Use the default.
                    defaultValue match {
                        case None if Utils.isOptional(inpDfn.womType) =>
                            // Default value was not specified
                            WomOptionalValue(inpDfn.womType, None)
                        case None =>
                            // Default value was not specified, and null is not a valid
                            // value, given the WDL type.
                            throw new Exception(s"Invalid conversion from null to non optional type ${inpDfn.womType}")
                        case Some(x: WomValue) =>
                            x
                    }
                case Some(JsNull) =>
                    // override the default will a null value, even though it exists
                    WomOptionalValue(inpDfn.womType, None)
                case Some(jsv) =>
                    // The field was specified
                    val jsv =
                        if (Utils.isNativeDxType(inpDfn.womType)) {
                            jsValue
                        } else {
                            val (womType2, jsv) = unmarshalHash(jsValue)
                            assert(womType2 == inpDfn.womType)
                            jsv
                        }
                    jobInputToWomValue(inptDfn.womType, jsv)
            }
            inpDfn.name -> wv
        }.toMap
    }

    // find all file URLs in a Wom value
    private def findFiles(v : WomValue) : Vector[String] = {
        v match {
            case (WomSingleFile(url)) => Vector(url)
            case (WomMap(_, m: Map[WomValue, WomValue])) =>
                m.foldLeft(Vector.empty[String]) {
                    case (accu, (k,v)) =>
                        findFiles(k) ++ findFiles(v) ++ accu
                }
            case WomPair(lf, rt) =>
                findFiles(lf) ++ findFiles(rt)
            case WomObjectType =>
                throw new Exception("WOM objects not supported")

            // empty array
            case (WomArray(_, value: Seq[WomValue])) =>
                value.map(findFiles).flatten

            case (WomOptional(_, Some(value))) =>
                findFiles(value)

            case _ => Vector.empty
        }
    }

    // After applying [loadInputs] above, we have the job inputs in the correct format,
    // except for files. These are represented as dx URLs (dx://proj-xxxx:file-yyyy::/A/B/C.txt)
    // instead of local files (/home/C.txt).
    //
    // 1. Figure out what files to download.
    // 2. Return a new map with the localized files replacing the platform files.
    //
    // Notes:
    // A file may be referenced more than once, we want to download it
    // just once.
    def localizeFiles(inputs: Map[String, WomValue]) : Map[String, WomValue] = {
        val fileURLs : Vector[String] = findFiles(inputs.values)

        // remove duplicates; we want to download each file just once
        val fileToDownload: Set[String] = fileURLs.toSet


    }
}
