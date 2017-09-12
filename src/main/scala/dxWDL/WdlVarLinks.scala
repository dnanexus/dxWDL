// Conversions from WDL types and data structures to
// DNAx JSON representations.
package dxWDL

import com.dnanexus.{DXFile, DXJob}
import com.fasterxml.jackson.databind.JsonNode
import java.nio.file.Paths
import net.jcazevedo.moultingyaml._
import spray.json._
import Utils.{dxFileOfJsValue, DXWorkflowStage}
import wdl4s.wdl.types._
import wdl4s.wdl.values._

object IORef extends Enumeration {
    val Input, Output = Value
}

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
case class DxlJob(dxJob: DXJob, varName: String) extends DxLink
case class DxlJobArray(dxJobVec: Vector[DXJob], varName: String) extends DxLink

case class WdlVarLinks(wdlType: WdlType,
                       attrs: DeclAttrs,
                       dxlink: DxLink)

// Bridge between WDL values and DNAx values.
case class BValue(wvl: WdlVarLinks, wdlValue: WdlValue)

object WdlVarLinks {
    // Human readable representation of a WdlVarLinks structure
    def yaml(wvl: WdlVarLinks) : YamlObject = {
        val (key, value) = wvl.dxlink match {
            case DxlValue(jsn) =>
                "JSON" -> jsn.prettyPrint
            case DxlStage(dxStage, ioRef, varEncName) =>
                "stageRef" -> varEncName
            case DxlJob(dxJob, varEncName) =>
                "jobRef" -> varEncName
            case DxlJobArray(dxJobVec, varEncName) =>
                "jobRefArray" -> varEncName
        }
        YamlObject(
            YamlString("type") -> YamlString(wvl.wdlType.toWdlString),
            YamlString(key) -> YamlString(value))
    }

    // remove persistent resources used by this variable
    def deleteLocal(wdlValue: WdlValue) : Unit = {
        wdlValue match {
            case WdlBoolean(_) | WdlInteger(_) | WdlFloat(_) | WdlString(_) => ()
            case WdlSingleFile(path) =>
                LocalDxFiles.delete(Paths.get(path))

            // recursion
            case WdlOptionalValue(_, None) => ()
            case WdlOptionalValue(_, Some(x)) =>
                deleteLocal(x)
            case WdlArray(_, a) =>
                a.foreach(x => deleteLocal(x))
            case WdlMap(_, m) =>
                m.foreach{ case (k,v) =>
                    deleteLocal(k)
                    deleteLocal(v)
                }
            case WdlObject(m) =>
                m.foreach { case (_, v) => deleteLocal(v) }
            case WdlPair(left, right) =>
                deleteLocal(left)
                deleteLocal(right)
            case _ =>
                throw new Exception(s"Don't know how to delete a ${wdlValue} value")
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

    // Is this a WDL type that maps to a native DX type?
    def isNativeDxType(wdlType: WdlType) : Boolean = {
        Utils.stripOptional(wdlType) match {
            case WdlBooleanType | WdlIntegerType | WdlFloatType | WdlStringType | WdlFileType
                   | WdlArrayType(WdlBooleanType)
                   | WdlArrayType(WdlIntegerType)
                   | WdlArrayType(WdlFloatType)
                   | WdlArrayType(WdlStringType)
                   | WdlArrayType(WdlFileType) => true
            case _ => false
        }
    }

    // Search through a JSON value for all the dx:file links inside it. Returns
    // those as a vector.
    def findDxFiles(jsValue: JsValue) : Vector[DXFile] = {
        jsValue match {
            case JsBoolean(_) | JsNull | JsNumber(_) | JsString(_) =>
                Vector.empty[DXFile]
            case JsObject(_) if isDxFile(jsValue) =>
                Vector(dxFileOfJsValue(jsValue))
            case JsObject(fields) =>
                fields.map{ case(_,v) => findDxFiles(v) }.toVector.flatten
            case JsArray(elems) =>
                elems.map(e => findDxFiles(e)).flatten
        }
    }

    // Get the file-id
    def getFileId(wvl: WdlVarLinks) : String = {
        assert(Utils.stripOptional(wvl.wdlType) == WdlFileType)
        wvl.dxlink match {
            case DxlValue(jsn) =>
                val dxFiles = findDxFiles(jsn)
                assert(dxFiles.length == 1)
                dxFiles.head.getId()
            case _ =>
                throw new Exception("cannot get file-id from non-JSON")
        }
    }

    private def unmarshalJsMap(jsValue: JsValue) : (Vector[JsValue], Vector[JsValue]) = {
        try {
            val fields = jsValue.asJsObject.fields
            val kJs: Vector[JsValue] = fields("keys") match {
                case JsArray(x) => x
                case _ => throw new Exception("Malformed JSON")
            }
            val vJs: Vector[JsValue] = fields("values") match {
                case JsArray(x) => x
                case _ => throw new Exception("Malformed JSON")
            }
            assert(kJs.length == vJs.length)
            (kJs, vJs)
        } catch {
            case e: Throwable =>
                System.err.println(s"Deserialization error: ${jsValue}")
                throw e
        }
    }

    private def unmarshalJsObject(jsValue: JsValue) :Vector[(String, WdlType, JsValue)] = {
        try {
            val fields = jsValue.asJsObject.fields
            val keys: Vector[String] = fields("keys") match {
                case JsArray(x) => x.map{
                    case JsString(s) => s
                    case other  => throw new Exception(s"key field is not a string (${other})")
                }
                case _ => throw new Exception("Malformed JSON")
            }
            val wdlTypes: Vector[WdlType] = fields("types") match {
                case JsArray(x) => x.map{
                    case JsString(s) => WdlType.fromWdlString(s)
                    case other  => throw new Exception(s"type field is not a string (${other})")
                }
                case _ => throw new Exception("Malformed JSON")
            }
            val values: Vector[JsValue] = fields("values") match {
                case JsArray(x) => x
                case _ => throw new Exception("Malformed JSON")
            }

            // all the vectors should have the same length
            val len = keys.length
            assert(len == wdlTypes.length)
            assert(len == values.length)

            // create tuples from the separate vectors
            val range = (0 to (len-1)).toVector
            range.map{ i =>
                (keys(i), wdlTypes(i), values(i))
            }.toVector
        } catch {
            case e : Throwable =>
                System.err.println(s"Deserialization error: ${jsValue}")
                throw e
        }
    }

    private def evalCore(wdlType: WdlType, jsValue: JsValue, force: Boolean) : WdlValue = {
        (wdlType, jsValue)  match {
            // base case: primitive types
            case (WdlBooleanType, JsBoolean(b)) => WdlBoolean(b.booleanValue)
            case (WdlIntegerType, JsNumber(bnm)) => WdlInteger(bnm.intValue)
            case (WdlFloatType, JsNumber(bnm)) => WdlFloat(bnm.doubleValue)
            case (WdlStringType, JsString(s)) => WdlString(s)
            case (WdlFileType, _) => LocalDxFiles.wdlFileOfDxLink(jsValue, force)

            // arrays
            case (WdlArrayType(t), JsArray(vec)) =>
                val wVec: Seq[WdlValue] = vec.map{
                    elem:JsValue => evalCore(t, elem, force)
                }
                WdlArray(WdlArrayType(t), wVec)

            // Maps. These are serialized as an object with a keys array and
            // a values array.
            case (WdlMapType(keyType, valueType), _) =>
                val (kJs, vJs) = unmarshalJsMap(jsValue)
                val kv = kJs zip vJs
                val m: Map[WdlValue, WdlValue] = kv.map{ case (k,v) =>
                    val kWdl = evalCore(keyType, k, force)
                    val vWdl = evalCore(valueType, v, force)
                    kWdl -> vWdl
                }.toMap
                WdlMap(WdlMapType(keyType, valueType), m)

            case (WdlObjectType, JsObject(_)) =>
                val vec: Vector[(String, WdlType, JsValue)] = unmarshalJsObject(jsValue)
                val m = vec.map{ case (key, wdlType, jsv) =>
                    key -> evalCore(wdlType, jsv, force)
                }.toMap
                WdlObject(m)

            case (WdlPairType(lType, rType), JsArray(vec)) if (vec.length == 2) =>
                val left = evalCore(lType, vec(0), force)
                val right = evalCore(rType, vec(1), force)
                WdlPair(left, right)

            case _ =>
                throw new AppInternalException(
                    s"Unsupport combination ${wdlType.toWdlString} ${jsValue.prettyPrint}"
                )
        }
    }

    private def getRawJsValue(wvl: WdlVarLinks) : JsValue = {
        wvl.dxlink match {
            case DxlValue(jsn) => jsn
            case _ =>
                throw new AppInternalException(
                    s"Unsupported conversion from ${wvl.dxlink} to WdlValue")
        }
    }

    // Calculate a WdlValue from the dx-links structure. If [force] is true,
    // any files included in the structure will be downloaded.
    def eval(wvl: WdlVarLinks, force: Boolean) : WdlValue = {
        val jsValue = getRawJsValue(wvl)
        evalCore(wvl.wdlType, jsValue, force)
    }

    // The reason we need a special method for unpacking an array (or a map),
    // is because we DO NOT want to evaluate the sub-structures. The trouble is
    // files, that may all have the same paths, causing collisions.
    def unpackWdlArray(wvl: WdlVarLinks) : Seq[WdlVarLinks] = {
        val jsn = getRawJsValue(wvl)
        (wvl.wdlType, jsn) match {
            case (WdlArrayType(t), JsArray(l)) =>
                // Array
                l.map(elem => WdlVarLinks(t, wvl.attrs, DxlValue(elem)))

            case (WdlMapType(keyType, valueType), _) =>
                // Map. Convert into an array of WDL pairs.
                val (kJs, vJs) = unmarshalJsMap(jsn)
                val kv = kJs zip vJs
                val wdlType = WdlPairType(keyType, valueType)
                kv.map{ case (k, v) =>
                    val js:JsValue = JsArray(Vector(k, v))
                    WdlVarLinks(wdlType, wvl.attrs, DxlValue(js))
                }

            case (_,_) =>
                // Error
                throw new AppInternalException(s"Can't unpack ${wvl.wdlType.toWdlString} ${jsn}")
            }
    }

    // Access a field in a complex WDL type, such as Pair, Map, Object.
    private def memberAccessStep(wvl: WdlVarLinks, field: String) : WdlVarLinks = {
        val jsValue = getRawJsValue(wvl)
        (wvl.wdlType, jsValue) match {
            case (_:WdlObject, JsObject(_)) =>
                val vec:Vector[(String, WdlType, JsValue)] = unmarshalJsObject(jsValue)
                val fieldVal = vec.find{ case (key,_,_) => key == field }
                fieldVal match {
                    case Some((_,wdlType,jsv)) => WdlVarLinks(wdlType, wvl.attrs, DxlValue(jsv))
                    case _ => throw new Exception(s"Unknown field ${field} in object ${wvl}")
                }
            case (WdlPairType(lType, rType), JsArray(vec)) =>
                field match {
                    case "left" =>  WdlVarLinks(lType, wvl.attrs, DxlValue(vec(0)))
                    case "right" =>  WdlVarLinks(rType, wvl.attrs, DxlValue(vec(1)))
                    case _ => throw new Exception(s"Unknown field ${field} in pair ${wvl}")
                }
            case _ =>
                throw new Exception(s"member access to field ${field} wvl=${wvl}")
        }
    }

    // Multi step member access, appropriate for nested structures. For example:
    //
    // Pair[Int, Pair[String, File]] p
    // File veggies = p.left.right
    //
    def memberAccess(wvl: WdlVarLinks, components: List[String]) : WdlVarLinks = {
        components match {
            case Nil => wvl
            case head::tail =>
                val wvl1 = memberAccessStep(wvl, head)
                memberAccess(wvl1, tail)
        }
    }

    // Serialize a complex WDL value into a JSON value. The value could potentially point
    // to many files. Serialization proceeds as follows:
    // 1. Make a pass on the object, upload any files, and keep an in-memory JSON representation
    // 2. In memory we have a, potentially very large, JSON value. Upload it to the platform
    //    as a file, and return a JSON link to the file.
    private def jsOfComplexWdlValue(wdlType: WdlType, wdlValue: WdlValue) : JsValue = {
        (wdlType, wdlValue) match {
            // Base case: primitive types
            case (WdlFileType, WdlString(path)) => LocalDxFiles.upload(Paths.get(path))
            case (WdlFileType, WdlSingleFile(path)) => LocalDxFiles.upload(Paths.get(path))
            case (WdlStringType, WdlSingleFile(path)) => JsString(path)
            case (_,WdlBoolean(b)) => JsBoolean(b)
            case (_,WdlInteger(n)) => JsNumber(n)
            case (_,WdlFloat(x)) => JsNumber(x)
            case (_,WdlString(buf)) =>
                if (buf.length > Utils.MAX_STRING_LEN)
                    throw new AppInternalException(s"string is longer than ${Utils.MAX_STRING_LEN}")
                JsString(buf)

            // Base case: empty array
            case (_, WdlArray(_, ar)) if ar.length == 0 =>
                JsArray(Vector.empty)

            // Non empty array
            case (WdlArrayType(t), WdlArray(_, elems)) =>
                val jsVals = elems.map(e => jsOfComplexWdlValue(t, e))
                JsArray(jsVals.toVector)

            // Maps. These are projections from a key to value, where
            // the key and value types are statically known. We
            // represent them in JSON as an array of keys, followed by
            // an array of values.
            case (WdlMapType(keyType, valueType), WdlMap(_, m)) =>
                val keys:WdlValue = WdlArray(WdlArrayType(keyType), m.keys.toVector)
                val kJs = jsOfComplexWdlValue(keys.wdlType, keys)
                val values:WdlValue = WdlArray(WdlArrayType(valueType), m.values.toVector)
                val vJs = jsOfComplexWdlValue(values.wdlType, values)
                JsObject("keys" -> kJs, "values" -> vJs)

            // objects. These are represented as three arrays: keys,
            // wdl-types, and values (in JSON).
            case (WdlObjectType, WdlObject(m: Map[String, WdlValue])) =>
                val keys = m.keys.map(k => JsString(k))
                val types = m.values.map(v => JsString(v.wdlType.toWdlString))
                val values = m.values.map(v => jsOfComplexWdlValue(v.wdlType, v))
                JsObject("keys" -> JsArray(keys.toVector),
                         "types" -> JsArray(types.toVector),
                         "values" -> JsArray(values.toVector))

            case (WdlPairType(lType, rType), WdlPair(l,r)) =>
                val lJs = jsOfComplexWdlValue(lType, l)
                val rJs = jsOfComplexWdlValue(rType, r)
                JsArray(lJs, rJs)

            case _ => throw new Exception(
                s"Unsupported WDL type ${wdlType.toWdlString} ${wdlValue.toWdlString}"
            )
        }
    }

    // import a WDL value
    def apply(wdlTypeOrg: WdlType, attrs: DeclAttrs, wdlValue: WdlValue) : WdlVarLinks = {
        // Strip optional types
        val wdlType = Utils.stripOptional(wdlTypeOrg)
        val jsValue = jsOfComplexWdlValue(wdlType, wdlValue)
        WdlVarLinks(wdlTypeOrg, attrs, DxlValue(jsValue))
    }

    def mkJborArray(dxJobVec: Vector[DXJob],
                    varName: String) : JsonNode = {
        val jbors: Vector[JsValue] = dxJobVec.map{ dxJob =>
            val jobId : String = dxJob.getId()
            Utils.makeJBOR(jobId, varName)
        }
        val retval = Utils.jsonNodeOfJsValue(JsArray(jbors))
        //System.err.println(s"mkJborArray(${varName})  ${retval}")
        retval
    }


    // Dx allows hashes as an input/output type. If the JSON value is
    // not a hash (js-object), we need to add an outer layer to it.
    private def jsValueToDxHash(wdlType: WdlType, jsVal: JsValue) : JsValue = {
        jsVal match {
            case JsObject(_) => jsVal
            case _ => JsObject("tag" -> jsVal)
        }
    }
    private def dxHashToJsValue(wdlType: WdlType, jsValue: JsValue) : JsValue = {
        jsValue match {
            case JsObject(fields) if fields contains "tag" =>
                fields("tag")
            case _ => jsValue
        }
    }

    // create input/output fields that bind the variable name [bindName] to
    // this WdlVar
    def genFields(wvl : WdlVarLinks, bindName: String) : List[(String, JsonNode)] = {
        val bindEncName = Utils.encodeAppletVarName(Utils.transformVarName(bindName))

        def mkSimple() : (String, JsonNode) = {
            val jsNode : JsonNode = wvl.dxlink match {
                case DxlValue(jsn) => Utils.jsonNodeOfJsValue(jsn)
                case DxlStage(dxStage, ioRef, varEncName) =>
                    ioRef match {
                        case IORef.Input => dxStage.getInputReference(varEncName)
                        case IORef.Output => dxStage.getOutputReference(varEncName)
                    }
                case DxlJob(dxJob, varEncName) =>
                    val jobId : String = dxJob.getId()
                    Utils.jsonNodeOfJsValue(Utils.makeJBOR(jobId, varEncName))
                case DxlJobArray(dxJobVec, varEncName) =>
                    mkJborArray(dxJobVec, varEncName)
            }
            (bindEncName, jsNode)
        }
        def mkComplex(wdlType: WdlType) : Map[String,JsonNode] = {
            val bindEncName_F = bindEncName + Utils.FLAT_FILES_SUFFIX
            wvl.dxlink match {
                case DxlValue(jsn) =>
                    // files that are embedded in the structure
                    val dxFiles = findDxFiles(jsn)
                    val jsFiles = dxFiles.map(x => Utils.jsValueOfJsonNode(x.getLinkAsJson))
                    // convert the top level structure into a hash
                    val jsSrlFileLink = jsValueToDxHash(wdlType, jsn)
                    Map(bindEncName -> Utils.jsonNodeOfJsValue(jsSrlFileLink),
                        bindEncName_F -> Utils.jsonNodeOfJsValue(JsArray(jsFiles)))
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
                case DxlJob(dxJob, varEncName) =>
                    val varEncName_F = varEncName + Utils.FLAT_FILES_SUFFIX
                    val jobId : String = dxJob.getId()
                    Map(bindEncName -> Utils.jsonNodeOfJsValue(Utils.makeJBOR(jobId, varEncName)),
                        bindEncName_F -> Utils.jsonNodeOfJsValue(Utils.makeJBOR(jobId, varEncName_F))
                    )
                case DxlJobArray(dxJobVec, varEncName) =>
                    val varEncName_F = varEncName + Utils.FLAT_FILES_SUFFIX
                    Map(bindEncName -> mkJborArray(dxJobVec, varEncName),
                        bindEncName_F -> mkJborArray(dxJobVec, varEncName_F))
            }
        }

        val wdlType = Utils.stripOptional(wvl.wdlType)
        if (isNativeDxType(wdlType)) {
            // Types that are supported natively in DX
            List(mkSimple())
        } else {
            // General complex type requiring two fields: a JSON
            // structure, and a flat array of files.
            mkComplex(wdlType).toList
        }
    }


    // Convert an input field to a dx-links structure. This allows
    // passing it to other jobs.
    //
    // Note: we need to represent dx-files as local paths, even if we
    // do not download them. This is because accessing these files
    // later on will cause a WDL failure.
    def importFromDxExec(wdlType: WdlType, attrs: DeclAttrs, jsValue: JsValue) : WdlVarLinks = {
        WdlVarLinks(wdlType, attrs, DxlValue(dxHashToJsValue(wdlType, jsValue)))
    }

    // Read the job-inputs JSON file, and convert the variables
    // to links that can be passed to other applets.
    def loadJobInputsAsLinks(inputLines : String, closureTypes : Map[String, Option[WdlType]]) :
            Map[String, WdlVarLinks] = {
        // Read the job_inputs.json file. Convert it to a mapping from string to JSON
        // value.
        val jsonAst : JsValue = inputLines.parseJson
        val fields : Map[String, JsValue] = jsonAst.asJsObject.fields

        // Create a mapping from each key to its WDL value,
        // ignore all untyped fields.
        closureTypes.map { case (key,wdlTypeOpt) =>
            wdlTypeOpt match {
                case None => None
                case Some(WdlOptionalType(wType)) =>
                    fields.get(key) match {
                        case None => None
                        case Some(jsValue) =>
                            val wvl = importFromDxExec(wType, DeclAttrs.empty, jsValue)
                            Some(key -> wvl)
                    }
                case Some(wType) =>
                    val jsValue = fields(key)
                    val wvl = importFromDxExec(wType, DeclAttrs.empty, jsValue)
                    Some(key -> wvl)
            }
        }.flatten.toMap
    }

    // Merge an array of links into one. All the links
    // have to be of the same dxlink type.
    def merge(vec: Vector[WdlVarLinks]) : WdlVarLinks = {
        if (vec.isEmpty)
            throw new Exception("Sanity: WVL array has to be non empty")

        val wdlType = WdlArrayType(vec.head.wdlType)
        val declAttrs = vec.head.attrs
        vec.head.dxlink match {
            case DxlValue(_) =>
                val jsVec:Vector[JsValue] = vec.map{ wvl =>
                    wvl.dxlink match {
                        case DxlValue(jsv) => jsv
                        case _ => throw new Exception("Sanity")
                    }
                }
                WdlVarLinks(wdlType, declAttrs, DxlValue(JsArray(jsVec)))

            case DxlJob(_, varName) =>
                val jobVec:Vector[DXJob] = vec.map{ wvl =>
                    wvl.dxlink match {
                        case DxlJob(job,name) =>
                            assert(name == varName)
                            job
                        case _ => throw new Exception("Sanity")
                    }
                }
                WdlVarLinks(wdlType, declAttrs, DxlJobArray(jobVec, varName))
            case _ => throw new Exception(s"Don't know how to merge WVL arrays of type ${vec.head}")
        }
    }
}
