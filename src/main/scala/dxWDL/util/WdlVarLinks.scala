/**

Conversions from WDL types and data structures to DNAx JSON
representations. There are two difficulties this module needs to deal
with: (1) WDL has high order types which DNAx does not, and (2) the
file type is very different between WDL and DNAx.

  */
package dxWDL.util

import spray.json._

import dxWDL.base._
import dxWDL.base.WomCompat._
import dxWDL.dx._

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
case class DxlValue(jsn: JsValue) extends DxLink // This may contain dx-files
case class DxlStage(dxStage: DxWorkflowStage, ioRef: IORef.Value, varName: String) extends DxLink
case class DxlWorkflowInput(varName: String) extends DxLink
case class DxlExec(dxExec: DxExecution, varName: String) extends DxLink

case class WdlVarLinks(wdlType: WdlType, dxlink: DxLink)

case class WdlVarLinksConverter(verbose: Verbose,
                                fileInfoDir: Map[String, (DxFile, DxFileDescribe)],
                                typeAliases: Map[String, WdlType]) {
  val wdlTypeSerializer = WdlTypeSerialization(typeAliases)

  private def isDoubleOptional(t: WdlType): Boolean = {
    t match {
      case WdlOptionalType(WdlOptionalType(_)) => true
      case _                                   => false
    }
  }

  // Serialize a complex WDL value into a JSON value. The value could potentially point
  // to many files. The assumption is that files are already in the format of dxWDLs,
  // requiring not upload/download or any special conversion.
  private def jsFromWdlValue(wdlType: WdlType, wdlValue: WdlValue): JsValue = {
    if (isDoubleOptional(wdlType) ||
        isDoubleOptional(wdlValue.wdlType)) {
      System.err.println(s"""|jsFromWdlValue
                             |    type=${wdlType}
                             |    val=${wdlValue.toWdlString}
                             |    val.type=${wdlValue.wdlType}
                             |""".stripMargin)
      throw new Exception("a double optional type")
    }
    def handleFile(path: String): JsValue = {
      Furl.parse(path) match {
        case FurlDx(path, _, dxFile) =>
          DxUtils.dxFileToJsValue(dxFile)
        case FurlLocal(path) =>
          // A local file.
          JsString(path)
      }
    }
    (wdlType, wdlValue) match {
      // Base case: primitive types
      case (WdlFileType, WdlString(path))     => handleFile(path)
      case (WdlFileType, WdlSingleFile(path)) => handleFile(path)
      case (WdlStringType, WdlSingleFile(path))     => JsString(path)
      case (WdlStringType, WdlString(buf)) =>
        if (buf.length > Utils.MAX_STRING_LEN)
          throw new AppInternalException(s"string is longer than ${Utils.MAX_STRING_LEN}")
        JsString(buf)
      case (WdlBooleanType, WdlBoolean(b))      => JsBoolean(b)
      case (WdlBooleanType, WdlString("true"))  => JsBoolean(true)
      case (WdlBooleanType, WdlString("false")) => JsBoolean(false)

      // Integer conversions
      case (WdlIntegerType, WdlInteger(n)) => JsNumber(n)
      case (WdlIntegerType, WdlString(s))  => JsNumber(s.toInt)
      case (WdlIntegerType, WdlFloat(x))   => JsNumber(x.toInt)

      // Float conversions
      case (WdlFloatType, WdlFloat(x))   => JsNumber(x)
      case (WdlFloatType, WdlInteger(n)) => JsNumber(n.toFloat)
      case (WdlFloatType, WdlString(s))  => JsNumber(s.toFloat)

      case (WdlPairType(lType, rType), WdlPair(l, r)) =>
        val lJs = jsFromWdlValue(lType, l)
        val rJs = jsFromWdlValue(rType, r)
        JsObject("left" -> lJs, "right" -> rJs)

      // Maps. These are projections from a key to value, where
      // the key and value types are statically known. We
      // represent them in JSON as an array of keys, followed by
      // an array of values.
      case (WdlMapType(keyType, valueType), WdlMap(_, m)) =>
        // general case
        val keys: WdlValue = WdlArray(WdlArrayType(keyType), m.keys.toVector)
        val kJs = jsFromWdlValue(keys.wdlType, keys)
        val values: WdlValue = WdlArray(WdlArrayType(valueType), m.values.toVector)
        val vJs = jsFromWdlValue(values.wdlType, values)
        JsObject("keys" -> kJs, "values" -> vJs)

      // Arrays: these come after maps, because there is an automatic coercion from
      // a map to an array.
      //
      // Base case: empty array
      case (_, WdlArray(_, ar)) if ar.length == 0 =>
        JsArray(Vector.empty)
      case (WdlArrayType(t), null) =>
        JsArray(Vector.empty)

      // Non empty array
      case (WdlArrayType(t), WdlArray(_, elems)) =>
        val jsVals = elems.map { x =>
          jsFromWdlValue(t, x)
        }
        JsArray(jsVals.toVector)

      // Strip optional type
      case (WdlOptionalType(t), WdlOptionalValue(_, Some(w))) =>
        jsFromWdlValue(t, w)
      case (WdlOptionalType(t), WdlOptionalValue(_, None)) =>
        JsNull
      case (WdlOptionalType(t), w) =>
        jsFromWdlValue(t, w)
      case (t, WdlOptionalValue(_, Some(w))) =>
        jsFromWdlValue(t, w)

      // structs
      case (WdlStructType(structName, typeMap), WdlStruct(_, m: Map[String, WdlValue], _)) =>
        // Convert each of the elements
        val mJs = m.map {
          case (key, wdlValue) =>
            val elemType = typeMap.get(key) match {
              case None =>
                throw new Exception(s"""|ERROR
                                        |WdlStructType
                                        |  structName=${structName}
                                        |  typeMap=${typeMap}
                                        |  wdl-object=${m}
                                        |typeMap is missing key=${key}
                                        |""".stripMargin)
              case Some(t) => t
            }
            key -> jsFromWdlValue(elemType, wdlValue)
        }.toMap
        JsObject(mJs)

      case (_, _) =>
        val wdlTypeStr =
          if (wdlType == null)
            "null"
          else
            WdlTypeSerialization.typeName(wdlType)
        val wdlValueStr =
          if (wdlValue == null)
            "null"
          else
            s"(${wdlValue.toWdlString}, ${wdlValue.wdlType})"
        throw new Exception(s"""|Unsupported combination:
                                |    wdlType:  ${wdlTypeStr}
                                |    wdlValue: ${wdlValueStr}""".stripMargin)
    }
  }

  // import a WDL value
  def importFromWDL(wdlType: WdlType, wdlValue: WdlValue): WdlVarLinks = {
    val jsValue = jsFromWdlValue(wdlType, wdlValue)
    WdlVarLinks(wdlType, DxlValue(jsValue))
  }

  // Convert a job input to a WdlValue. Do not download any files, convert them
  // to a string representation. For example: dx://proj-xxxx:file-yyyy::/A/B/C.txt
  //
  private def jobInputToWdlValue(name: String, wdlType: WdlType, jsValue: JsValue): WdlValue = {
    (wdlType, jsValue) match {
      // base case: primitive types
      case (WdlBooleanType, JsBoolean(b))   => WdlBoolean(b.booleanValue)
      case (WdlIntegerType, JsNumber(bnm))  => WdlInteger(bnm.intValue)
      case (WdlFloatType, JsNumber(bnm))    => WdlFloat(bnm.doubleValue)
      case (WdlStringType, JsString(s))     => WdlString(s)
      case (WdlFileType, JsString(s)) => WdlFile(s)
      case (WdlFileType, JsObject(_)) =>
        // Convert the path in DNAx to a string. We can later
        // decide if we want to download it or not
        val dxFile = DxUtils.dxFileFromJsValue(jsValue)
        val FurlDx(s, _, _) = Furl.dxFileToFurl(dxFile, fileInfoDir)
        WdlSingleFile(s)

      // Maps. These are serialized as an object with a keys array and
      // a values array.
      case (WdlMapType(keyType, valueType), _) =>
        val fields = jsValue.asJsObject.fields
        // [mJs] is a map from json key to json value
        val mJs: Map[JsValue, JsValue] =
          (fields("keys"), fields("values")) match {
            case (JsArray(x), JsArray(y)) =>
              if (x.length != y.length)
                throw new Exception(s"""|len(keys) != len(values)
                                        |fields: ${fields}
                                        |""".stripMargin)
              (x zip y).toMap
            case _ =>
              throw new Exception(s"Malformed JSON ${fields}")
          }
        val m: Map[WdlValue, WdlValue] = mJs.map {
          case (k: JsValue, v: JsValue) =>
            val kWdl = jobInputToWdlValue(name, keyType, k)
            val vWdl = jobInputToWdlValue(name, valueType, v)
            kWdl -> vWdl
        }.toMap
        WdlMap(WdlMapType(keyType, valueType), m)

      case (WdlPairType(lType, rType), JsObject(fields))
          if (List("left", "right").forall(fields contains _)) =>
        val left = jobInputToWdlValue(name, lType, fields("left"))
        val right = jobInputToWdlValue(name, rType, fields("right"))
        WdlPair(left, right)

      // empty array
      case (WdlArrayType(t), JsNull) =>
        WdlArray(List.empty[WdlValue])

      // array
      case (WdlArrayType(t), JsArray(vec)) =>
        val wVec: Seq[WdlValue] = vec.map { elem: JsValue =>
          jobInputToWdlValue(name, t, elem)
        }
        WdlArray(wVec)

      case (WdlOptionalType(t), JsNull) =>
        WdlOptionalValue(t, None)
      case (WdlOptionalType(t), jsv) =>
        val value = jobInputToWdlValue(name, t, jsv)
        WdlOptionalValue(t, Some(value))

      // structs
      case (WdlStructType(typeMap, Some(structName)), JsObject(fields)) =>
        val m: Map[String, WdlValue] = fields.map {
          case (key, jsValue) =>
            val t = typeMap.get(key) match {
              case None =>
                throw new Exception(s"""|ERROR
                                        |WdlStructType
                                        |  structName=${structName}
                                        |  typeMap=${typeMap}
                                        |  fields=${fields}
                                        |typeMap is missing key=${key}
                                        |""".stripMargin)
              case Some(t) => t
            }
            key -> jobInputToWdlValue(key, t, jsValue)
        }.toMap
        WdlStruct(structName, m)

      case _ =>
        throw new AppInternalException(s"""|Unsupported combination
                                           |  name:    ${name}
                                           |  wdlType: ${wdlType}
                                           |  JSON:    ${jsValue.prettyPrint}
                                           |""".stripMargin)
    }
  }

  def unpackJobInput(name: String, wdlType: WdlType, jsv: JsValue): (WdlValue, Vector[DxFile]) = {
    val jsv1 =
      jsv match {
        case JsObject(fields) if fields contains "___" =>
          // unpack the hash with which complex JSON values are
          // wrapped in dnanexus.
          fields("___")
        case _ => jsv
      }
    val wdlValue = jobInputToWdlValue(name, wdlType, jsv1)
    val dxFiles = DxUtils.findDxFiles(jsv)
    (wdlValue, dxFiles)
  }

  // create input/output fields that bind the variable name [bindName] to
  // this WdlVar
  def genFields(wvl: WdlVarLinks,
                bindName: String,
                encodeDots: Boolean = true): List[(String, JsValue)] = {
    def nodots(s: String): String =
      Utils.encodeAppletVarName(Utils.transformVarName(s))
    val bindEncName =
      if (encodeDots) nodots(bindName)
      else bindName
    def mkSimple(): (String, JsValue) = {
      val jsv: JsValue = wvl.dxlink match {
        case DxlValue(jsn) => jsn
        case DxlStage(dxStage, ioRef, varEncName) =>
          ioRef match {
            case IORef.Input  => dxStage.getInputReference(nodots(varEncName))
            case IORef.Output => dxStage.getOutputReference(nodots(varEncName))
          }
        case DxlWorkflowInput(varEncName) =>
          JsObject(
              "$dnanexus_link" -> JsObject("workflowInputField" -> JsString(nodots(varEncName)))
          )
        case DxlExec(dxJob, varEncName) =>
          DxUtils.makeEBOR(dxJob, nodots(varEncName))
      }
      (bindEncName, jsv)
    }
    def mkComplex(wdlType: WdlType): Map[String, JsValue] = {
      val bindEncName_F = bindEncName + Utils.FLAT_FILES_SUFFIX
      wvl.dxlink match {
        case DxlValue(jsn) =>
          // files that are embedded in the structure
          val dxFiles = DxUtils.findDxFiles(jsn)
          val jsFiles = dxFiles.map(_.getLinkAsJson)
          // Dx allows hashes as an input/output type. If the JSON value is
          // not a hash (js-object), we need to add an outer layer to it.
          val jsn1 = JsObject("___" -> jsn)
          Map(bindEncName -> jsn1, bindEncName_F -> JsArray(jsFiles))
        case DxlStage(dxStage, ioRef, varEncName) =>
          val varEncName_F = varEncName + Utils.FLAT_FILES_SUFFIX
          ioRef match {
            case IORef.Input =>
              Map(
                  bindEncName -> dxStage.getInputReference(varEncName),
                  bindEncName_F -> dxStage.getInputReference(varEncName_F)
              )
            case IORef.Output =>
              Map(
                  bindEncName -> dxStage.getOutputReference(varEncName),
                  bindEncName_F -> dxStage.getOutputReference(varEncName_F)
              )
          }
        case DxlWorkflowInput(varEncName) =>
          val varEncName_F = varEncName + Utils.FLAT_FILES_SUFFIX
          Map(
              bindEncName ->
                JsObject(
                    "$dnanexus_link" -> JsObject("workflowInputField" -> JsString(varEncName))
                ),
              bindEncName_F ->
                JsObject(
                    "$dnanexus_link" -> JsObject("workflowInputField" -> JsString(varEncName_F))
                )
          )
        case DxlExec(dxJob, varEncName) =>
          val varEncName_F = varEncName + Utils.FLAT_FILES_SUFFIX
          Map(bindEncName -> DxUtils.makeEBOR(dxJob, nodots(varEncName)),
              bindEncName_F -> DxUtils.makeEBOR(dxJob, nodots(varEncName_F)))
      }
    }

    val wdlType = Utils.stripOptional(wvl.wdlType)
    if (DxUtils.isNativeDxType(wdlType)) {
      // Types that are supported natively in DX
      List(mkSimple())
    } else {
      // General complex type requiring two fields: a JSON
      // structure, and a flat array of files.
      mkComplex(wdlType).toList
    }
  }
}
