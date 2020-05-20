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
                                typeAliases: Map[String, WomType]) {
  val wdlTypeSerializer = WomTypeSerialization(typeAliases)

  private def isDoubleOptional(t: WomType): Boolean = {
    t match {
      case WdlOptionalType(WdlOptionalType(_)) => true
      case _                                   => false
    }
  }

  // Serialize a complex WDL value into a JSON value. The value could potentially point
  // to many files. The assumption is that files are already in the format of dxWDLs,
  // requiring not upload/download or any special conversion.
  private def jsFromWdlValue(wdlType: WomType, wdlValue: WdlValue): JsValue = {
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
      case (WomFileType, WomString(path))     => handleFile(path)
      case (WomFileType, WomSingleFile(path)) => handleFile(path)
      case (WomStringType, WomSingleFile(path))     => JsString(path)
      case (WomStringType, WomString(buf)) =>
        if (buf.length > Utils.MAX_STRING_LEN)
          throw new AppInternalException(s"string is longer than ${Utils.MAX_STRING_LEN}")
        JsString(buf)
      case (WomBooleanType, WomBoolean(b))      => JsBoolean(b)
      case (WomBooleanType, WomString("true"))  => JsBoolean(true)
      case (WomBooleanType, WomString("false")) => JsBoolean(false)

      // Integer conversions
      case (WomIntegerType, WomInteger(n)) => JsNumber(n)
      case (WomIntegerType, WomString(s))  => JsNumber(s.toInt)
      case (WomIntegerType, WomFloat(x))   => JsNumber(x.toInt)

      // Float conversions
      case (WomFloatType, WomFloat(x))   => JsNumber(x)
      case (WomFloatType, WomInteger(n)) => JsNumber(n.toFloat)
      case (WomFloatType, WomString(s))  => JsNumber(s.toFloat)

      case (WomPairType(lType, rType), WomPair(l, r)) =>
        val lJs = jsFromWomValue(lType, l)
        val rJs = jsFromWomValue(rType, r)
        JsObject("left" -> lJs, "right" -> rJs)

      // Maps. These are projections from a key to value, where
      // the key and value types are statically known. We
      // represent them in JSON as an array of keys, followed by
      // an array of values.
      case (WomMapType(keyType, valueType), WomMap(_, m)) =>
        // general case
        val keys: WomValue = WomArray(WomArrayType(keyType), m.keys.toVector)
        val kJs = jsFromWomValue(keys.wdlType, keys)
        val values: WomValue = WomArray(WomArrayType(valueType), m.values.toVector)
        val vJs = jsFromWomValue(values.wdlType, values)
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
        val jsVals = elems.map { x =>
          jsFromWomValue(t, x)
        }
        JsArray(jsVals.toVector)

      // Strip optional type
      case (WomOptionalType(t), WomOptionalValue(_, Some(w))) =>
        jsFromWomValue(t, w)
      case (WomOptionalType(t), WomOptionalValue(_, None)) =>
        JsNull
      case (WomOptionalType(t), w) =>
        jsFromWomValue(t, w)
      case (t, WomOptionalValue(_, Some(w))) =>
        jsFromWomValue(t, w)

      // structs
      case (WomStructType(structName, typeMap), WomStruct(_, m: Map[String, WomValue], _)) =>
        // Convert each of the elements
        val mJs = m.map {
          case (key, wdlValue) =>
            val elemType = typeMap.get(key) match {
              case None =>
                throw new Exception(s"""|ERROR
                                        |WomStructType
                                        |  structName=${structName}
                                        |  typeMap=${typeMap}
                                        |  wdl-object=${m}
                                        |typeMap is missing key=${key}
                                        |""".stripMargin)
              case Some(t) => t
            }
            key -> jsFromWomValue(elemType, wdlValue)
        }.toMap
        JsObject(mJs)

      case (_, _) =>
        val wdlTypeStr =
          if (wdlType == null)
            "null"
          else
            WomTypeSerialization.typeName(wdlType)
        val wdlValueStr =
          if (wdlValue == null)
            "null"
          else
            s"(${wdlValue.toWomString}, ${wdlValue.wdlType})"
        throw new Exception(s"""|Unsupported combination:
                                |    wdlType:  ${wdlTypeStr}
                                |    wdlValue: ${wdlValueStr}""".stripMargin)
    }
  }

  // import a WDL value
  def importFromWDL(wdlType: WomType, wdlValue: WomValue): WdlVarLinks = {
    val jsValue = jsFromWomValue(wdlType, wdlValue)
    WdlVarLinks(wdlType, DxlValue(jsValue))
  }

  // Convert a job input to a WomValue. Do not download any files, convert them
  // to a string representation. For example: dx://proj-xxxx:file-yyyy::/A/B/C.txt
  //
  private def jobInputToWomValue(name: String, wdlType: WomType, jsValue: JsValue): WomValue = {
    (wdlType, jsValue) match {
      // base case: primitive types
      case (WomBooleanType, JsBoolean(b))   => WomBoolean(b.booleanValue)
      case (WomIntegerType, JsNumber(bnm))  => WomInteger(bnm.intValue)
      case (WomFloatType, JsNumber(bnm))    => WomFloat(bnm.doubleValue)
      case (WomStringType, JsString(s))     => WomString(s)
      case (WomFileType, JsString(s)) => WomFile(s)
      case (WomFileType, JsObject(_)) =>
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
              if (x.length != y.length)
                throw new Exception(s"""|len(keys) != len(values)
                                        |fields: ${fields}
                                        |""".stripMargin)
              (x zip y).toMap
            case _ =>
              throw new Exception(s"Malformed JSON ${fields}")
          }
        val m: Map[WomValue, WomValue] = mJs.map {
          case (k: JsValue, v: JsValue) =>
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
        WomArray(List.empty[WomValue])

      // array
      case (WomArrayType(t), JsArray(vec)) =>
        val wVec: Seq[WomValue] = vec.map { elem: JsValue =>
          jobInputToWomValue(name, t, elem)
        }
        WomArray(wVec)

      case (WomOptionalType(t), JsNull) =>
        WomOptionalValue(t, None)
      case (WomOptionalType(t), jsv) =>
        val value = jobInputToWomValue(name, t, jsv)
        WomOptionalValue(t, Some(value))

      // structs
      case (WomStructType(typeMap, Some(structName)), JsObject(fields)) =>
        val m: Map[String, WomValue] = fields.map {
          case (key, jsValue) =>
            val t = typeMap.get(key) match {
              case None =>
                throw new Exception(s"""|ERROR
                                        |WomStructType
                                        |  structName=${structName}
                                        |  typeMap=${typeMap}
                                        |  fields=${fields}
                                        |typeMap is missing key=${key}
                                        |""".stripMargin)
              case Some(t) => t
            }
            key -> jobInputToWomValue(key, t, jsValue)
        }.toMap
        WomStruct(structName, m)

      case _ =>
        throw new AppInternalException(s"""|Unsupported combination
                                           |  name:    ${name}
                                           |  wdlType: ${wdlType}
                                           |  JSON:    ${jsValue.prettyPrint}
                                           |""".stripMargin)
    }
  }

  def unpackJobInput(name: String, wdlType: WomType, jsv: JsValue): (WomValue, Vector[DxFile]) = {
    val jsv1 =
      jsv match {
        case JsObject(fields) if fields contains "___" =>
          // unpack the hash with which complex JSON values are
          // wrapped in dnanexus.
          fields("___")
        case _ => jsv
      }
    val wdlValue = jobInputToWomValue(name, wdlType, jsv1)
    val dxFiles = DxUtils.findDxFiles(jsv)
    (wdlValue, dxFiles)
  }

  // create input/output fields that bind the variable name [bindName] to
  // this WomVar
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
    def mkComplex(wdlType: WomType): Map[String, JsValue] = {
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
