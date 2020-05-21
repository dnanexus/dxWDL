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
                                typeAliases: Map[String, WdlTypes.T]) {
  val wdlTypeSerializer = WdlTypes.TSerialization(typeAliases)

  private def isDoubleOptional(t: WdlTypes.T): Boolean = {
    t match {
      case WdlOptionalType(WdlOptionalType(_)) => true
      case _                                   => false
    }
  }

  // Serialize a complex WDL value into a JSON value. The value could potentially point
  // to many files. The assumption is that files are already in the format of dxWDLs,
  // requiring not upload/download or any special conversion.
  private def jsFromWdlValue(wdlType: WdlTypes.T, wdlValue: WdlValue): JsValue = {
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
      case (WdlTypes.T_File, WdlValues.V_String(path))     => handleFile(path)
      case (WdlTypes.T_File, WdlValues.V_File(path)) => handleFile(path)
      case (WdlTypes.T_String, WdlValues.V_File(path))     => JsString(path)
      case (WdlTypes.T_String, WdlValues.V_String(buf)) =>
        if (buf.length > BaseUtils.MAX_STRING_LEN)
          throw new AppInternalException(s"string is longer than ${BaseUtils.MAX_STRING_LEN}")
        JsString(buf)
      case (WdlTypes.T_Boolean, WdlValues.V_Boolean(b))      => JsBoolean(b)
      case (WdlTypes.T_Boolean, WdlValues.V_String("true"))  => JsBoolean(true)
      case (WdlTypes.T_Boolean, WdlValues.V_String("false")) => JsBoolean(false)

      // Integer conversions
      case (WdlTypes.T_Int, WdlValues.V_Int(n)) => JsNumber(n)
      case (WdlTypes.T_Int, WdlValues.V_String(s))  => JsNumber(s.toInt)
      case (WdlTypes.T_Int, WdlValues.V_Float(x))   => JsNumber(x.toInt)

      // Float conversions
      case (WdlTypes.T_Float, WdlValues.V_Float(x))   => JsNumber(x)
      case (WdlTypes.T_Float, WdlValues.V_Int(n)) => JsNumber(n.toFloat)
      case (WdlTypes.T_Float, WdlValues.V_String(s))  => JsNumber(s.toFloat)

      case (WdlTypes.T_Pair(lType, rType), WdlValues.V_Pair(l, r)) =>
        val lJs = jsFromWdlValues.V(lType, l)
        val rJs = jsFromWdlValues.V(rType, r)
        JsObject("left" -> lJs, "right" -> rJs)

      // Maps. These are projections from a key to value, where
      // the key and value types are statically known. We
      // represent them in JSON as an array of keys, followed by
      // an array of values.
      case (WdlTypes.T_Map(keyType, valueType), WdlValues.V_Map(_, m)) =>
        // general case
        val keys: WdlValues.V = WdlValues.V_Array(WdlTypes.T_Array(keyType), m.keys.toVector)
        val kJs = jsFromWdlValues.V(keys.wdlType, keys)
        val values: WdlValues.V = WdlValues.V_Array(WdlTypes.T_Array(valueType), m.values.toVector)
        val vJs = jsFromWdlValues.V(values.wdlType, values)
        JsObject("keys" -> kJs, "values" -> vJs)

      // Arrays: these come after maps, because there is an automatic coercion from
      // a map to an array.
      //
      // Base case: empty array
      case (_, WdlValues.V_Array(_, ar)) if ar.length == 0 =>
        JsArray(Vector.empty)
      case (WdlTypes.T_Array(t), null) =>
        JsArray(Vector.empty)

      // Non empty array
      case (WdlTypes.T_Array(t), WdlValues.V_Array(_, elems)) =>
        val jsVals = elems.map { x =>
          jsFromWdlValues.V(t, x)
        }
        JsArray(jsVals.toVector)

      // Strip optional type
      case (WdlTypes.T_Optional(t), WdlValues.V_OptionalValue(_, Some(w))) =>
        jsFromWdlValues.V(t, w)
      case (WdlTypes.T_Optional(t), WdlValues.V_OptionalValue(_, None)) =>
        JsNull
      case (WdlTypes.T_Optional(t), w) =>
        jsFromWdlValues.V(t, w)
      case (t, WdlValues.V_OptionalValue(_, Some(w))) =>
        jsFromWdlValues.V(t, w)

      // structs
      case (WdlTypes.T_Struct(structName, typeMap), WdlValues.V_Struct(_, m: Map[String, WdlValues.V], _)) =>
        // Convert each of the elements
        val mJs = m.map {
          case (key, wdlValue) =>
            val elemType = typeMap.get(key) match {
              case None =>
                throw new Exception(s"""|ERROR
                                        |WdlTypes.T_Struct
                                        |  structName=${structName}
                                        |  typeMap=${typeMap}
                                        |  wdl-object=${m}
                                        |typeMap is missing key=${key}
                                        |""".stripMargin)
              case Some(t) => t
            }
            key -> jsFromWdlValues.V(elemType, wdlValue)
        }.toMap
        JsObject(mJs)

      case (_, _) =>
        val wdlTypeStr =
          if (wdlType == null)
            "null"
          else
            WdlTypes.TSerialization.typeName(wdlType)
        val wdlValueStr =
          if (wdlValue == null)
            "null"
          else
            s"(${wdlValue.toWdlValues.V_String}, ${wdlValue.wdlType})"
        throw new Exception(s"""|Unsupported combination:
                                |    wdlType:  ${wdlTypeStr}
                                |    wdlValue: ${wdlValueStr}""".stripMargin)
    }
  }

  // import a WDL value
  def importFromWDL(wdlType: WdlTypes.T, wdlValue: WdlValues.V): WdlVarLinks = {
    val jsValue = jsFromWdlValues.V(wdlType, wdlValue)
    WdlVarLinks(wdlType, DxlValue(jsValue))
  }

  // Convert a job input to a WdlValues.V. Do not download any files, convert them
  // to a string representation. For example: dx://proj-xxxx:file-yyyy::/A/B/C.txt
  //
  private def jobInputToWomValue(name: String, wdlType: WdlTypes.T, jsValue: JsValue): WdlValues.V = {
    (wdlType, jsValue) match {
      // base case: primitive types
      case (WdlTypes.T_Boolean, JsBoolean(b))   => WdlValues.V_Boolean(b.booleanValue)
      case (WdlTypes.T_Int, JsNumber(bnm))  => WdlValues.V_Int(bnm.intValue)
      case (WdlTypes.T_Float, JsNumber(bnm))    => WdlValues.V_Float(bnm.doubleValue)
      case (WdlTypes.T_String, JsString(s))     => WdlValues.V_String(s)
      case (WdlTypes.T_File, JsString(s)) => WdlValues.V_File(s)
      case (WdlTypes.T_File, JsObject(_)) =>
        // Convert the path in DNAx to a string. We can later
        // decide if we want to download it or not
        val dxFile = DxUtils.dxFileFromJsValue(jsValue)
        val FurlDx(s, _, _) = Furl.dxFileToFurl(dxFile, fileInfoDir)
        WdlValues.V_File(s)

      // Maps. These are serialized as an object with a keys array and
      // a values array.
      case (WdlTypes.T_Map(keyType, valueType), _) =>
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
        val m: Map[WdlValues.V, WdlValues.V] = mJs.map {
          case (k: JsValue, v: JsValue) =>
            val kWom = jobInputToWdlValues.V(name, keyType, k)
            val vWom = jobInputToWdlValues.V(name, valueType, v)
            kWom -> vWom
        }.toMap
        WdlValues.V_Map(WdlTypes.T_Map(keyType, valueType), m)

      case (WdlTypes.T_Pair(lType, rType), JsObject(fields))
          if (List("left", "right").forall(fields contains _)) =>
        val left = jobInputToWdlValues.V(name, lType, fields("left"))
        val right = jobInputToWdlValues.V(name, rType, fields("right"))
        WdlValues.V_Pair(left, right)

      // empty array
      case (WdlTypes.T_Array(t), JsNull) =>
        WdlValues.V_Array(List.empty[WdlValues.V])

      // array
      case (WdlTypes.T_Array(t), JsArray(vec)) =>
        val wVec: Seq[WdlValues.V] = vec.map { elem: JsValue =>
          jobInputToWdlValues.V(name, t, elem)
        }
        WdlValues.V_Array(wVec)

      case (WdlTypes.T_Optional(t), JsNull) =>
        WdlValues.V_OptionalValue(t, None)
      case (WdlTypes.T_Optional(t), jsv) =>
        val value = jobInputToWdlValues.V(name, t, jsv)
        WdlValues.V_OptionalValue(t, Some(value))

      // structs
      case (WdlTypes.T_Struct(typeMap, Some(structName)), JsObject(fields)) =>
        val m: Map[String, WdlValues.V] = fields.map {
          case (key, jsValue) =>
            val t = typeMap.get(key) match {
              case None =>
                throw new Exception(s"""|ERROR
                                        |WdlTypes.T_Struct
                                        |  structName=${structName}
                                        |  typeMap=${typeMap}
                                        |  fields=${fields}
                                        |typeMap is missing key=${key}
                                        |""".stripMargin)
              case Some(t) => t
            }
            key -> jobInputToWdlValues.V(key, t, jsValue)
        }.toMap
        WdlValues.V_Struct(structName, m)

      case _ =>
        throw new AppInternalException(s"""|Unsupported combination
                                           |  name:    ${name}
                                           |  wdlType: ${wdlType}
                                           |  JSON:    ${jsValue.prettyPrint}
                                           |""".stripMargin)
    }
  }

  def unpackJobInput(name: String, wdlType: WdlTypes.T, jsv: JsValue): (WdlValues.V, Vector[DxFile]) = {
    val jsv1 =
      jsv match {
        case JsObject(fields) if fields contains "___" =>
          // unpack the hash with which complex JSON values are
          // wrapped in dnanexus.
          fields("___")
        case _ => jsv
      }
    val wdlValue = jobInputToWdlValues.V(name, wdlType, jsv1)
    val dxFiles = DxUtils.findDxFiles(jsv)
    (wdlValue, dxFiles)
  }

  // create input/output fields that bind the variable name [bindName] to
  // this WomVar
  def genFields(wvl: WdlVarLinks,
                bindName: String,
                encodeDots: Boolean = true): List[(String, JsValue)] = {
    def nodots(s: String): String =
      BaseUtils.encodeAppletVarName(BaseUtils.transformVarName(s))
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
    def mkComplex(wdlType: WdlTypes.T): Map[String, JsValue] = {
      val bindEncName_F = bindEncName + BaseUtils.FLAT_FILES_SUFFIX
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
          val varEncName_F = varEncName + BaseUtils.FLAT_FILES_SUFFIX
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
          val varEncName_F = varEncName + BaseUtils.FLAT_FILES_SUFFIX
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
          val varEncName_F = varEncName + BaseUtils.FLAT_FILES_SUFFIX
          Map(bindEncName -> DxUtils.makeEBOR(dxJob, nodots(varEncName)),
              bindEncName_F -> DxUtils.makeEBOR(dxJob, nodots(varEncName_F)))
      }
    }

    val wdlType = BaseUtils.stripOptional(wvl.wdlType)
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
