package dxWDL.runner

import cats.data.Validated.{Invalid, Valid}
import com.dnanexus.{DXFile}
import common.validation.ErrorOr.ErrorOr
import dxWDL.util._
import java.nio.file.{Files, Path, Paths}
import spray.json._
import wom.callable.Callable._
import wom.expression.WomExpression
import wom.types._
import wom.values._

object JobInputOutput {

    private val DOWNLOAD_RETRY_LIMIT = 3
    private val UPLOAD_RETRY_LIMIT = 3
    private val DISAMBIGUATION_DIRS_MAX_NUM = 10

    // download a file from the platform to a path on the local disk. Use
    // 'dx download' as a separate process.
    //
    // Note: this function assumes that the target path does not exist yet
    def downloadFile(path: Path, dxfile: DXFile) : Unit = {
        def downloadOneFile(path: Path, dxfile: DXFile, counter: Int) : Boolean = {
            val fid = dxfile.getId()
            try {
                // Shell out to 'dx download'
                val dxDownloadCmd = s"dx download ${fid} -o ${path.toString()}"
                System.err.println(s"--  ${dxDownloadCmd}")
                val (outmsg, errmsg) = Utils.execCommand(dxDownloadCmd, None)

                true
            } catch {
                case e: Throwable =>
                    if (counter < DOWNLOAD_RETRY_LIMIT)
                        false
                    else throw e
            }
        }
        val dir = path.getParent()
        if (dir != null) {
            if (!Files.exists(dir))
                Files.createDirectories(dir)
        }
        var rc = false
        var counter = 0
        while (!rc && counter < DOWNLOAD_RETRY_LIMIT) {
            System.err.println(s"downloading file ${path.toString} (try=${counter})")
            rc = downloadOneFile(path, dxfile, counter)
            counter = counter + 1
        }
        if (!rc)
            throw new Exception(s"Failure to download file ${path}")
    }

    // Upload a local file to the platform, and return a json link.
    // Use 'dx upload' as a separate process.
    def uploadFile(path: Path) : DXFile = {
        if (!Files.exists(path))
            throw new AppInternalException(s"Output file ${path.toString} is missing")
        def uploadOneFile(path: Path, counter: Int) : Option[String] = {
            try {
                // shell out to 'dx upload'
                val dxUploadCmd = s"dx upload ${path.toString} --brief"
                System.err.println(s"--  ${dxUploadCmd}")
                val (outmsg, errmsg) = Utils.execCommand(dxUploadCmd, None)
                if (!outmsg.startsWith("file-"))
                    return None
                Some(outmsg.trim())
            } catch {
                case e: Throwable =>
                    if (counter < UPLOAD_RETRY_LIMIT)
                        None
                    else throw e
            }
        }

        var counter = 0
        while (counter < UPLOAD_RETRY_LIMIT) {
            System.err.println(s"upload file ${path.toString} (try=${counter})")
            uploadOneFile(path, counter) match {
                case Some(fid) =>
                   return DXFile.getInstance(fid)
                case None => ()
            }
            counter = counter + 1
        }
        throw new Exception(s"Failure to upload file ${path}")
    }

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
                val dxFile = Utils.dxFileFromJsValue(jsValue)
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
                val left = jobInputToWomValue(lType, fields("left"))
                val right = jobInputToWomValue(rType, fields("right"))
                WomPair(left, right)

            case (WomObjectType, JsObject(fields)) =>
                throw new Exception("WOM objects not supported")

            // empty array
            case (WomArrayType(t), JsNull) =>
                WomArray(WomArrayType(t), List.empty[WomValue])

            // array
            case (WomArrayType(t), JsArray(vec)) =>
                val wVec: Seq[WomValue] = vec.map{
                    elem:JsValue => jobInputToWomValue(t, elem)
                }
                WomArray(WomArrayType(t), wVec)

            case (WomOptionalType(t), JsNull) =>
                WomOptionalValue(t, None)
            case (WomOptionalType(t), jsv) =>
                val value = jobInputToWomValue(t, jsv)
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

    private def unpackJobInput(womType: WomType, jsv: JsValue) : WomValue = {
        val jsv2: JsValue =
            if (Utils.isNativeDxType(womType)) {
                jsv
            } else {
                // unpack the hash with which complex JSON values are
                // wrapped in dnanexus.
                val (womType2, jsv1) = unmarshalHash(jsv)
                assert(womType2 == womType)
                jsv1
            }
        jobInputToWomValue(womType, jsv2)
    }

    private def evaluateWomExpression(expr: WomExpression, env: Map[String, WomValue]) : WomValue = {
        val result: ErrorOr[WomValue] =
            expr.evaluateValue(env, DxIoFunctions)
        result match {
            case Invalid(errors) => throw new Exception(
                s"Failed to evaluate expression ${expr} with ${errors}")
            case Valid(x: WomValue) => x
        }
    }

    // Read the job-inputs JSON file, and convert the variables
    // from JSON to WOM values. Delay downloading the files.
    def loadInputs(inputLines: String,
                   callable: wom.callable.Callable): Map[InputDefinition, WomValue] = {
        // Discard auxiliary fields
        val jsonAst : JsValue = inputLines.parseJson
        val fields : Map[String, JsValue] = jsonAst
            .asJsObject.fields
            .filter{ case (fieldName,_) => !fieldName.endsWith(Utils.FLAT_FILES_SUFFIX) }
        //System.out.println(s"inputLines=${inputLines}")
        //System.out.println(s"fields=${fields}")

        // Get the declarations matching the input fields.
        // Create a mapping from each key to its WDL value
        callable.inputs.foldLeft(Map.empty[InputDefinition, WomValue]) {
            case (accu, inpDfn) =>
                val accuValues = accu.map{ case (inpDfn, value) =>
                    inpDfn.name -> value
                }.toMap
                val value: WomValue = inpDfn match {
                    // A required input, no default.
                    case RequiredInputDefinition(iName, womType, _, _) =>
                        fields.get(iName.value) match {
                            case None =>
                                throw new Exception(s"Input ${iName} is required but not provided")
                            case Some(x : JsValue) =>
                                // Conversion from JSON to WomValue
                                unpackJobInput(womType, x)
                        }

                    // An input definition that has a default value supplied.
                    // Typical WDL example would be a declaration like: "Int x = 5"
                    case InputDefinitionWithDefault(iName, womType, defaultExpr, _, _) =>
                        fields.get(iName.value) match {
                            case None =>
                                // use the default expression
                                evaluateWomExpression(defaultExpr, accuValues)
                            case Some(x : JsValue) =>
                                unpackJobInput(womType, x)
                        }

                    // An input whose value should always be calculated from the default, and is
                    // not allowed to be overridden.
                    case FixedInputDefinition(iName, womType, defaultExpr, _, _) =>
                        fields.get(iName.value) match {
                            case None => ()
                            case Some(_) =>
                                throw new Exception(s"Input ${iName} should not be provided")
                        }
                        evaluateWomExpression(defaultExpr, accuValues)

                    // There are several distinct cases
                    //
                    //   default   input           result   comments
                    //   -------   -----           ------   --------
                    //   d         not-specified   d
                    //   _         null            None     override
                    //   _         Some(v)         Some(v)
                    //
                    case OptionalInputDefinition(iName, WomOptionalType(womType), _, _) =>
                        fields.get(iName.value) match {
                            case None =>
                                // this key is not specified in the input
                                WomOptionalValue(womType, None)
                            case Some(JsNull) =>
                                // the input is null
                                WomOptionalValue(womType, None)
                            case Some(x) =>
                                val value:WomValue = unpackJobInput(womType, x)
                                WomOptionalValue(womType, Some(value))
                        }
                }
                accu + (inpDfn -> value)
        }
    }

    // find all file URLs in a Wom value
    private def findFiles(v : WomValue) : Vector[String] = {
        v match {
            case (WomSingleFile(s)) => Vector(s)
            case (WomMap(_, m: Map[WomValue, WomValue])) =>
                m.foldLeft(Vector.empty[String]) {
                    case (accu, (k,v)) =>
                        findFiles(k) ++ findFiles(v) ++ accu
                }
            case WomPair(lf, rt) =>
                findFiles(lf) ++ findFiles(rt)
            case _ : WomObject =>
                throw new Exception("WOM objects not supported")

            // empty array
            case (WomArray(_, value: Seq[WomValue])) =>
                value.map(findFiles).flatten.toVector

            case (WomOptionalValue(_, Some(value))) =>
                findFiles(value)

            case _ => Vector.empty
        }
    }

    // Create a local path for a DNAx file. The normal location, is to download
    // to the $HOME/inputs directory. However, since downloaded files may have the same
    // name, we may need to disambiguate them.
    private def createUniqueDownloadPath(dxUrl: DxURL,
                                         existingFiles: Set[Path],
                                         inputsDir: Path) : Path = {
        val DxPath.Parts(basename, dxFile) = DxPath.parse(dxUrl)
        val shortPath = inputsDir.resolve(basename)
        if (!(existingFiles contains shortPath))
            return shortPath

        // The path is already used for a file with this name. Try to place it
        // in directories: [inputs/1, inputs/2, ... ]
        System.err.println(s"Disambiguating file ${dxFile.getId} with name ${basename}")

        for (dirNum <- 1 to DISAMBIGUATION_DIRS_MAX_NUM) {
            val dir:Path = inputsDir.resolve(dirNum.toString)
            val longPath = dir.resolve(basename)
            if (!(existingFiles contains longPath))
                return longPath
        }
        throw new Exception(s"""|Tried to download ${dxFile.getId} ${basename} to local filesystem
                                |at ${inputsDir}/*/${basename}, all are taken"""
                                .stripMargin.replaceAll("\n", " "))
    }


    // Recursively go into a womValue, and replace file string with
    // an equivalent. Use the [translation] map to translate.
    private def translateFiles(womValue: WomValue,
                               translation: Map[String, String]) : WomValue = {
        womValue match {
            // primitive types, pass through
            case WomBoolean(_) | WomInteger(_) | WomFloat(_) | WomString(_) => womValue

            // single file
            case WomSingleFile(s) =>
                translation.get(s) match {
                    case None =>
                        throw new Exception(s"Did not localize file ${s}")
                    case Some(s2) =>
                        WomSingleFile(s2)
                }

            // Maps
            case (WomMap(t: WomMapType, m: Map[WomValue, WomValue])) =>
                val m1 = m.map{ case (k, v) =>
                    val k1 = translateFiles(k, translation)
                    val v1 = translateFiles(v, translation)
                    k1 -> v1
                }
                WomMap(t, m1)

            case (WomPair(l, r)) =>
                val left = translateFiles(l, translation)
                val right = translateFiles(r, translation)
                WomPair(left, right)

            case WomObject(_,_) =>
                throw new Exception("WOM objects not supported")

            case WomArray(t: WomArrayType, a: Seq[WomValue]) =>
                val a1 = a.map{ v => translateFiles(v, translation) }
                WomArray(t, a1)

            case WomOptionalValue(t,  None) =>
                WomOptionalValue(t, None)
            case WomOptionalValue(t, Some(v)) =>
                val v1 = translateFiles(v, translation)
                WomOptionalValue(t, Some(v1))

            case _ =>
                throw new AppInternalException(s"Unsupported wom value ${womValue}")
        }
    }

    // Recursively go into a womValue, and replace cloud URLs with the
    // equivalent local path.
    private def replaceURLsWithLocalPaths(womValue: WomValue,
                                          localizationPlan: Map[DxURL, Path]) : WomValue = {
        val translation : Map[String, String] = localizationPlan.map{
            case (dxUrl, path) =>  dxUrl.value -> path.toString
        }.toMap
        translateFiles(womValue, translation)
    }

    // Recursively go into a womValue, and replace cloud URLs with the
    // equivalent local path.
    private def replaceLocalPathsWithURLs(womValue: WomValue,
                                          path2dxUrl : Map[Path, DxURL]) : WomValue = {
        val translation : Map[String, String] = path2dxUrl.map{
            case (path, dxUrl) => path.toString -> dxUrl.value
        }.toMap
        translateFiles(womValue, translation)
    }



    // After applying [loadInputs] above, we have the job inputs in the correct format,
    // except for files. These are represented as dx URLs (dx://proj-xxxx:file-yyyy::/A/B/C.txt)
    // instead of local files (/home/C.txt).
    //
    // 1. Figure out what files to download.
    // 2. Return a new map with the localized files replacing the platform files.
    //    Also return a mapping from the dxURL to the local path.
    //
    // Notes:
    // A file may be referenced more than once, we want to download it
    // just once.
    def localizeFiles(inputs: Map[InputDefinition, WomValue],
                      inputsDir: Path) : (Map[InputDefinition, WomValue], Map[DxURL, Path]) = {
        val fileURLs : Vector[DxURL] = inputs.values.map(findFiles).flatten.toVector.map(x => DxURL(x))

        // remove duplicates; we want to download each file just once
        val filesToDownload: Set[DxURL] = fileURLs.toSet

        // Choose a local path for each cloud file
        val dxUrl2path: Map[DxURL, Path] =
            filesToDownload.foldLeft(Map.empty[DxURL, Path]) { case (accu, url) =>
                val existingFiles = accu.values.toSet
                val path = createUniqueDownloadPath(url, existingFiles, inputsDir)
                accu + (url -> path)
            }

        // download the files from the cloud.
        // This could be done in parallel using the download agent.
        // Right now, we are downloading the files one at a time
        dxUrl2path.foreach{ case (dxURL, localPath) =>
            val DxPath.Parts(_,dxFile) = DxPath.parse(dxURL)
            downloadFile(localPath, dxFile)
        }

        // Replace the dxURLs with local file paths
        val localizedInputs = inputs.map{ case (inpDef, womValue) =>
            val v1 = replaceURLsWithLocalPaths(womValue, dxUrl2path)
            inpDef -> v1
        }

        (localizedInputs, dxUrl2path)
    }

    // We have task outputs, where files are stored locally. Upload the files to
    // the cloud, and replace the WomValues with dxURLs.
    //
    // Edge cases:
    // 1) If a file is already on the cloud, do not re-upload it. The content has not
    // changed because files are immutable.
    def delocalizeFiles(outputs: Map[String, WomValue],
                        dxUrl2path: Map[DxURL, Path]) : Map[String, WomValue] = {
        val localFiles : Vector[Path] = outputs.values.map(findFiles).flatten.toVector.map(x => Paths.get(x))

        // 1) Filter out files that are already on the cloud.
        // 2) A path can appear multiple times, make paths unique
        //    so we don't upload the same file twice.
        val filesOnCloud : Set[Path] =  dxUrl2path.values.toSet
        val pathsToUpload : Set[Path] = localFiles.filter{ case path =>
            !(filesOnCloud contains path)
        }.toSet

        // upload the files; this could be in parallel in the future.
        val uploaded_path2dxUrl : Map[Path, DxURL] = pathsToUpload.map{ path =>
            val dxFile = uploadFile(path)

            path -> DxURL(DxPath.dxFileToURL(dxFile))
        }.toMap

        // invert the dxUrl2path map
        val alreadyOnCloud_path2dxURL : Map[Path, DxURL] = dxUrl2path.foldLeft(Map.empty[Path, DxURL]) {
            case (accu, (dxUrl, path)) =>
                accu + (path -> dxUrl)
        }
        val path2dxUrl = alreadyOnCloud_path2dxURL ++ uploaded_path2dxUrl

        // Replace the local file paths with dxURLs
        outputs.map{ case (outputName, womValue) =>
            val v1 = replaceLocalPathsWithURLs(womValue, path2dxUrl)
            outputName -> v1
        }.toMap
    }
}
