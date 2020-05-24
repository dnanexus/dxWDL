package dxWDL.exec

import java.nio.file.{Files, Path, Paths}
import spray.json._
import wdlTools.eval.{Context => EvalContext, Eval => WdlExprEval, WdlValues}
import wdlTools.syntax.WdlVersion
import wdlTools.types.{TypedAbstractSyntax => TAT, WdlTypes}
import wdlTools.util.{EvalConfig, Options}

import dxWDL.base._
import dxWDL.base.Utils.{FLAT_FILES_SUFFIX}
import dxWDL.dx.{DxFile, DxUtils, DxdaManifest, DxfuseManifest}
import dxWDL.util._

case class JobInputOutput(dxIoFunctions: DxIoFunctions,
                          structDefs: Map[String, WdlTypes.T],
                          wdlVersion : WdlVersion,
                          runtimeDebugLevel: Int) {
  private val verbose = (runtimeDebugLevel >= 1)
  private val utlVerbose = Verbose(runtimeDebugLevel >= 1, false, Set.empty)
  private val wdlVarLinksConverter =
    WdlVarLinksConverter(utlVerbose, dxIoFunctions.fileInfoDir, structDefs)

  private val DISAMBIGUATION_DIRS_MAX_NUM = 200

  def unpackJobInput(name: String, wdlType: WdlTypes.T, jsv: JsValue): WdlValues.V = {
    val (wdlValue, _) = wdlVarLinksConverter.unpackJobInput(name, wdlType, jsv)
    wdlValue
  }

  def unpackJobInputFindRefFiles(wdlType: WdlTypes.T, jsv: JsValue): Vector[DxFile] = {
    val (_, dxFiles) = wdlVarLinksConverter.unpackJobInput("", wdlType, jsv)
    dxFiles
  }


  val evaluator : WdlExprEval = {
    val evalOpts = Options(typeChecking = wdlTools.util.TypeCheckingRegime.Strict,
                           antlr4Trace = false,
                           localDirectories = Vector.empty,
                           verbosity = wdlTools.util.Verbosity.Quiet)
    val evalCfg = EvalConfig(dxIoFunctions.config.homeDir,
                             dxIoFunctions.config.tmpDir,
                             dxIoFunctions.config.stdout,
                             dxIoFunctions.config.stderr)

    new WdlExprEval(evalOpts, evalCfg, wdlVersion, None)
  }

  private def evaluateWomExpression(expr: TAT.Expr,
                                    wdlType: WdlTypes.T,
                                    env: Map[String, WdlValues.V]): WdlValues.V = {
    evaluator.applyExprAndCoerce(expr, wdlType, EvalContext(env))
  }

  // Read the job-inputs JSON file, and convert the variables
  // from JSON to WOM values. No files are downloaded here.
  def loadInputs(inputs: JsValue,
                 callable: TAT.Callable): Map[TAT.InputDefinition, WdlValues.V] = {
    // Discard auxiliary fields
    val fields: Map[String, JsValue] = inputs.asJsObject.fields
      .filter { case (fieldName, _) => !fieldName.endsWith(FLAT_FILES_SUFFIX) }

    // Get the declarations matching the input fields.
    // Create a mapping from each key to its WDL value
    callable.inputs.foldLeft(Map.empty[TAT.InputDefinition, WdlValues.V]) {
      case (accu, inpDfn) =>
        val accuValues = accu.map {
          case (inpDfn, value) =>
            inpDfn.name -> value
        }.toMap
        val value: WdlValues.V = inpDfn match {
          // A required input, no default.
          case TAT.RequiredInputDefinition(iName, wdlType, _) =>
            fields.get(iName) match {
              case None =>
                throw new Exception(s"Input ${iName} is required but not provided")
              case Some(x: JsValue) =>
                // Conversion from JSON to WdlValues.V
                unpackJobInput(iName, wdlType, x)
            }

          // An input definition that has a default value supplied.
          // Typical WDL example would be a declaration like: "Int x = 5"
          case TAT.OverridableInputDefinitionWithDefault(iName, wdlType, defaultExpr, _) =>
            fields.get(iName) match {
              case None =>
                // use the default expression
                evaluateWomExpression(defaultExpr, wdlType, accuValues)
              case Some(x: JsValue) =>
                unpackJobInput(iName, wdlType, x)
            }

          // There are several distinct cases
          //
          //   default   input           result   comments
          //   -------   -----           ------   --------
          //   d         not-specified   d
          //   _         null            None     override
          //   _         Some(v)         Some(v)
          //
          case TAT.OptionalInputDefinition(iName, WdlTypes.T_Optional(wdlType), _) =>
            fields.get(iName) match {
              case None =>
                // this key is not specified in the input
                WdlValues.V_Null
              case Some(JsNull) =>
                // the input is null
                WdlValues.V_Null
              case Some(x) =>
                val value: WdlValues.V = unpackJobInput(iName, wdlType, x)
                WdlValues.V_Optional(value)
            }
        }
        accu + (inpDfn -> value)
    }
  }

  // find all file URLs in a Wom value
  private def findFiles(v: WdlValues.V): Vector[Furl] = {
    v match {
      case WdlValues.V_File(s) => Vector(Furl.parse(s))
      case WdlValues.V_Map(m) =>
        m.foldLeft(Vector.empty[Furl]) {
          case (accu, (k, v)) =>
            findFiles(k) ++ findFiles(v) ++ accu
        }
      case WdlValues.V_Pair(lf, rt) =>
        findFiles(lf) ++ findFiles(rt)

      // empty array
      case WdlValues.V_Array(value) =>
        value.map(findFiles).flatten.toVector

      case WdlValues.V_Optional(value) =>
        findFiles(value)

      // structs
      case WdlValues.V_Object(m) =>
        m.map {
            case (k, v) => findFiles(v)
          }
          .flatten
          .toVector

      case WdlValues.V_Struct(_, m) =>
        m.map {
            case (k, v) => findFiles(v)
          }
          .flatten
          .toVector

      case _ => Vector.empty
    }
  }

  // Create a local path for a DNAx file. The normal location, is to download
  // to the $HOME/inputs directory. However, since downloaded files may have the same
  // name, we may need to disambiguate them.
  private def createUniqueDownloadPath(basename: String,
                                       dxFile: DxFile,
                                       existingFiles: Set[Path],
                                       inputsDir: Path): Path = {
    val shortPath = inputsDir.resolve(basename)
    if (!(existingFiles contains shortPath))
      return shortPath

    // The path is already used for a file with this name. Try to place it
    // in directories: [inputs/1, inputs/2, ... ]
    System.err.println(s"Disambiguating file ${basename}")

    for (dirNum <- 1 to DISAMBIGUATION_DIRS_MAX_NUM) {
      val dir: Path = inputsDir.resolve(dirNum.toString)
      val longPath = dir.resolve(basename)
      if (!(existingFiles contains longPath))
        return longPath
    }
    throw new Exception(
        s"""|Tried to download ${dxFile.getId} ${basename} to local filesystem
            |at ${inputsDir}/*/${basename}, all are taken""".stripMargin
          .replaceAll("\n", " ")
    )
  }

  // Recursively go into a womValue, and replace file string with
  // an equivalent. Use the [translation] map to translate.
  private def translateFiles(womValue: WdlValues.V, translation: Map[String, String]): WdlValues.V = {
    womValue match {
      // primitive types, pass through
      case WdlValues.V_Null |
          WdlValues.V_Boolean(_) |
          WdlValues.V_Int(_) |
          WdlValues.V_Float(_) |
          WdlValues.V_String(_) => womValue

      // single file
      case WdlValues.V_File(s) =>
        translation.get(s) match {
          case None =>
            throw new Exception(s"Did not localize file ${s}")
          case Some(s2) =>
            WdlValues.V_File(s2)
        }

      // Maps
      case WdlValues.V_Map(m) =>
        val m1 = m.map {
          case (k, v) =>
            val k1 = translateFiles(k, translation)
            val v1 = translateFiles(v, translation)
            k1 -> v1
        }
        WdlValues.V_Map(m1)

      case WdlValues.V_Pair(l, r) =>
        val left = translateFiles(l, translation)
        val right = translateFiles(r, translation)
        WdlValues.V_Pair(left, right)

      case WdlValues.V_Array(arr) =>
        val arr1 = arr.map { v =>
          translateFiles(v, translation)
        }.toVector
        WdlValues.V_Array(arr1)

      // special case: an optional file. If it doesn't exist,
      // return None
      case WdlValues.V_Optional(WdlValues.V_File(localPath)) =>
        translation.get(localPath) match {
          case None =>
            WdlValues.V_Null
          case Some(url) =>
            WdlValues.V_Optional(WdlValues.V_File(url))
        }

      case WdlValues.V_Optional(value) =>
        WdlValues.V_Optional(translateFiles(value, translation))

      case WdlValues.V_Struct(sName, m) =>
        val m2 = m.map {
          case (k, v) => k -> translateFiles(v, translation)
        }.toMap
        WdlValues.V_Struct(sName, m2)

      case WdlValues.V_Object(m) =>
        val m2 = m.map {
          case (k, v) => k -> translateFiles(v, translation)
        }.toMap
        WdlValues.V_Object(m2)

      case _ =>
        throw new AppInternalException(s"Unsupported wom value ${womValue}")
    }
  }

  // Recursively go into a womValue, and replace cloud URLs with the
  // equivalent local path.
  private def replaceFURLsWithLocalPaths(womValue: WdlValues.V,
                                         localizationPlan: Map[Furl, Path]): WdlValues.V = {
    val translation: Map[String, String] = localizationPlan.map {
      case (FurlDx(value, _, _), path) => value -> path.toString
      case (FurlLocal(p1), p2)         => p1 -> p2.toString
    }.toMap
    translateFiles(womValue, translation)
  }

  // Recursively go into a womValue, and replace cloud URLs with the
  // equivalent local path.
  private def replaceLocalPathsWithURLs(womValue: WdlValues.V,
                                        path2furl: Map[Path, Furl]): WdlValues.V = {
    val translation: Map[String, String] = path2furl.map {
      case (path, dxUrl: FurlDx)    => path.toString -> dxUrl.value
      case (path, local: FurlLocal) => path.toString -> local.path
    }.toMap
    translateFiles(womValue, translation)
  }

  val PARAM_META_STREAM = "stream"
  val PARAM_META_DX_STREAM = "dx_stream"
  val PARAM_META_LOCALIZATION_OPTIONAL = "localizationOptional"

  // Figure out which files need to be streamed
  private def areStreaming(parameterMeta: Map[String, TAT.MetaValue],
                           inputs: Map[TAT.InputDefinition, WdlValues.V]): Set[Furl] = {
    inputs
      .map {
        case (iDef, womValue) =>
          if (dxIoFunctions.config.streamAllFiles) {
            findFiles(womValue)
          } else {
            // This is better than "iDef.parameterMeta", but it does not
            // work on draft2.
            parameterMeta.get(iDef.name) match {
              case Some(TAT.MetaValueString(PARAM_META_STREAM)) =>
                findFiles(womValue)
              case Some(TAT.MetaValueObject(value)) =>
                // This enables the stream annotation in the object form of metadata value, e.g.
                // bam_file : {
                //   stream : true
                // }
                // We also support two aliases, dx_stream and localizationOptional
                value
                  .filterKeys(
                      Set(PARAM_META_STREAM, PARAM_META_DX_STREAM, PARAM_META_LOCALIZATION_OPTIONAL)
                  )
                  .headOption match {
                  case Some((_, TAT.MetaValueBoolean(b))) if b =>
                    findFiles(womValue)
                  case _ => Vector.empty
                }
              case _ =>
                Vector.empty
            }
          }
      }
      .flatten
      .toSet
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
  def localizeFiles(
      parameterMeta: Map[String, TAT.MetaValue],
      inputs: Map[TAT.InputDefinition, WdlValues.V],
      inputsDir: Path
  ): (Map[TAT.InputDefinition, WdlValues.V], Map[Furl, Path], DxdaManifest, DxfuseManifest) = {
    val fileURLs: Vector[Furl] = inputs.values.map(findFiles).flatten.toVector
    val streamingFiles: Set[Furl] = areStreaming(parameterMeta, inputs)
    Utils.appletLog(verbose, s"streaming files = ${streamingFiles}")

    // remove duplicates; we want to download each file just once
    val filesToDownload: Set[Furl] = fileURLs.toSet

    // Choose a local path for each cloud file
    val furl2path: Map[Furl, Path] =
      filesToDownload.foldLeft(Map.empty[Furl, Path]) {
        case (accu, furl) =>
          furl match {
            case FurlLocal(p) =>
              // The file is already on the local disk, there
              // is no need to download it.
              //
              // TODO: make sure this file is NOT in the applet input/output
              // directories.
              accu + (FurlLocal(p) -> Paths.get(p))

            case dxUrl: FurlDx if streamingFiles contains dxUrl =>
              // file should be streamed
              val existingFiles = accu.values.toSet
              val (_, desc) = dxIoFunctions.fileInfoDir(dxUrl.dxFile.id)
              val path = createUniqueDownloadPath(desc.name,
                                                  dxUrl.dxFile,
                                                  existingFiles,
                                                  dxIoFunctions.config.dxfuseMountpoint)
              accu + (dxUrl -> path)

            case dxUrl: FurlDx =>
              // The file needs to be localized
              val existingFiles = accu.values.toSet
              val (_, desc) = dxIoFunctions.fileInfoDir(dxUrl.dxFile.id)
              val path = createUniqueDownloadPath(desc.name, dxUrl.dxFile, existingFiles, inputsDir)
              accu + (dxUrl -> path)
          }
      }

    // Create a manifest for all the streaming files; we'll use dxfuse to handle them.
    val filesToMount: Map[DxFile, Path] =
      furl2path.collect {
        case (dxUrl: FurlDx, localPath) if (streamingFiles contains dxUrl) =>
          dxUrl.dxFile -> localPath
      }
    val dxfuseManifest = DxfuseManifest.apply(filesToMount, dxIoFunctions)

    // Create a manifest for the download agent (dxda)
    val filesToDownloadWithDxda: Map[String, (DxFile, Path)] =
      furl2path.collect {
        case (dxUrl: FurlDx, localPath) if !(streamingFiles contains dxUrl) =>
          dxUrl.dxFile.id -> (dxUrl.dxFile, localPath)
      }
    val dxdaManifest = DxdaManifest.apply(filesToDownloadWithDxda)

    // Replace the dxURLs with local file paths
    val localizedInputs = inputs.map {
      case (inpDef, womValue) =>
        val v1 = replaceFURLsWithLocalPaths(womValue, furl2path)
        inpDef -> v1
    }

    (localizedInputs, furl2path, dxdaManifest, dxfuseManifest)
  }

  // We have task outputs, where files are stored locally. Upload the files to
  // the cloud, and replace the WdlValues.Vs with dxURLs.
  //
  // Edge cases:
  // 1) If a file is already on the cloud, do not re-upload it. The content has not
  // changed because files are immutable.
  // 2) A file that was initially local, does not need to be uploaded.
  def delocalizeFiles(outputs: Map[String, WdlValues.V],
                      furl2path: Map[Furl, Path]): Map[String, WdlValues.V] = {
    // Files that were local to begin with
    val localInputFiles: Set[Path] = furl2path.collect {
      case (FurlLocal(_), path) => path
    }.toSet

    val localOutputFilesAll: Vector[Path] = outputs.values
      .map(findFiles)
      .flatten
      .toVector
      .map {
        case FurlLocal(p) => Paths.get(p)
        case dxUrl: FurlDx =>
          throw new Exception(s"Should not find cloud file on local machine (${dxUrl})")
      }
      .toVector

    // Remove files that were local to begin with, and do not need to be uploaded to the cloud.
    val localOutputFiles: Vector[Path] = localOutputFilesAll.filter { path =>
      !(localInputFiles contains path)
    }.toVector

    // 1) A path can appear multiple times, make paths unique
    //    so we don't upload the same file twice.
    // 2) Filter out files that are already on the cloud.
    val filesOnCloud: Set[Path] = furl2path.values.toSet
    val pathsToUpload: Set[Path] = localOutputFiles.filter {
      case path =>
        !(filesOnCloud contains path)
    }.toSet

    // upload the files; this could be in parallel in the future.
    val uploaded_path2furl: Map[Path, Furl] = pathsToUpload.flatMap { path =>
      if (Files.exists(path)) {
        val dxFile = DxUtils.uploadFile(path, verbose)
        Some(path -> Furl.dxFileToFurl(dxFile, Map.empty)) // no cache
      } else {
        // The file does not exist on the local machine. This is
        // legal if it is optional.
        None
      }
    }.toMap

    // invert the furl2path map
    val alreadyOnCloud_path2furl: Map[Path, Furl] = furl2path.foldLeft(Map.empty[Path, Furl]) {
      case (accu, (furl, path)) =>
        accu + (path -> furl)
    }
    val path2furl = alreadyOnCloud_path2furl ++ uploaded_path2furl

    // Replace the files that need to be uploaded, file paths with FURLs
    outputs.map {
      case (outputName, womValue) =>
        val v1 = replaceLocalPathsWithURLs(womValue, path2furl)
        outputName -> v1
    }.toMap
  }
}
