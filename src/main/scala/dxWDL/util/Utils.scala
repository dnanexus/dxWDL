package dxWDL.util

import com.dnanexus._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.JsonNode
import com.typesafe.config._
import java.io.{ByteArrayOutputStream, ByteArrayInputStream}
import java.nio.charset.{StandardCharsets}
import java.nio.file.{Path, Paths, Files}
import java.util.Base64
import java.util.zip.{GZIPOutputStream, GZIPInputStream}
import scala.collection.JavaConverters._
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.sys.process._
import spray.json._
import wom.types._

object Utils {
    val APPLET_LOG_MSG_LIMIT = 1000
    val CHECKSUM_PROP = "dxWDL_checksum"
    val DEFAULT_RUNTIME_DEBUG_LEVEL = 1
    val DX_URL_PREFIX = "dx://"
    val DX_WDL_ASSET = "dxWDLrt"
    val DX_WDL_RUNTIME_CONF_FILE = "dxWDL_runtime.conf"
    val FLAT_FILES_SUFFIX = "___dxfiles"
    val INTERMEDIATE_RESULTS_FOLDER = "intermediate"
    val LAST_STAGE = "last"
    val LINK_INFO_FILENAME = "linking.json"
    val MAX_NUM_RENAME_TRIES = 100
    val MAX_STRING_LEN = 32 * 1024     // Long strings cause problems with bash and the UI
    val MAX_STAGE_NAME_LEN = 60       // maximal length of a workflow stage name
    val MAX_NUM_FILES_MOVE_LIMIT = 1000
    val META_INFO = "_metaInfo"
    val RESERVED_APPLET_INPUT_NAMES = Set(META_INFO)
    val UBUNTU_VERSION = "16.04"
    var traceLevel = 0

    lazy val dxEnv = DXEnvironment.create()

    // The version lives in application.conf
    def getVersion() : String = {
        val config = ConfigFactory.load("application.conf")
        config.getString("dxWDL.version")
    }

    // the regions live in dxWDL.conf
    def getRegions() : Map[String, String] = {
        val config = ConfigFactory.load(DX_WDL_RUNTIME_CONF_FILE)
        val l: List[Config] = config.getConfigList("dxWDL.region2project").asScala.toList
        val region2project:Map[String, String] = l.map{ pair =>
            val r = pair.getString("region")
            val projName = pair.getString("path")
            r -> projName
        }.toMap
        region2project
    }

    // Ignore a value. This is useful for avoiding warnings/errors
    // on unused variables.
    def ignore[A](value: A) : Unit = {}

    lazy val appCompileDirPath : Path = {
        val p = Paths.get("/tmp/dxWDL_Compile")
        safeMkdir(p)
        p
    }

    // Convert a fully qualified name to a local name.
    // Examples:
    //   SOURCE         RESULT
    //   lib.concat     concat
    //   lib.sum_list   sum_list
    def getUnqualifiedName(fqn: String) : String = {
        if (fqn contains ".")
            fqn.split("\\.").last
        else
            fqn
    }

    // Is this a WDL type that maps to a native DX type?
    def isNativeDxType(wdlType: WomType) : Boolean = {
        wdlType match {
            // optional dx:native types
            case WomOptionalType(WomBooleanType) => true
            case WomOptionalType(WomIntegerType) => true
            case WomOptionalType(WomFloatType) => true
            case WomOptionalType(WomStringType) => true
            case WomOptionalType(WomSingleFileType) => true
            case WomMaybeEmptyArrayType(WomBooleanType) => true
            case WomMaybeEmptyArrayType(WomIntegerType) => true
            case WomMaybeEmptyArrayType(WomFloatType) => true
            case WomMaybeEmptyArrayType(WomStringType) => true
            case WomMaybeEmptyArrayType(WomSingleFileType) => true

                // compulsory dx:native types
            case WomBooleanType => true
            case WomIntegerType => true
            case WomFloatType => true
            case WomStringType => true
            case WomSingleFileType => true
            case WomNonEmptyArrayType(WomBooleanType) => true
            case WomNonEmptyArrayType(WomIntegerType) => true
            case WomNonEmptyArrayType(WomFloatType) => true
            case WomNonEmptyArrayType(WomStringType) => true
            case WomNonEmptyArrayType(WomSingleFileType) => true

            // A tricky, but important case, is `Array[File]+?`. This
            // cannot be converted into a dx file array, unfortunately.
            case _ => false
        }
    }

    // Create a file from a string
    def writeFileContent(path : Path, str : String) : Unit = {
        Files.write(path, str.getBytes(StandardCharsets.UTF_8))
    }

    def readFileContent(path : Path) : String = {
        // Java 8 Example - Uses UTF-8 character encoding
        val lines = Files.readAllLines(path, StandardCharsets.UTF_8).asScala.toList
        val sb = new StringBuilder(1024)
        lines.foreach{ line =>
            sb.append(line)
            sb.append("\n")
        }
        sb.toString
    }

    def exceptionToString(e : Throwable) : String = {
        val sw = new java.io.StringWriter
        e.printStackTrace(new java.io.PrintWriter(sw))
        sw.toString
    }

    // Convert from spray-json to jackson JsonNode
    // Used to convert into the JSON datatype used by dxjava
    private val objMapper : ObjectMapper = new ObjectMapper()

    def jsonNodeOfJsValue(jsValue : JsValue) : JsonNode = {
        val s : String = jsValue.prettyPrint
        objMapper.readTree(s)
    }

    // Convert from jackson JsonNode to spray-json
    def jsValueOfJsonNode(jsNode : JsonNode) : JsValue = {
        jsNode.toString().parseJson
    }


    // Create a dx link to a field in an execution. The execution could
    // be a job or an analysis.
    def makeEBOR(dxExec: DXExecution, fieldName: String) : JsValue = {
        if (dxExec.isInstanceOf[DXJob]) {
            JsObject("$dnanexus_link" -> JsObject(
                         "field" -> JsString(fieldName),
                         "job" -> JsString(dxExec.getId)))
        } else if (dxExec.isInstanceOf[DXAnalysis]) {
            JsObject("$dnanexus_link" -> JsObject(
                         "field" -> JsString(fieldName),
                         "analysis" -> JsString(dxExec.getId)))
        } else {
            throw new Exception(s"makeEBOR can't work with ${dxExec.getId}")
        }
    }

    def runSubJob(entryPoint:String,
                  instanceType:Option[String],
                  inputs:JsValue,
                  dependsOn: Vector[DXExecution],
                  verbose: Boolean) : DXJob = {
        val fields = Map(
            "function" -> JsString(entryPoint),
            "input" -> inputs
        )
        val instanceFields = instanceType match {
            case None => Map.empty
            case Some(iType) =>
                Map("systemRequirements" -> JsObject(
                        entryPoint -> JsObject("instanceType" -> JsString(iType))
                    ))
        }
        val dependsFields =
            if (dependsOn.isEmpty) {
                Map.empty
            } else {
                val execIds = dependsOn.map{ dxExec => JsString(dxExec.getId) }.toVector
                Map("dependsOn" -> JsArray(execIds))
            }
        val req = JsObject(fields ++ instanceFields ++ dependsFields)
        appletLog(verbose, s"subjob request=${req.prettyPrint}")

        val retval: JsonNode = DXAPI.jobNew(jsonNodeOfJsValue(req), classOf[JsonNode])
        val info: JsValue =  jsValueOfJsonNode(retval)
        val id:String = info.asJsObject.fields.get("id") match {
            case Some(JsString(x)) => x
            case _ => throw new AppInternalException(
                s"Bad format returned from jobNew ${info.prettyPrint}")
        }
        DXJob.getInstance(id)
    }

    // dx does not allow dots in variable names, so we
    // convert them to underscores.
    def transformVarName(varName: String) : String = {
        varName.replaceAll("\\.", "___")
    }

    def revTransformVarName(varName: String) : String = {
        varName.replaceAll("___", "\\.")
    }


    // Dots are illegal in applet variable names.
    def encodeAppletVarName(varName : String) : String = {
        if (varName contains ".")
            throw new Exception(s"Variable ${varName} includes the illegal symbol \\.")
        varName
    }

    // recursive directory delete
    //    http://stackoverflow.com/questions/25999255/delete-directory-recursively-in-scala
    def deleteRecursive(file : java.io.File) : Unit = {
        if (file.isDirectory) {
            Option(file.listFiles).map(_.toList).getOrElse(Nil).foreach(deleteRecursive(_))
        }
        file.delete
    }

    def safeMkdir(path : Path) : Unit = {
        if (!Files.exists(path)) {
            Files.createDirectories(path)
        } else {
            // Path exists, make sure it is a directory, and not a file
            if (!Files.isDirectory(path))
                throw new Exception(s"Path ${path} exists, but is not a directory")
        }
    }

    // Add a suffix to a filename, before the regular suffix. For example:
    //  xxx.wdl -> xxx.simplified.wdl
    def replaceFileSuffix(src: Path, suffix: String) : String = {
        val fName = src.toFile().getName()
        val index = fName.lastIndexOf('.')
        if (index == -1) {
            fName + suffix
        }
        else {
            val prefix = fName.substring(0, index)
            prefix + suffix
        }
    }

// From: https://gist.github.com/owainlewis/1e7d1e68a6818ee4d50e
// By: owainlewis
//
    def gzipCompress(input: Array[Byte]): Array[Byte] = {
        val bos = new ByteArrayOutputStream(input.length)
        val gzip = new GZIPOutputStream(bos)
        gzip.write(input)
        gzip.close()
        val compressed = bos.toByteArray
        bos.close()
        compressed
    }

    def gzipDecompress(compressed: Array[Byte]): String = {
        val inputStream = new GZIPInputStream(new ByteArrayInputStream(compressed))
        scala.io.Source.fromInputStream(inputStream).mkString
    }

    def gzipAndBase64Encode(buf: String) : String = {
        val bytes = buf.getBytes
        val gzBytes = gzipCompress(bytes)
        Base64.getEncoder.encodeToString(gzBytes)
    }

    def base64DecodeAndGunzip(buf64: String) : String = {
        val ba : Array[Byte] = Base64.getDecoder.decode(buf64.getBytes)
        gzipDecompress(ba)
    }

    // Job input, output,  error, and info files are located relative to the home
    // directory
    def jobFilesOfHomeDir(homeDir : Path) : (Path, Path, Path, Path) = {
        val jobInputPath = homeDir.resolve("job_input.json")
        val jobOutputPath = homeDir.resolve("job_output.json")
        val jobErrorPath = homeDir.resolve("job_error.json")
        val jobInfoPath = homeDir.resolve("dnanexus-job.json")
        (jobInputPath, jobOutputPath, jobErrorPath, jobInfoPath)
    }

    // describe a project, and extract fields that not currently available
    // through dxjava.
    def projectDescribeExtraInfo(dxProject: DXProject) : (String,String) = {
        val rep = DXAPI.projectDescribe(dxProject.getId(), classOf[JsonNode])
        val jso:JsObject = jsValueOfJsonNode(rep).asJsObject

        val billTo = jso.fields.get("billTo") match {
            case Some(JsString(x)) => x
            case _ => throw new Exception(
                s"Failed to get billTo from project ${dxProject.getId()}")
        }
        val region = jso.fields.get("region") match {
            case Some(JsString(x)) => x
            case _ => throw new Exception(
                s"Failed to get region from project ${dxProject.getId()}")
        }
        (billTo,region)
    }

    // Run a child process and collect stdout and stderr into strings
    def execCommand(cmdLine : String, timeout: Option[Int] = None) : (String, String) = {

        val cmds = Seq("/bin/sh", "-c", cmdLine)
        val outStream = new StringBuilder()
        val errStream = new StringBuilder()
        val logger = ProcessLogger(
            (o: String) => { outStream.append(o ++ "\n") },
            (e: String) => { errStream.append(e ++ "\n") }
        )

        val p : Process = Process(cmds).run(logger, false)
        timeout match {
            case None =>
                // blocks, and returns the exit code. Does NOT connect
                // the standard in of the child job to the parent
                val retcode = p.exitValue()
                if (retcode != 0) {
                    System.err.println(s"STDOUT: ${outStream.toString()}")
                    System.err.println(s"STDERR: ${errStream.toString()}")
                    throw new Exception(s"Error running command ${cmdLine}")
                }
            case Some(nSec) =>
                val f = Future(blocking(p.exitValue()))
                try {
                    Await.result(f, duration.Duration(nSec, "sec"))
                } catch {
                    case _: TimeoutException =>
                        p.destroy()
                        throw new Exception(s"Timeout exceeded (${nSec} seconds)")
                }
        }
        (outStream.toString(), errStream.toString())
    }


    // Download platform file contents directly into an in-memory string.
    // This makes sense for small files.
    def downloadString(dxfile: DXFile) : String = {
        val bytes = dxfile.downloadBytes()
        new String(bytes, StandardCharsets.UTF_8)
    }

    // Parse a dnanexus file descriptor. Examples:
    //
    // "$dnanexus_link": {
    //    "project": "project-BKJfY1j0b06Z4y8PX8bQ094f",
    //    "id": "file-BKQGkgQ0b06xG5560GGQ001B"
    //   }
    //
    //  {"$dnanexus_link": "file-F0J6JbQ0ZvgVz1J9q5qKfkqP"}
    //
    def dxFileFromJsValue(jsValue : JsValue) : DXFile = {
        val innerObj = jsValue match {
            case JsObject(fields) =>
                fields.get("$dnanexus_link") match {
                    case None => throw new AppInternalException(s"Non-dxfile json $jsValue")
                    case Some(x) => x
                }
            case  _ =>
                throw new AppInternalException(s"Non-dxfile json $jsValue")
        }

        val (fid, projId) : (String, Option[String]) = innerObj match {
            case JsString(fid) =>
                // We just have a file-id
                (fid, None)
            case JsObject(linkFields) =>
                // file-id and project-id
                val fid =
                    linkFields.get("id") match {
                        case Some(JsString(s)) => s
                        case _ => throw new AppInternalException(s"No file ID found in $jsValue")
                    }
                linkFields.get("project") match {
                    case Some(JsString(pid : String)) => (fid, Some(pid))
                    case _ => (fid, None)
                }
            case _ =>
                throw new AppInternalException(s"Could not parse a dxlink from $innerObj")
        }

        projId match {
            case None => DXFile.getInstance(fid)
            case Some(pid) => DXFile.getInstance(fid, DXProject.getInstance(pid))
        }
    }

    def dxFileToJsValue(dxFile: DXFile) : JsValue = {
        jsValueOfJsonNode(dxFile.getLinkAsJson)
    }

    // types
    def isOptional(t: WomType) : Boolean = {
        t match {
            case WomOptionalType(_) => true
            case t => false
        }
    }

    // We need to deal with types like:
    //     Int??, Array[File]??
    def stripOptional(t: WomType) : WomType = {
        t match {
            case WomOptionalType(x) => stripOptional(x)
            case x => x
        }
    }

    // Replace all special json characters from with a white space.
    def sanitize(s : String) : String = {
        def sanitizeChar(ch: Char) : String = ch match {
            case '}' => " "
            case '{' => " "
            case '$' => " "
            case '/' => " "
            case '\\' => " "
            case '\"' => " "
            case '\'' => " "
            case _ if (ch.isLetterOrDigit) => ch.toString
            case _ if (ch.isControl) =>  " "
            case _ => ch.toString
        }
        if (s != null)
            s.flatMap(sanitizeChar)
        else
            ""
    }

    // Logging output for applets at runtime
    def appletLog(verbose: Boolean,
                  msg: String,
                  limit: Int = APPLET_LOG_MSG_LIMIT) : Unit = {
        if (verbose) {
            val shortMsg =
                if (msg.length > limit)
                    "Message is too long for logging"
                else
                    msg
            System.err.println(shortMsg)
        }
    }

    def genNSpaces(n: Int) = s"${" " * n}"

    def traceLevelSet(i: Int) : Unit = {
        traceLevel = i
    }

    def traceLevelInc() : Unit = {
        traceLevel += 1
    }

    def traceLevelDec() : Unit = {
        if (traceLevel > 0)
            traceLevel -= 1
    }

        // Used by the compiler to provide more information to the user
    def trace(verbose: Boolean, msg: String) : Unit = {
        if (!verbose)
            return
        val indent = genNSpaces(traceLevel * 2)
        System.err.println(indent + msg)
    }

    // color warnings yellow
    def warning(verbose: Verbose, msg:String) : Unit = {
        if (verbose.quiet)
            return;
        System.err.println(Console.YELLOW + msg + Console.RESET)
    }

    def error(msg: String) : Unit = {
        System.err.println(Console.RED + msg + Console.RESET)
    }

    // copy asset to local project, if it isn't already here.
    def cloneAsset(assetRecord: DXRecord,
                   dxProject: DXProject,
                   pkgName: String,
                   rmtProject: DXProject,
                   verbose: Verbose) : Unit = {
        if (dxProject == rmtProject) {
            trace(verbose.on, s"The asset ${pkgName} is from this project ${rmtProject.getId}, no need to clone")
            return
        }
        trace(verbose.on, s"The asset ${pkgName} is from a different project ${rmtProject.getId}")

        // clone
        val req = JsObject( "objects" -> JsArray(JsString(assetRecord.getId)),
                            "project" -> JsString(dxProject.getId),
                            "destination" -> JsString("/"))
        val rep = DXAPI.projectClone(rmtProject.getId,
                                     jsonNodeOfJsValue(req),
                                     classOf[JsonNode])
        val repJs:JsValue = jsValueOfJsonNode(rep)

        val exists = repJs.asJsObject.fields.get("exists") match {
            case None => throw new Exception("API call did not returnd an exists field")
            case Some(JsArray(x)) => x.map {
                case JsString(id) => id
                case _ => throw new Exception("bad type, not a string")
            }.toVector
            case other => throw new Exception(s"API call returned invalid exists field")
        }
        val existingRecords = exists.filter(_.startsWith("record-"))
        existingRecords.size match {
            case 0 =>
                val localAssetRecord = DXRecord.getInstance(assetRecord.getId)
                trace(verbose.on, s"Created ${localAssetRecord.getId} pointing to asset ${pkgName}")
            case 1 =>
                trace(verbose.on, s"The project already has a record pointing to asset ${pkgName}")
            case _ =>
                throw new Exception(s"clone returned too many existing records ${exists}")
        }
    }
}
