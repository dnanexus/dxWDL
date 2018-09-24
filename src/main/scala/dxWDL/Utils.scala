package dxWDL

import cats.data.Validated
import cats.data.Validated.{Invalid, Valid}
import com.dnanexus._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.JsonNode
import com.typesafe.config._
import common.validation.ErrorOr.ErrorOr
import java.io.PrintStream
import java.nio.charset.{Charset, StandardCharsets}
import java.nio.file.{Path, Paths, Files}
import java.util.Base64
import scala.collection.JavaConverters._
import scala.concurrent._
import scala.util.{Failure, Success}
import ExecutionContext.Implicits.global
import scala.sys.process._
import spray.json._
import wom.expression._
import wom.types._
import wom.values._

object Utils {
    // A stand in for the DXWorkflow.Stage inner class (we don't have a constructor for it)
    case class DXWorkflowStage(id: String) {
        def getId() = id

        def getInputReference(inputName:String) : JsValue = {
            JsObject("$dnanexus_link" -> JsObject(
                         "stage" -> JsString(id),
                         "inputField" -> JsString(inputName)))
        }
        def getOutputReference(outputName:String) : JsValue = {
            JsObject("$dnanexus_link" -> JsObject(
                         "stage" -> JsString(id),
                         "outputField" -> JsString(outputName)))
        }
    }

    val APPLET_LOG_MSG_LIMIT = 1000
    val CHECKSUM_PROP = "dxWDL_checksum"
    val COMMAND_DEFAULT_BRACKETS = ("{", "}")
    val COMMAND_HEREDOC_BRACKETS = ("<<<", ">>>")
    val COMMON = "common"
    val DEFAULT_INSTANCE_TYPE = "mem1_ssd1_x4"
    val DEFAULT_RUNTIME_DEBUG_LEVEL = 1
    val DECOMPOSE_MAX_NUM_RENAME_TRIES = 100
    val DISAMBIGUATION_DIRS_MAX_NUM = 10
    val DOWNLOAD_RETRY_LIMIT = 3
    val DX_FUNCTIONS_FILES = "dx_functions_files.json"
    val DX_HOME = "/home/dnanexus"
    val DX_URL_PREFIX = "dx://"
    val DX_WDL_ASSET = "dxWDLrt"
    val DX_WDL_RUNTIME_CONF_FILE = "dxWDL_runtime.conf"
    val FLAT_FILES_SUFFIX = "___dxfiles"
    val INSTANCE_TYPE_DB_FILENAME = "instanceTypeDB.json"
    val INTERMEDIATE_RESULTS_FOLDER = "intermediate"
    val LAST_STAGE = "last"
    val LOCAL_DX_FILES_CHECKPOINT_FILE = "localized_files.json"
    val LINK_INFO_FILENAME = "linking.json"
    val MAX_NUM_REDUCE_ITERATIONS = 100
    val MAX_STRING_LEN = 8 * 1024     // Long strings cause problems with bash and the UI
    val MAX_STAGE_NAME_LEN = 60       // maximal length of a workflow stage name
    val MAX_NUM_FILES_MOVE_LIMIT = 1000
    val OUTPUT_SECTION = "outputs"
    val REORG = "reorg"

    // All the words defined in the WDL language, and NOT to be confused
    // with identifiers.
    val RESERVED_WORDS: Set[String] = Set(
        "stdout", "stderr",
        "true", "false",
        "left", "right",
        "if", "scatter", "else", "then"
    )

    val STDLIB_FUNCTIONS: Set[String] = Set(
        "read_lines", "read_tsv", "read_map",
        "read_object", "read_objects", "read_json",
        "read_int", "read_string", "read_float", "read_boolean",

        "write_lines", "write_tsv", "write_map",
        "write_object", "write_objects", "write_json",
        "write_int", "write_string", "write_float", "write_boolean",

        "size", "sub", "range",
        "transpose", "zip", "cross", "length", "flatten", "prefix",
        "select_first", "select_all", "defined", "basename",
        "floor", "ceil", "round",
    )

    val RUNNER_TASK_ENV_FILE = "taskEnv.json"
    val UPLOAD_RETRY_LIMIT = DOWNLOAD_RETRY_LIMIT

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

    lazy val execDirPath : Path = {
        val currentDir = System.getProperty("user.dir")
        val p = Paths.get(currentDir, "execution")
        safeMkdir(p)
        p
    }

    // This directory has to reside under the user home directory. If
    // a task runs under docker, the container will need access to
    // temporary files created with stdlib calls like "write_lines".
    lazy val tmpDirPath : Path = {
        val currentDir = System.getProperty("user.dir")
        val p = Paths.get(currentDir, "job_scratch_space")
        safeMkdir(p)
        p
    }
    lazy val appCompileDirPath : Path = {
        val p = Paths.get("/tmp/dxWDL_Compile")
        safeMkdir(p)
        p
    }

    // Running applets download files from the platform to
    // this location
    lazy val inputFilesDirPath : Path = {
        val p = execDirPath.resolve("inputs")
        safeMkdir(p)
        p
    }

    // where script files are placed and generated
    def getMetaDirPath() : Path = {
        val p = execDirPath.resolve("meta")
        p
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


    // Check if the WDL expression is a constant. If so, calculate and return it.
    // Otherwise, return None.
    private def ifConstEval(expr: WomExpression) : Option[WomValue] = {
        val result: ErrorOr[WomValue] =
            expr.evaluateValue(Map.empty[String, WomValue], wom.expression.NoIoFunctionSet)
        result match {
            case Invalid(_) => None
            case Valid(x: WomValue) => Some(x)
        }
    }

    def isExpressionConst(expr: WomExpression) : Boolean = {
        ifConstEval(expr) match {
            case None => false
            case Some(_) => true
        }
    }

    def evalConst(expr: WomExpression) : WomValue = {
        ifConstEval(expr) match {
            case None => throw new Exception(s"Expression ${expr} is not a WDL constant")
            case Some(wdlValue) => wdlValue
        }
    }


    // Used to convert into the JSON datatype used by dxjava
    val objMapper : ObjectMapper = new ObjectMapper()

    // Get the dx:classes for inputs and outputs
    def loadExecInfo : (Map[String, DXIOParam], Map[String, DXIOParam]) = {
        val dxapp : DXApplet = dxEnv.getJob().describe().getApplet()
        val desc : DXApplet.Describe = dxapp.describe()
        val inputSpecRaw: List[InputParameter] = desc.getInputSpecification().asScala.toList
        val inputSpec:Map[String, DXIOParam] = inputSpecRaw.map(
            iSpec => iSpec.getName -> DXIOParam(iSpec.getIOClass, iSpec.isOptional)
        ).toMap
        val outputSpecRaw: List[OutputParameter] = desc.getOutputSpecification().asScala.toList
        val outputSpec:Map[String, DXIOParam] = outputSpecRaw.map(
            iSpec => iSpec.getName -> DXIOParam(iSpec.getIOClass, iSpec.isOptional)
        ).toMap

        // remove auxiliary fields
        (inputSpec.filter{ case (fieldName,_) => !fieldName.endsWith(FLAT_FILES_SUFFIX) },
         outputSpec.filter{ case (fieldName,_) => !fieldName.endsWith(FLAT_FILES_SUFFIX) })
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
                  dependsOn: Vector[DXExecution]) : DXJob = {
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
        System.err.println(s"subjob request=${req.prettyPrint}")
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
        varName.replaceAll("\\.", "_")
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

    def base64Encode(buf: String) : String = {
        Base64.getEncoder.encodeToString(buf.getBytes(StandardCharsets.UTF_8))
    }

    def base64Decode(buf64: String) : String = {
        val ba : Array[Byte] = Base64.getDecoder.decode(buf64.getBytes(StandardCharsets.UTF_8))
        ba.map(x => x.toChar).mkString
    }

    def unicodeFromHex(hexBuf: String) : String = {
        val output = new StringBuilder("")
        for (i <- 0 until hexBuf.length by 4) {
            val str = hexBuf.substring(i, i + 4)
            val unicodeCodepoint: Int  = Integer.parseInt(str, 16)
            val ch = Character.toChars(unicodeCodepoint).charAt(0)
            output.append(ch)
        }
        output.toString
    }

    def unicodeToHex(buf: String) : String = {
        buf.flatMap{ ch => ch.toInt.toHexString }
    }

    def unicodePrint(strToPrint: String) : Unit = {
        val utf8: Charset  = Charset.forName("UTF-8")
        val message: String = new String(strToPrint.getBytes("UTF-8"),
                                         Charset.defaultCharset().name())

        val printStream: PrintStream = new PrintStream(System.out, true, utf8.name())
        printStream.println(message) // should print your Character
    }

    // Marshal an arbitrary WDL value, such as a ragged array,
    // into a scala string.
    //
    // We encode as base64, to remove special characters. This allows
    // embedding the resulting string as a field in a JSON document.
    def marshal(v: WomValue) : String = {
        val js = JsArray(
            JsString(v.womType.toDisplayString),
            JsString(v.toWomString)
        )
        base64Encode(js.compactPrint)
    }

    // reverse of [marshal]
    def unmarshal(buf64 : String) : WomValue = {
        val buf = base64Decode(buf64)
        buf.parseJson match {
            case JsArray(vec) if (vec.length == 2) =>
                (vec(0), vec(1)) match {
                    case (JsString(wTypeStr), JsString(wValueStr)) =>
                        val t : WomType = WdlFlavoredWomType.fromDisplayString(wTypeStr)
                        try {
                            WdlFlavoredWomType.FromString(t).fromWorkflowSource(wValueStr)
                        } catch {
                            case e: Throwable =>
                                t match {
                                    case WomOptionalType(t1) =>
                                        System.err.println(s"Error unmarshalling wdlType=${t.toDisplayString}")
                                        System.err.println(s"Value=${wValueStr}")
                                        System.err.println(s"Trying again with type=${t1.toDisplayString}")
                                        WdlFlavoredWomType.FromString(t1).fromWorkflowSource(wValueStr)
                                    case _ =>
                                        throw e
                                }
                        }
                    case _ => throw new AppInternalException(s"JSON vector should have two strings ${buf}")
                }
            case _ => throw new AppInternalException(s"Error unmarshalling json value ${buf}")
        }
    }

    // Job input, output,  error, and info files are located relative to the home
    // directory
    def jobFilesOfHomeDir(homeDir : Path) : (Path, Path, Path, Path) = {
        val jobInputPath = homeDir.resolve("job_input.json")
        val jobOutputPath = homeDir.resolve("job_output.json")
        val jobErrorPath = homeDir.resolve("job_error.json")
        val jobInfoPath = homeDir.resolve("dnanexus-executable.json")
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
    def execCommand(cmdLine : String, timeout: Option[Int]) : (String, String) = {
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

    // download a file from the platform to a path on the local disk.
    //
    // Note: this function assumes that the target path does not exist yet
    def downloadFile(path: Path, dxfile: DXFile) : Unit = {
        def downloadOneFile(path: Path, dxfile: DXFile, counter: Int) : Boolean = {
            val fid = dxfile.getId()
            try {
                // Use dx download
                val dxDownloadCmd = s"dx download ${fid} -o ${path.toString()}"
                System.err.println(s"--  ${dxDownloadCmd}")
                val (outmsg, errmsg) = execCommand(dxDownloadCmd, None)

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

    // Upload a local file to the platform, and return a json link
    def uploadFile(path: Path) : JsValue = {
        if (!Files.exists(path))
            throw new AppInternalException(s"Output file ${path.toString} is missing")
        def uploadOneFile(path: Path, counter: Int) : Option[String] = {
            try {
                // shell out to dx upload
                val dxUploadCmd = s"dx upload ${path.toString} --brief"
                System.err.println(s"--  ${dxUploadCmd}")
                val (outmsg, errmsg) = execCommand(dxUploadCmd, None)
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
                    val v = s"""{ "$$dnanexus_link": "${fid}" }"""
                    return v.parseJson
                case None => ()
            }
            counter = counter + 1
        }
        throw new Exception(s"Failure to upload file ${path}")
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

    def makeOptional(t: WomType) : WomType = {
        t match {
            // If the type is already optional, don't make it
            // double optional.
            case WomOptionalType(_) => t
            case _ => WomOptionalType(t)
        }
    }

    def stripArray(t: WomType) : WomType = {
        t match {
            case WomArrayType(x) => x
            case _ => throw new Exception(s"WDL type $t is not an array")
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

    private def genNSpaces(n: Int) = s"${" " * n}"

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

    // This code is copied the lookupType method in the WdlNamespace trait
    //   https://github.com/broadinstitute/wdl4s/blob/develop/wom/src/main/scala/wdl/WdlNamespace.scala
    //
    // We should ask to make it public.
    //
    def lookupType(from: Scope)(n: String): WomType = {
        val resolved: Option[WdlGraphNode] = from.resolveVariable(n)
        resolved match {
            case Some(d: DeclarationInterface) => d.relativeWdlType(from)
            case Some(c: WdlCall) => WdlCallOutputsObjectType(c)
            case Some(s: Scatter) => s.collection.evaluateType(lookupType(s),
                                                               new WdlStandardLibraryFunctionsType,
                                                               Option(from)) match {
                case Success(WomArrayType(aType)) => aType
                // We don't need to check for a WOM map type, because
                // of the custom unapply in object WomArrayType
                case _ => throw new Exception(s"Variable $n references a scatter block ${s.fullyQualifiedName}, but the collection does not evaluate to an array")
            }
            case Some(_: WdlNamespace) => WdlNamespaceType
            case _ => throw new Exception(s"Could not resolve $n from scope ${from.fullyQualifiedName}")
        }
    }

    // Figure out the type of an expression
    def evalType(expr: WomExpression,
                 parent: Scope,
                 cef: CompilerErrorFormatter,
                 verbose: Verbose) : WomType = {
        TypeEvaluator(lookupType(parent),
                      new WdlStandardLibraryFunctionsType,
                      Some(parent)).evaluate(expr.ast) match {
            case Success(wdlType) => wdlType
            case Failure(f) =>
                warning(verbose, cef.couldNotEvaluateType(expr))
                throw f
        }
    }

    // Here, we use the flat namespace assumption. We use
    // unqualified names as Fully-Qualified-Names, because
    // task and workflow names are unique.
    def calleeGetName(call: WdlCall) : String = {
        call match {
            case tc: WdlTaskCall =>
                tc.task.unqualifiedName
            case wfc: WdlWorkflowCall =>
                wfc.calledWorkflow.unqualifiedName
        }
    }
}
