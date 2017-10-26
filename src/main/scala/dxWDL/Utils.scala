package dxWDL

import com.dnanexus.{DXApplet, DXAPI, DXEnvironment, DXFile, DXJob, DXJSON, DXProject,
    IOClass, InputParameter, OutputParameter}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import java.nio.charset.StandardCharsets
import java.nio.file.{Path, Paths, Files}
import java.util.Base64
import scala.collection.JavaConverters._
import scala.concurrent._
import scala.collection.mutable.HashMap
import scala.util.{Failure, Success}
import ExecutionContext.Implicits.global
import scala.sys.process._
import spray.json._
import wdl4s.wdl._
import wdl4s.wdl.expression._
import wdl4s.wdl.types._
import wdl4s.wdl.values._


// Exception used for AppInternError
class AppInternalException private(ex: RuntimeException) extends RuntimeException(ex) {
    def this(message:String) = this(new RuntimeException(message))
}

// Exception used for AppError
class AppException private(ex: RuntimeException) extends RuntimeException(ex) {
    def this(message:String) = this(new RuntimeException(message))
}

class UnboundVariableException private(ex: RuntimeException) extends RuntimeException(ex) {
    def this(varName: String) = this(new RuntimeException(s"Variable ${varName} is unbound"))
}

object Utils {
    // Information used to link applets that call other applets. For example, a scatter
    // applet calls applets that implement tasks.
    case class AppletLinkInfo(inputs: Map[String, WdlType], dxApplet: DXApplet)

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

    // Encapsulation of verbosity flags.
    //  on --       is the overall setting true/false
    //  keywords -- specific words to trace
    case class Verbose(on: Boolean,
                       keywords: Set[String])

    // Topological sort mode of operation
    object TopoMode extends Enumeration {
        val Check, Sort, SortRelaxed = Value
    }

    object AppletLinkInfo {
        def writeJson(ali: AppletLinkInfo) : JsValue = {
            // Serialize applet input definitions, so they could be used
            // at runtime.
            val appInputDefs: Map[String, JsString] = ali.inputs.map{
                case (name, wdlType) => name -> JsString(wdlType.toWdlString)
            }.toMap
            JsObject(
                "id" -> JsString(ali.dxApplet.getId()),
                "inputs" -> JsObject(appInputDefs)
            )
        }

        def readJson(aplInfo: JsValue, dxProject: DXProject) = {
            val dxApplet = aplInfo.asJsObject.fields("id") match {
                case JsString(appletId) => DXApplet.getInstance(appletId, dxProject)
                case _ => throw new Exception("Bad JSON")
            }
            val inputDefs = aplInfo.asJsObject.fields("inputs").asJsObject.fields.map{
                case (key, JsString(wdlTypeStr)) => key -> WdlType.fromWdlString(wdlTypeStr)
                case _ => throw new Exception("Bad JSON")
            }.toMap
            AppletLinkInfo(inputDefs, dxApplet)
        }
    }

    val APPLET_LOG_MSG_LIMIT = 1000
    val CHECKSUM_PROP = "dxWDL_checksum"
    val COMMON = "common"
    val DEFAULT_APPLET_TIMEOUT = 48
    val DOWNLOAD_RETRY_LIMIT = 3
    val DX_HOME = "/home/dnanexus"
    val DX_INSTANCE_TYPE_ATTR = "dx_instance_type"
    val FLAT_FILES_SUFFIX = "___dxfiles"
    val IF = "if"
    val INSTANCE_TYPE_DB_FILENAME = "instanceTypeDB.json"
    val INTERMEDIATE_RESULTS_FOLDER = "intermediate"
    val LINK_INFO_FILENAME = "linking.json"
    val MAX_STRING_LEN = 8 * 1024     // Long strings cause problems with bash and the UI
    val MAX_NUM_FILES_MOVE_LIMIT = 1000
    val OUTPUT_SECTION = "outputs"
    val SCATTER = "scatter"
    val TMP_VAR_NAME_PREFIX = "xtmp"
    val UPLOAD_RETRY_LIMIT = DOWNLOAD_RETRY_LIMIT
    val UNIVERSAL_FILE_PREFIX = "dx://"

    lazy val dxEnv = DXEnvironment.create()

    // Lookup cache for projects. This saves
    // repeated searches for projects we already found.
    val projectDict = HashMap.empty[String, DXProject]

    // Substrings used by the compiler for encoding purposes
    val reservedSubstrings = List("___")

    // Prefixes used for generated applets
    val reservedAppletPrefixes = List(SCATTER, IF)

    lazy val execDirPath : Path = {
        val currentDir = System.getProperty("user.dir")
        val p = Paths.get(currentDir, "execution")
        Utils.safeMkdir(p)
        p
    }
    lazy val tmpDirPath : Path = {
        val p = Paths.get("/tmp/dxWDL_Runner/job_scratch_space")
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

    var tmpVarCnt = 0
    def genTmpVarName() : String = {
        val tmpVarName: String = s"${TMP_VAR_NAME_PREFIX}${tmpVarCnt}"
        tmpVarCnt = tmpVarCnt + 1
        tmpVarName
    }


    def isGeneratedVar(varName: String) : Boolean = {
        varName.startsWith(TMP_VAR_NAME_PREFIX)
    }

    // Is this a WDL type that maps to a native DX type?
    def isNativeDxType(wdlType: WdlType) : Boolean = {
        wdlType match {
            case WdlBooleanType | WdlIntegerType | WdlFloatType | WdlStringType | WdlFileType
                   | WdlArrayType(WdlBooleanType)
                   | WdlArrayType(WdlIntegerType)
                   | WdlArrayType(WdlFloatType)
                   | WdlArrayType(WdlStringType)
                   | WdlArrayType(WdlFileType) => true
            case WdlOptionalType(t) => isNativeDxType(t)
            case _ => false
        }
    }

    // Is a declaration of a task/workflow an input for the
    // compiled dx:applet/dx:workflow ?
    //
    // Examples:
    //   File x
    //   String y = "abc"
    //   Float pi = 3 + .14
    //   Int? z = 3
    //
    // x - must be provided as an applet input
    // y, pi -- calculated, non inputs
    // z - is an input with a default value
    def declarationIsInput(decl: Declaration) : Boolean = {
        (decl.expression, decl.wdlType) match {
            case (None,_) => true
            case (Some(_), WdlOptionalType(_)) => true
            case (_,_) => false
        }
    }

    // where script files are placed and generated
    def getMetaDirPath() : Path = {
        val p = execDirPath.resolve("meta")
        p
    }

    // Used to convert into the JSON datatype used by dxjava
    val objMapper : ObjectMapper = new ObjectMapper()

    // Get the dx:classes for inputs and outputs
    def loadExecInfo : (Map[String, IOClass], Map[String, IOClass]) = {
        val dxapp : DXApplet = dxEnv.getJob().describe().getApplet()
        val desc : DXApplet.Describe = dxapp.describe()
        val inputSpecRaw: List[InputParameter] = desc.getInputSpecification().asScala.toList
        val inputSpec:Map[String, IOClass] = inputSpecRaw.map(
            iSpec => iSpec.getName -> iSpec.getIOClass
        ).toMap
        val outputSpecRaw: List[OutputParameter] = desc.getOutputSpecification().asScala.toList
        val outputSpec:Map[String, IOClass] = outputSpecRaw.map(
            iSpec => iSpec.getName -> iSpec.getIOClass
        ).toMap

        // remove auxiliary fields
        (inputSpec.filter{ case (fieldName,_) => !fieldName.endsWith(FLAT_FILES_SUFFIX) },
         outputSpec.filter{ case (fieldName,_) => !fieldName.endsWith(FLAT_FILES_SUFFIX) })
    }

    def lookupProject(projName: String): DXProject = {
        if (projName.startsWith("project-")) {
            // A project ID
            DXProject.getInstance(projName)
        } else {
            if (projectDict contains projName) {
                //System.err.println(s"Cached project ${projName}")
                return projectDict(projName)
            }

            // A project name, resolve it
            val req: ObjectNode = DXJSON.getObjectBuilder()
                .put("name", projName)
                .put("limit", 2)
                .build()
            val rep = DXAPI.systemFindProjects(req, classOf[JsonNode])
            val repJs:JsValue = Utils.jsValueOfJsonNode(rep)

            val results = repJs.asJsObject.fields.get("results") match {
                case Some(JsArray(x)) => x
                case _ => throw new Exception(
                    s"Bad response from systemFindProject API call (${repJs.prettyPrint}), when resolving project ${projName}.")
            }
            if (results.length > 1)
                throw new Exception(s"Found more than one project named ${projName}")
            if (results.length == 0)
                throw new Exception(s"Project ${projName} not found")
            val dxProject = results(0).asJsObject.fields.get("id") match {
                case Some(JsString(id)) => DXProject.getInstance(id)
                case _ => throw new Exception(s"Bad response from SystemFindProject API call ${repJs.prettyPrint}")
            }
            projectDict(projName) = dxProject
            dxProject
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
    def jsonNodeOfJsValue(jsValue : JsValue) : JsonNode = {
        val s : String = jsValue.prettyPrint
        objMapper.readTree(s)
    }

    // Convert from jackson JsonNode to spray-json
    def jsValueOfJsonNode(jsNode : JsonNode) : JsValue = {
        jsNode.toString().parseJson
    }

    // Create a dx link to a field in a job.
    def makeJBOR(jobId : String, fieldName :  String) : JsValue = {
        val oNode : ObjectNode =
            DXJSON.getObjectBuilder().put("job", jobId).put("field", fieldName).build()
        // convert from ObjectNode to JsValue
        oNode.toString().parseJson
    }

    def runSubJob(entryPoint:String,
                  instanceType:Option[String],
                  inputs:JsValue,
                  dependsOn: Vector[DXJob]) : DXJob = {
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
                val jobIds = dependsOn.map{ dxJob => JsString(dxJob.getId) }.toVector
                Map("dependsOn" -> JsArray(jobIds))
            }
        val req = JsObject(fields ++ instanceFields ++ dependsFields)
        System.err.println(s"subjob request=${req.prettyPrint}")
        val retval: JsonNode = DXAPI.jobNew(jsonNodeOfJsValue(req), classOf[JsonNode])
        val info: JsValue =  Utils.jsValueOfJsonNode(retval)
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

    // Dots are illegal in applet variable names. For example,
    // "Add.sum" must be encoded. As a TEMPORARY hack, we convert
    // dots into "___".
    def encodeAppletVarName(varName : String) : String = {
        // Make sure the variable does not already have the "___"
        // sequence.
        reservedSubstrings.foreach{ s =>
            if (varName contains s)
                throw new Exception(s"Variable ${varName} includes the reserved substring ${s}")
        }
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

    def taskOfCall(call : WdlCall) : WdlTask = {
        call.callable match {
            case task: WdlTask => task
            case workflow: WdlWorkflow =>
                throw new AppInternalException(s"Workflows are not support in calls ${call.callable}")
        }
    }

    def callUniqueName(call : WdlCall) : String = {
        val retval = call.alias match {
            case Some(x) => x
            case None => Utils.taskOfCall(call).name
        }
        assert(retval == call.unqualifiedName)
        retval
    }


    def base64Encode(buf: String) : String = {
        Base64.getEncoder.encodeToString(buf.getBytes(StandardCharsets.UTF_8))
    }

    def base64Decode(buf64: String) : String = {
        val ba : Array[Byte] = Base64.getDecoder.decode(buf64.getBytes(StandardCharsets.UTF_8))
        ba.map(x => x.toChar).mkString
    }

    // Marshal an arbitrary WDL value, such as a ragged array,
    // into a scala string.
    //
    // We encode as base64, to remove special characters. This allows
    // embedding the resulting string as a field in a JSON document.
    def marshal(v: WdlValue) : String = {
        val js = JsArray(
            JsString(v.wdlType.toWdlString),
            JsString(v.toWdlString)
        )
        base64Encode(js.compactPrint)
    }

    // reverse of [marshal]
    def unmarshal(buf64 : String) : WdlValue = {
        val buf = base64Decode(buf64)
        buf.parseJson match {
            case JsArray(vec) if (vec.length == 2) =>
                (vec(0), vec(1)) match {
                    case (JsString(wTypeStr), JsString(wValueStr)) =>
                        val wType : WdlType = WdlType.fromWdlString(wTypeStr)
                        try {
                            wType.fromWdlString(wValueStr)
                        } catch {
                            case e: Throwable =>
                                wType match {
                                    case WdlOptionalType(t) =>
                                        System.err.println(s"Error unmarshalling wdlType=${wType.toWdlString}")
                                        System.err.println(s"Value=${wValueStr}")
                                        System.err.println(s"Trying again with type=${t.toWdlString}")
                                        t.fromWdlString(wValueStr)
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

    // In a block, split off the beginning declarations, from the rest.
    // For example, the scatter block below, will be split into
    // the top two declarations, and the other calls.
    // scatter (unmapped_bam in flowcell_unmapped_bams) {
    //    String sub_strip_path = "gs://.*/"
    //    String sub_strip_unmapped = unmapped_bam_suffix + "$"
    //    call SamToFastqAndBwaMem {..}
    //    call MergeBamAlignment {..}
    // }
    def splitBlockDeclarations(children: List[Scope]) :
            (List[Declaration], List[Scope]) = {
        def collect(topDecls: List[Declaration],
                    rest: List[Scope]) : (List[Declaration], List[Scope]) = {
            rest match {
                case hd::tl =>
                    hd match {
                        case decl: Declaration =>
                            collect(decl :: topDecls, tl)
                        // Next element is not a declaration
                        case _ => (topDecls, rest)
                    }
                // Got to the end of the children list
                case Nil => (topDecls, rest)
            }
        }

        val (decls, rest) = collect(Nil, children)
        (decls.reverse, rest)
    }


    // describe a project, and extract fields that not currently available
    // through dxjava.
    def projectDescribeExtraInfo(dxProject: DXProject) : (String,String) = {
        val rep = DXAPI.projectDescribe(dxProject.getId(), classOf[JsonNode])
        val jso:JsObject = Utils.jsValueOfJsonNode(rep).asJsObject

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


    // download a file from the platform to a path on the local disk.
    //
    // Note: this function assumes that the target path does not exist yet
    def downloadFile(path: Path, dxfile: DXFile) = {
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
    def dxFileOfJsValue(jsValue : JsValue) : DXFile = {
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

    // types
    def isOptional(t: WdlType) : Boolean = {
        t match {
            case WdlOptionalType(_) => true
            case t => false
        }
    }

    // We need to deal with types like:
    //     Int??, Array[File]??
    def stripOptional(t: WdlType) : WdlType = {
        t match {
            case WdlOptionalType(x) => stripOptional(x)
            case x => x
        }
    }

    def stripArray(t: WdlType) : WdlType = {
        t match {
            case WdlArrayType(x) => x
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
    def appletLog(msg:String) : Unit = {
        val shortMsg =
            if (msg.length > APPLET_LOG_MSG_LIMIT)
                "Message is too long for logging"
            else
                msg
        System.err.println(shortMsg)
    }

    // Used by the compiler to provide more information to the user
    def trace(verbose: Boolean, msg: String) : Unit = {
        if (!verbose)
            return
        System.err.println(msg)
    }

    // coerce a WDL value to the required type (if needed)
    def cast(wdlType: WdlType, v: WdlValue, varName: String) : WdlValue = {
        val retVal =
            if (v.wdlType != wdlType) {
                // we need to convert types
                System.err.println(s"Casting variable ${varName} from ${v.wdlType} to ${wdlType}")
                wdlType.coerceRawValue(v).get
            } else {
                // no need to change types
                v
            }
        assert(retVal.wdlType == wdlType)
        retVal
    }

    // This code is based on the lookupType method in the WdlNamespace trait
    //   https://github.com/broadinstitute/wdl4s/blob/develop/wom/src/main/scala/wdl/WdlNamespace.scala
    //
    // It handles the case where the scatter collection is not an
    // array.
    def lookupType(from: Scope)(n: String): WdlType = {
        val resolved:Option[WdlGraphNode] = from.resolveVariable(n)
        System.err.println(s"resolved=${resolved}")
        val wdlType = resolved match {
            case Some(d: DeclarationInterface) => d.relativeWdlType(from)
            case Some(c: WdlCall) => WdlCallOutputsObjectType(c)
            case Some(s: Scatter) => s.collection.evaluateType(lookupType(s),
                                                               new WdlStandardLibraryFunctionsType,
                                                               Option(from)) match {
                case Success(a: WdlArrayType) =>
                    // Collection is an array
                    a.memberType
                case Success(WdlMapType(kType,vType)) =>
                    // Collection is a map
                    WdlPairType(kType, vType)
                case Success(other) =>
                    throw new Exception(
                        s"""|Variable $n references a scatter block ${s.fullyQualifiedName},
                            |but the collection evaluates to ${other.toWdlString}"""
                            .stripMargin.replaceAll("\n", " "))
                case Failure(f) =>
                    throw f
            }
            case Some(_: WdlNamespace) => WdlNamespaceType

            case _ =>
                throw new Exception(s"Could not resolve $n from scope ${from.fullyQualifiedName}")
        }
        System.err.println(s"lookupType(${n}) = ${wdlType.toWdlString}")
        wdlType
    }
}
