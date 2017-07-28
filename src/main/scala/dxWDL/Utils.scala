package dxWDL

import com.dnanexus.{DXApplet, DXAPI, DXFile, DXProject, DXJSON, DXUtil, DXDataObject}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import java.nio.charset.StandardCharsets
import java.nio.file.{Path, Paths, Files}
import java.util.Base64
import scala.collection.JavaConverters._
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.sys.process._
import scala.util.{Failure, Success, Try}
import spray.json._
import spray.json.DefaultJsonProtocol
import wdl4s.AstTools
import wdl4s.AstTools.EnhancedAstNode
import wdl4s.{Call, Declaration, Scatter, Scope, Task, WdlExpression, WdlNamespaceWithWorkflow,
    WdlNamespace, WdlSource, Workflow}
import wdl4s.parser.WdlParser.{Ast, AstNode, Terminal}
import wdl4s.types._
import wdl4s.values._

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

    val CHECKSUM_PROP = "dxWDL_checksum"
    val COMMON = "common"
    val DOWNLOAD_RETRY_LIMIT = 3
    val DX_HOME = "/home/dnanexus"
    val DX_INSTANCE_TYPE_ATTR = "dx_instance_type"
    val FLAT_FILES_SUFFIX = "___dxfiles"
    val IF = "if"
    val INSTANCE_TYPE_DB_FILENAME = "instanceTypeDB.json"
    val INTERMEDIATE_RESULTS_FOLDER = "intermediate"
    val LINK_INFO_FILENAME = "linking.json"
    val MAX_HOURLY_RATE = 10.0
    val MAX_STRING_LEN = 8 * 1024     // Long strings cause problems with bash and the UI
    val MAX_NUM_FILES_MOVE_LIMIT = 1000
    val OUTPUT_SECTION = "outputs"
    val SCATTER = "scatter"
    val TMP_VAR_NAME_PREFIX = "xtmp"
    val UPLOAD_RETRY_LIMIT = DOWNLOAD_RETRY_LIMIT
    val UNIVERSAL_FILE_PREFIX = "dx://"
    val WDL_SNIPPET_FILENAME = "source.wdl"

    // Substrings used by the compiler for encoding purposes
    val reservedSubstrings = List("___")

    // Prefixes used for generated applets
    val reservedAppletPrefixes = List(SCATTER, COMMON)

    var tmpVarCnt = 0
    def genTmpVarName() : String = {
        val tmpVarName: String = s"${TMP_VAR_NAME_PREFIX}${tmpVarCnt}"
        tmpVarCnt = tmpVarCnt + 1
        tmpVarName
    }


    def isGeneratedVar(varName: String) : Boolean = {
        varName.startsWith(TMP_VAR_NAME_PREFIX)
    }

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

    // where script files are placed and generated
    def getMetaDirPath() : Path = {
        val p = execDirPath.resolve("meta")
        p
    }

    // Used to convert into the JSON datatype used by dxjava
    val objMapper : ObjectMapper = new ObjectMapper()

    // Load the information from:
    //   ${HOME}/dnanexus-executable.json
    //
    // We specifically need the help strings, which map each variable to
    // its WDL type. The specification looks like this:
    // [
    //   {"help": "Int", "name": "bi", "class": "int"}, ...
    // ]
    def loadExecInfo(jobInfo: String) : (Map[String, Option[WdlType]], Map[String, Option[WdlType]]) = {
        val info: JsValue = jobInfo.parseJson
        val inputSpec: Vector[JsValue] = info.asJsObject.fields.get("inputSpec") match {
            case None => Vector()
            case Some(JsArray(x)) => x
            case Some(_) => throw new AppInternalException("Bad format for exec information")
        }
        val outputSpec: Vector[JsValue] = info.asJsObject.fields.get("outputSpec") match {
            case None => Vector()
            case Some(JsArray(x)) => x
            case Some(_) => throw new AppInternalException("Bad format for exec information")
        }
        def getJsString(js: JsValue) = {
            js match {
                case JsString(s) => s
                case _ => throw new AppInternalException("Bad format for exec information: getJsString")
            }
        }
        def wdlTypeOfVar(varDef: JsValue) : (String, Option[WdlType]) = {
            val v = varDef.asJsObject
            val name = getJsString(v.fields("name"))
            v.fields.get("help") match {
                case None => name -> None
                case Some(x) =>
                    val helpStr = getJsString(x)
                    val wType : WdlType = WdlType.fromWdlString(helpStr)
                    name -> Some(wType)
            }
        }

        (inputSpec.map(wdlTypeOfVar).toMap,
         outputSpec.map(wdlTypeOfVar).toMap)
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

    def taskOfCall(call : Call) : Task = {
        call.callable match {
            case task: Task => task
            case workflow: Workflow =>
                throw new AppInternalException(s"Workflows are not support in calls ${call.callable}")
        }
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
            case JsArray(vec) =>
                if (vec.length != 2)
                    throw new AppInternalException(s"JSON vector should have two elements ${buf}");
                (vec(0), vec(1)) match {
                    case (JsString(wTypeStr), JsString(wValueStr)) =>
                        val wType : WdlType = WdlType.fromWdlString(wTypeStr)
                        wType.fromWdlString(wValueStr)
                    case _ => throw new AppInternalException("JSON vector should have two strings ${buf}")
                }
            case _ => throw new AppInternalException("Error unmarshalling json value ${buf}")
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
        val processBuilder = new java.lang.ProcessBuilder()
        val cmds = Seq("/bin/sh", "-c", cmdLine)

        val outStream = new StringBuilder()
        val errStream = new StringBuilder()
        val logger = ProcessLogger(
            (o: String) => { outStream.append(o ++ "\n") },
            (e: String) => { errStream.append(e ++ "\n") }
        )

        val p : Process = Process(cmds).run(logger, false)
        val retcode = timeout match {
            case None =>
                // blocks, and returns the exit code. Does NOT connect
                // the standard in of the child job to the parent
                val retcode = p.exitValue()
                if (retcode != 0) {
                    System.err.println(s"STDOUT: ${outStream.toString()}")
                    System.err.println(s"STDERR: ${errStream.toString()}")
                    throw new Exception(s"Error running command ${cmdLine}")
                }
                retcode
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


    // download platform file contents directly into an in-memory string
    def downloadString(dxfile: DXFile) : String = {
        val bytes = dxfile.downloadBytes()
        new String(bytes, StandardCharsets.UTF_8)
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

    def uploadString(buf: String, fileNameDbg: String) : JsValue = {
        val dxfile = DXFile.newFile().setName(fileNameDbg).build()
        dxfile.upload(buf.getBytes())
        dxfile.closeAndWait()

        // return a dx-link
        val fid = dxfile.getId()
        val v = s"""{ "$$dnanexus_link": "${fid}" }"""
        v.parseJson
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

    // types
    def isOptional(t: WdlType) : Boolean = {
        t match {
            case WdlOptionalType(_) => true
            case t => false
        }
    }

    def stripOptional(t: WdlType) : WdlType = {
        t match {
            case WdlOptionalType(x) => x
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

    def inputsToString(m: Map[String, WdlValue]) : String = {
        m.map{ case(key, wVal) =>
            key ++ " -> " ++ wVal.wdlType.toString ++ "(" ++ wVal.toWdlString ++ ")"
        }.mkString("\n")
    }

    // Used by the compiler to provide more information to the user
    def trace(verbose: Boolean, msg: String) : Unit = {
        if (!verbose)
            return
        System.err.println(msg)
    }
}
