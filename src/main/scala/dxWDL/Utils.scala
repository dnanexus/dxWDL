package dxWDL

import com.dnanexus.{DXWorkflow, DXApplet, DXFile, DXProject, DXJSON, DXUtil, DXContainer, DXSearch, DXDataObject, InputParameter}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import java.nio.charset.StandardCharsets
import java.nio.file.{Path, Paths, Files}
import java.util.Base64
import org.apache.commons.io.IOUtils
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.sys.process._
import scala.util.{Failure, Success, Try}
import spray.json._
import spray.json.DefaultJsonProtocol
import spray.json.JsString
import wdl4s.AstTools
import wdl4s.AstTools.EnhancedAstNode
import wdl4s.{Call, Declaration, Scatter, Scope, Task, WdlExpression, WdlNamespaceWithWorkflow,
    WdlNamespace, WdlSource, Workflow}
import wdl4s.parser.WdlParser.{Ast, AstNode, Terminal}
import wdl4s.types._
import wdl4s.values._
import wdl4s.expression.WdlStandardLibraryFunctionsType

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
    val VERSION = "0.12"

    // Long strings cause problems with bash and the UI
    val MAX_STRING_LEN = 8 * 1024
    val FLAT_FILE_ARRAY_SUFFIX = "_ffa"
    val DX_HOME = "/home/dnanexus"
    val DOWNLOAD_RETRY_LIMIT = 3
    val UPLOAD_RETRY_LIMIT = DOWNLOAD_RETRY_LIMIT
    val DXPY_FILE_TRANSFER = true

    // Substrings used by the compiler for encoding purposes
    val reservedSuffixes = List(FLAT_FILE_ARRAY_SUFFIX)
    val reservedSubstrings = List("___")

    // Prefixes used for generated applets
    val COMMON = "common"
    val SCATTER = "scatter"
    val reservedAppletPrefixes = List(SCATTER, COMMON)

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
    def loadExecInfo(jobInfo: String) : Map[String, Option[WdlType]] = {
        val info: JsValue = jobInfo.parseJson
        val inputSpec: Vector[JsValue] = info.asJsObject.fields.get("inputSpec") match {
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
        inputSpec.map{ case varDef =>
            val v = varDef.asJsObject
            val name = getJsString(v.fields("name"))
            v.fields.get("help") match {
                case None => name -> None
                case Some(x) =>
                    val helpStr = getJsString(x)
                    val wType : WdlType = WdlType.fromWdlString(helpStr)
                    name -> Some(wType)
            }
        }.toMap
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
        reservedSuffixes.foreach{ s =>
            if (varName.endsWith(s))
                throw new Exception(s"Variable ${varName} ends with reserved suffix ${s}")
        }
        varName.replaceAll("\\.", "___")
    }

    // Dots are illegal in applet variable names, so they
    // are encoded as three underscores "___". Decode
    // these occurrences back into dots.
    def decodeAppletVarName(varName : String) : String = {
        if (varName contains "\\.")

            throw new Exception(s"Variable ${varName} includes the illegal symbol \\.")
        varName.replaceAll("___", "\\.")
    }

    def appletVarNameSplit(varName : String) : (String,String) = {
        reservedSuffixes.foreach(suff =>
            if (varName.endsWith(suff)) {
                val prefix = varName.substring(0, varName.indexOf(suff))
                return (decodeAppletVarName(prefix), suff)
            }
        )
        (decodeAppletVarName(varName), "")
    }

    def appletVarNameStripSuffix(varName : String) : String = {
        val (prefix,_) = appletVarNameSplit(varName)
        prefix
    }

    def appletVarNameGetSuffix(varName : String) : String= {
        val (_,suffix) = appletVarNameSplit(varName)
        suffix
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

    // Figure out the expression type for a collection we loop over in a scatter
    //
    // Expressions like A.B.C are converted to A___B___C, in order to avoid
    // the wdl4s library from interpreting these as member accesses. The environment
    // passed into the method [env] has types for all these variables, as fully qualified names.
    def calcIterWdlType(scatter : Scatter, env : Map[String,WdlType]) : WdlType = {
        def lookup(varName : String) : WdlType = {
            val v = varName.replaceAll("___", "\\.")
            env.get(v) match {
                case Some(x) => x
                case None => throw new Exception(s"No type found for variable ${varName}")
            }
        }

        // convert all sub-expressions of the form A.B.C to A___B___C
        val s : String = scatter.collection.toWdlString.replaceAll("\\.", "___")
        val collection : WdlExpression = WdlExpression.fromString(s)

        val collectionType : WdlType = collection.evaluateType(lookup, new WdlStandardLibraryFunctionsType) match {
            case Success(wdlType) => wdlType
            case _ => throw new Exception(s"Could not evaluate the WdlType for ${collection.toWdlString}")
        }

        // remove the array qualifier
        collectionType match {
            case WdlArrayType(x) => x
            case _ => throw new Exception(s"type ${collectionType} is not an array")
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

    // Report an error, since this is called from a bash script, we
    // can't simply raise an exception. Instead, we write the error to
    // a standard JSON file.
    def writeJobErrorAndExit(jobErrorPath : Path, e: Throwable) : Unit = {
        val errType = e match {
            case _ : AppException => "AppError"
            case _ : AppInternalException => "AppInternalError"
            case _ : Throwable => "AppInternalError"
        }
        val message = exceptionToString(e)
        val errorReport =
            s"""|{
                |  "error": {
                |    "type": $errType,
                |    "message": $message
                |  }
                |}""".stripMargin

        // For some reason, sometimes the platform does not
        // report this error properly. This line prints it
        // to stderr as well.
        System.err.println(errorReport)
        writeFileContent(jobErrorPath, errorReport)
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


    // Return true, only if the expression can be evaluated
    // based on toplevel variables. If so,
    // the variables this expression depends on.
    def findToplevelVarDeps(expr: WdlExpression) : (Boolean, List[String]) = {
        if (!expr.prerequisiteCallNames.isEmpty)
            return (false, Nil)
        if (!expr.ast.findTopLevelMemberAccesses().isEmpty)
            return (false, Nil)
        val variables = AstTools.findVariableReferences(expr.ast).map{ case t:Terminal =>
            WdlExpression.toString(t)
        }
        System.err.println(s"findToplevelVarDeps  ${expr.toWdlString}  => ${variables}")
        (true, variables.toList)
    }

    // Lift declarations that can be evaluated at the top of the block
    //
    // 1) build an environment from [topDecls]
    // 2) for each prospective declaration, check if depends only on the available
    //    variables. Pull up all such statements.
    def liftDeclarations(topDeclarations: List[Declaration], body: List[Scope])
            : (List[Declaration], List[Scope]) = {
        // build an environment from [topDecls]
        var env : Set[String] = topDeclarations.map{ decl => decl.unqualifiedName }.toSet
        var lifted : List[Declaration] = Nil

        val calls : Seq[Option[Scope]] = body.map {
            case call: Call => Some(call)
            case decl: Declaration =>
                val (possible, deps) = Utils.findToplevelVarDeps(decl.expression.get)
                if (!possible) {
                    Some(decl)
                } else if (!deps.forall(x => env(x))) {
                    // some dependency is missing, can't lift to top level
                    Some(decl)
                } else {
                    env = env + decl.unqualifiedName
                    lifted = decl :: lifted
                    None
                }
            case ssc: Scatter => Some(ssc)
            case swf: Workflow => Some(swf)
        }

        (lifted.reverse, calls.flatten.toList)
    }


    // Run a child process and collect stdout and stderr into strings
    def execCommand(cmdLine : String) : (String, String) = {
        val processBuilder = new java.lang.ProcessBuilder()
        val cmds = Seq("/bin/sh", "-c", cmdLine)

        val outStream = new StringBuilder()
        val errStream = new StringBuilder()
        val logger = ProcessLogger(
            (o: String) => {
                outStream.append(o ++ "\n")
            },
            (e: String) => {
                errStream.append(e ++ "\n")
            }
        )

        // blocks, and returns the exit code. Does NOT connect the standard in of the child job
        // to the parent
        val p : Process = Process(cmds).run(logger, false)
        val retcode = p.exitValue()
        if (retcode != 0)
            throw new Exception(s"Error running command ${cmdLine}")
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
                if (DXPY_FILE_TRANSFER) {
                    // Use dx download
                    val dxDownloadCmd = s"dx download ${fid} -o ${path.toString()}"
                    println(s"--  ${dxDownloadCmd}")
                    val (outmsg, errmsg) = execCommand(dxDownloadCmd)
                } else {
                    // downloading with java
                    val fos = new java.io.FileOutputStream(path.toString())
                    IOUtils.copy(dxfile.getDownloadStream(), fos)
                }
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
            println(s"downloading file ${path.toString} (try=${counter})")
            rc = downloadOneFile(path, dxfile, counter)
            counter = counter + 1
        }
        if (!rc)
            throw new Exception(s"Failure to download file ${path}")
    }

    def uploadString(buf: String) : JsValue = {
        val dxfile = DXFile.newFile().build()
        dxfile.upload(buf.getBytes())
        dxfile.close()

        // return a dx-link
        val fid = dxfile.getId()
        val v = s"""{ "$$dnanexus_link": "${fid}" }"""
        v.parseJson
    }

    // Upload a local file to the platform, and return a json link
    def uploadFile(path: Path) : JsValue = {
        if (!Files.exists(path))
            throw new AppException(s"Output file ${path.toString} is missing")
        def uploadOneFile(path: Path, counter: Int) : Option[String] = {
            try {
                if (DXPY_FILE_TRANSFER) {
                    // shell out to dx upload
                    val dxUploadCmd = s"dx upload ${path.toString} --brief"
                    println(s"--  ${dxUploadCmd}")
                    val (outmsg, errmsg) = execCommand(dxUploadCmd)
                    if (!outmsg.startsWith("file-"))
                        return None
                    Some(outmsg.trim())
                } else {
                    // upload with java
                    val fname = path.getFileName().toString()
                    val dxfile = DXFile.newFile().setName(fname).build()
                    val fis = new java.io.FileInputStream(path.toString())
                    dxfile.upload(fis)
                    dxfile.close()
                    fis.close()
                    Some(dxfile.getId())
                }
            } catch {
                case e: Throwable =>
                    if (counter < UPLOAD_RETRY_LIMIT)
                        None
                    else throw e
            }
        }

        var counter = 0
        while (counter < UPLOAD_RETRY_LIMIT) {
            println(s"upload file ${path.toString} (try=${counter})")
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

    // syntax errors
    def line(t:Terminal,
             terminalMap: Map[Terminal, WdlSource]): String =
        terminalMap.get(t).get.split("\n")(t.getLine - 1)
    def pointToSource(t: Terminal,
                      terminalMap: Map[Terminal, WdlSource]): String =
        s"${line(t, terminalMap)}\n${" " * (t.getColumn - 1)}^"

}
