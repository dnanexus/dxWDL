/** Run a wdl task.

In the example below, we want to run the Add task in a dx:applet.

task Add {
    Int a
    Int b

    command {
        echo $((a + b))
    }
    output {
        Int sum = read_int(stdout())
    }
}

  */

package dxWDL

import com.dnanexus.{DXJob, IOClass}
import java.nio.file.{Path, Paths}
import scala.collection.mutable.HashMap
import spray.json._
import Utils.{appletLog, DX_URL_PREFIX, RUNNER_TASK_ENV_FILE}
import wdl4s.wdl.{Declaration, DeclarationInterface, WdlExpression, WdlTask}
import wdl4s.wdl.values._
import wdl4s.wdl.types._

private object RunnerTaskSerialization {
    // Serialization of a WDL value to JSON
    private def wdlToJSON(t:WdlType, w:WdlValue) : JsValue = {
        (t, w)  match {
            // Base case: primitive types.
            // Files are encoded as their full path.
            case (WdlBooleanType, WdlBoolean(b)) => JsBoolean(b)
            case (WdlIntegerType, WdlInteger(n)) => JsNumber(n)
            case (WdlFloatType, WdlFloat(x)) => JsNumber(x)
            case (WdlStringType, WdlString(s)) => JsString(s)
            case (WdlStringType, WdlSingleFile(path)) => JsString(path)
            case (WdlFileType, WdlSingleFile(path)) => JsString(path)
            case (WdlFileType, WdlString(path)) => JsString(path)

            // arrays
            // Base case: empty array
            case (_, WdlArray(_, ar)) if ar.length == 0 =>
                JsArray(Vector.empty)

            // Non empty array
            case (WdlArrayType(t), WdlArray(_, elems)) =>
                val jsVals = elems.map(e => wdlToJSON(t, e))
                JsArray(jsVals.toVector)

            // Maps. These are projections from a key to value, where
            // the key and value types are statically known.
            //
            // keys are strings, we can use JSON objects
            case (WdlMapType(WdlStringType, valueType), WdlMap(_, m)) =>
                JsObject(m.map{
                         case (WdlString(k), v) =>
                             k -> wdlToJSON(valueType, v)
                         case (k,_) =>
                             throw new Exception(s"key ${k.toWdlString} should be a WdlStringType")
                     }.toMap)

            // general case, the keys are not strings.
            case (WdlMapType(keyType, valueType), WdlMap(_, m)) =>
                val keys:WdlValue = WdlArray(WdlArrayType(keyType), m.keys.toVector)
                val kJs = wdlToJSON(keys.wdlType, keys)
                val values:WdlValue = WdlArray(WdlArrayType(valueType), m.values.toVector)
                val vJs = wdlToJSON(values.wdlType, values)
                JsObject("keys" -> kJs, "values" -> vJs)

            // keys are strings, requiring no conversion. We do
            // need to carry the types are runtime.
            case (WdlObjectType, WdlObject(m: Map[String, WdlValue])) =>
                JsObject(m.map{ case (k, v) =>
                             k -> JsObject(
                                 "type" -> JsString(v.wdlType.toWdlString),
                                 "value" -> wdlToJSON(v.wdlType, v))
                         }.toMap)

            case (WdlPairType(lType, rType), WdlPair(l,r)) =>
                val lJs = wdlToJSON(lType, l)
                val rJs = wdlToJSON(rType, r)
                JsObject("left" -> lJs, "right" -> rJs)

            // Strip optional type
            case (WdlOptionalType(t), WdlOptionalValue(_,Some(w))) =>
                wdlToJSON(t, w)
            case (WdlOptionalType(t), w) =>
                wdlToJSON(t, w)
            case (t, WdlOptionalValue(_,Some(w))) =>
                wdlToJSON(t, w)

            // missing value
            case (_, WdlOptionalValue(_,None)) => JsNull

            case (_,_) => throw new Exception(
                s"""|Unsupported combination type=(${t.toWdlString},${t})
                    |value=(${w.toWdlString}, ${w})"""
                    .stripMargin.replaceAll("\n", " "))
        }
    }

    private def wdlFromJSON(t:WdlType, jsv:JsValue) : WdlValue = {
        (t, jsv)  match {
            // base case: primitive types
            case (WdlBooleanType, JsBoolean(b)) => WdlBoolean(b.booleanValue)
            case (WdlIntegerType, JsNumber(bnm)) => WdlInteger(bnm.intValue)
            case (WdlFloatType, JsNumber(bnm)) => WdlFloat(bnm.doubleValue)
            case (WdlStringType, JsString(s)) => WdlString(s)
            case (WdlFileType, JsString(path)) => WdlSingleFile(path)

            // arrays
            case (WdlArrayType(t), JsArray(vec)) =>
                WdlArray(WdlArrayType(t),
                         vec.map{ elem => wdlFromJSON(t, elem) })


            // maps with string keys
            case (WdlMapType(WdlStringType, valueType), JsObject(fields)) =>
                val m: Map[WdlValue, WdlValue] = fields.map {
                    case (k,v) =>
                        WdlString(k) -> wdlFromJSON(valueType, v)
                }.toMap
                WdlMap(WdlMapType(WdlStringType, valueType), m)

            // General maps. These are serialized as an object with a keys array and
            // a values array.
            case (WdlMapType(keyType, valueType), JsObject(_)) =>
                jsv.asJsObject.getFields("keys", "values") match {
                    case Seq(JsArray(kJs), JsArray(vJs)) =>
                        val m = (kJs zip vJs).map{ case (k, v) =>
                            val kWdl = wdlFromJSON(keyType, k)
                            val vWdl = wdlFromJSON(valueType, v)
                            kWdl -> vWdl
                        }.toMap
                        WdlMap(WdlMapType(keyType, valueType), m)
                    case _ => throw new Exception(s"Malformed serialized map ${jsv}")
                }

            case (WdlObjectType, JsObject(fields)) =>
                val m: Map[String, WdlValue] = fields.map{ case (k,v) =>
                    val elem:WdlValue =
                        v.asJsObject.getFields("type", "value") match {
                            case Seq(JsString(elemTypeStr), elemValue) =>
                                val elemType:WdlType = WdlType.fromWdlString(elemTypeStr)
                                wdlFromJSON(elemType, elemValue)
                        }
                    k -> elem
                }.toMap
                WdlObject(m)

            case (WdlPairType(lType, rType), JsObject(_)) =>
                jsv.asJsObject.getFields("left", "right") match {
                    case Seq(lJs, rJs) =>
                        val left = wdlFromJSON(lType, lJs)
                        val right = wdlFromJSON(rType, rJs)
                        WdlPair(left, right)
                    case _ => throw new Exception(s"Malformed serialized par ${jsv}")
                }

            case (WdlOptionalType(t), JsNull) =>
                WdlOptionalValue(t, None)
            case (WdlOptionalType(t), _) =>
                wdlFromJSON(t, jsv)

            case _ =>
                throw new AppInternalException(
                    s"Unsupported combination ${t.toWdlString} ${jsv.prettyPrint}"
                )
        }
    }

    // serialization routines
    def toJSON(w:WdlValue) : JsValue = {
        JsObject("wdlType" -> JsString(w.wdlType.toWdlString),
                 "wdlValue" -> wdlToJSON(w.wdlType, w))
    }

    def fromJSON(jsv:JsValue) : WdlValue = {
        jsv.asJsObject.getFields("wdlType", "wdlValue") match {
            case Seq(JsString(typeStr), wValue) =>
                val wdlType = WdlType.fromWdlString(typeStr)
                wdlFromJSON(wdlType, wValue)
            case other => throw new DeserializationException(s"WdlValue unexpected ${other}")
        }
    }
}


case class RunnerTask(task:WdlTask,
                      cef: CompilerErrorFormatter) {
    def getMetaDir() = {
        val metaDir = Utils.getMetaDirPath()
        Utils.safeMkdir(metaDir)
        metaDir
    }

    // serialize the task inputs to json, and then write to a file.
    private def writeEnvToDisk(env: Map[String, WdlValue]) : Unit = {
        val m : Map[String, JsValue] = env.map{ case(varName, v) =>
            (varName, RunnerTaskSerialization.toJSON(v))
        }.toMap
        val buf = (JsObject(m)).prettyPrint
        Utils.writeFileContent(getMetaDir().resolve(RUNNER_TASK_ENV_FILE),
                               buf)
    }

    private def readEnvFromDisk() : Map[String, WdlValue] = {
        val buf = Utils.readFileContent(getMetaDir().resolve(RUNNER_TASK_ENV_FILE))
        val json : JsValue = buf.parseJson
        val m = json match {
            case JsObject(m) => m
            case _ => throw new Exception("Malformed task declarations")
        }
        m.map { case (key, jsVal) =>
            key -> RunnerTaskSerialization.fromJSON(jsVal)
        }.toMap
    }

    // Figure out if a docker image is specified. If so, return it as a string.
    private def dockerImageEval(env: Map[String, WdlValue]) : Option[String] = {
        def lookup(varName : String) : WdlValue = {
            env.get(varName) match {
                case Some(x) => x
                case None => throw new AppInternalException(
                    s"No value found for variable ${varName}")
            }
        }
        def evalStringExpr(expr: WdlExpression) : String = {
            val v : WdlValue = expr.evaluate(lookup, DxFunctions).get
            v match {
                case WdlString(s) => s
                case _ => throw new AppInternalException(
                    s"docker is not a string expression ${v.toWdlString}")
            }
        }
        // Figure out if docker is used. If so, it is specified by an
        // expression that requires evaluation.
        task.runtimeAttributes.attrs.get("docker") match {
            case None => None
            case Some(expr) => Some(evalStringExpr(expr))
        }
    }

    private def dockerImage(env: Map[String, WdlValue]) : Option[String] = {
        val dImg = dockerImageEval(env)
        dImg match {
            case Some(url) if url.startsWith(DX_URL_PREFIX) =>
                // This is a record on the platform, created with
                // dx-docker. Describe it with an API call, and get
                // the docker image name.
                appletLog(s"looking up dx:url ${url}")
                val dxRecord = DxPath.lookupDxURLRecord(url)
                appletLog(s"Found record ${dxRecord}")
                val imageName = dxRecord.describe().getName
                appletLog(s"Image name is ${imageName}")
                Some(imageName)
            case _ => dImg
        }
    }

    // Each file marked "stream" is converted into a special fifo
    // file on the instance.
    //
    // Make a named pipe, and stream the file from the platform to the
    // pipe. Ensure pipes have different names, even if the
    // file-names are the same. Write the process ids of the download
    // jobs to stdout. The calling script will keep track of them,
    // and check for abnormal termination.
    //
    private var fifoCount = 0
    private def mkfifo(wvl: WdlVarLinks, path: String) : (WdlValue, String) = {
        val filename = Paths.get(path).toFile.getName
        val fifo:Path = Paths.get(Utils.DX_HOME, s"fifo_${fifoCount}_${filename}")
        fifoCount += 1
        val dxFileId = WdlVarLinks.getFileId(wvl)
        val bashSnippet:String =
            s"""|mkfifo ${fifo.toString}
                |dx cat ${dxFileId} > ${fifo.toString} &
                |echo $$!
                |""".stripMargin
        (WdlSingleFile(fifo.toString), bashSnippet)
    }

    private def handleStreamingFile(wvl: WdlVarLinks,
                                    wdlValue:WdlValue) : (WdlValue, String) = {
        wdlValue match {
            case WdlSingleFile(path) if wvl.attrs.stream =>
                mkfifo(wvl, path)
            case WdlOptionalValue(_,Some(WdlSingleFile(path))) if wvl.attrs.stream =>
                mkfifo(wvl, path)
            case _ =>
                throw new Exception(s"Value is not a streaming file ${wvl} ${wdlValue}")
        }
    }

    // Write the core bash script into a file. In some cases, we
    // need to run some shell setup statements before and after this
    // script. Returns these as two strings (prolog, epilog).
    private def writeBashScript(env: Map[String, WdlValue]) : Unit = {
        val metaDir = getMetaDir()
        val scriptPath = metaDir.resolve("script")
        val stdoutPath = metaDir.resolve("stdout")
        val stderrPath = metaDir.resolve("stderr")
        val rcPath = metaDir.resolve("rc")

        // instantiate the command
        val cmdEnv: Map[Declaration, WdlValue] = env.map {
            case (varName, wdlValue) =>
                val decl = task.declarations.find(_.unqualifiedName == varName) match {
                    case Some(x) => x
                    case None => throw new Exception(
                        s"Cannot find declaration for variable ${varName}")
                }
                decl -> wdlValue
        }.toMap
        val shellCmd : String = task.instantiateCommand(cmdEnv, DxFunctions).get

        // This is based on Cromwell code from
        // [BackgroundAsyncJobExecutionActor.scala].  Generate a bash
        // script that captures standard output, and standard
        // error. We need to be careful to pipe stdout/stderr to the
        // parent stdout/stderr, and not lose the result code of the
        // shell command. Notes on bash magic symbols used here:
        //
        //  Symbol  Explanation
        //    >     redirect stdout
        //    2>    redirect stderr
        //    <     redirect stdin
        //
        val script =
            if (shellCmd.isEmpty) {
                s"""|#!/bin/bash
                    |echo 0 > ${rcPath}
                    |""".stripMargin.trim + "\n"
            } else {
                s"""|#!/bin/bash
                    |(
                    |    cd ${Utils.DX_HOME}
                    |    ${shellCmd}
                    |) \\
                    |  > >( tee ${stdoutPath} ) \\
                    |  2> >( tee ${stderrPath} >&2 )
                    |echo $$? > ${rcPath}
                    |""".stripMargin.trim + "\n"
            }
        appletLog(s"writing bash script to ${scriptPath}")
        Utils.writeFileContent(scriptPath, script)
    }

    private def writeDockerSubmitBashScript(env: Map[String, WdlValue],
                                            imgName: String) : Unit = {
        // The user wants to use a docker container with the
        // image [imgName]. We implement this with dx-docker.
        // There may be corner cases where the image will run
        // into permission limitations due to security.
        //
        // Map the home directory into the container, so that
        // we can reach the result files, and upload them to
        // the platform.
        val DX_HOME = Utils.DX_HOME
        val dockerCmd = s"""|dx-docker run --entrypoint /bin/bash
                            |-v ${DX_HOME}:${DX_HOME}
                            |${imgName}
                            |$${HOME}/execution/meta/script""".stripMargin.replaceAll("\n", " ")
        val dockerRunPath = getMetaDir().resolve("script.submit")
        val dockerRunScript =
            s"""|#!/bin/bash -ex
                |${dockerCmd}""".stripMargin
        appletLog(s"writing docker run script to ${dockerRunPath}")
        Utils.writeFileContent(dockerRunPath, dockerRunScript)
        dockerRunPath.toFile.setExecutable(true)
    }

    // Calculate the input variables for the task, download the input files,
    // and build a shell script to run the command.
    def prolog(inputSpec: Map[String, IOClass],
               outputSpec: Map[String, IOClass],
               inputs: Map[String, WdlVarLinks]) : Map[String, JsValue] = {
        // add the attributes from the parameter_meta section of the task
        val inputWvls = inputs.map{ case (varName, wvl) =>
            val attrs = DeclAttrs.get(task, varName, cef)
            varName -> WdlVarLinks(wvl.wdlType, attrs, wvl.dxlink)
        }.toMap

        val forceFlag =
            if (task.commandTemplateString.trim.isEmpty) {
                // The shell command is empty, there is no need to download the files.
                false
            } else {
                // default: download all input files
                appletLog(s"Eagerly download input files")
                true
            }

        var bashSnippetVec = Vector.empty[String]
        val envInput = inputWvls.map{ case (key, wvl) =>
            val w:WdlValue =
                if (wvl.attrs.stream) {
                    // streaming file, create a named fifo for it
                    val w:WdlValue = WdlVarLinks.localize(wvl, false)
                    val (wdlValueRewrite,bashSnippet) = handleStreamingFile(wvl, w)
                    bashSnippetVec = bashSnippetVec :+ bashSnippet
                    wdlValueRewrite
                } else  {
                    // regular file
                    WdlVarLinks.localize(wvl, forceFlag)
                }
            key -> w
        }.toMap

        // evaluate the declarations, and localize any files if necessary
        val env: Map[String, WdlValue] =
            RunnerEval.evalDeclarations(task.declarations, envInput)
                .map{ case (decl, v) => decl.unqualifiedName -> v}.toMap
        val docker = dockerImage(env)

        // deal with files that need streaming
        if (bashSnippetVec.size > 0) {
            // set up all the named pipes
            val path = getMetaDir().resolve("setup_streams")
            appletLog(s"writing bash script for stream(s) set up to ${path}")
            val snippet = bashSnippetVec.mkString("\n")
            Utils.writeFileContent(path, snippet)
            path.toFile.setExecutable(true)
        }

        // Write shell script to a file. It will be executed by the dx-applet code
        writeBashScript(env)
        docker match {
            case Some(img) =>
                // write a script that launches the actual command inside a docker image.
                // Streamed files are set up before launching docker.
                writeDockerSubmitBashScript(env, img)
            case None => ()
        }

        // serialize the environment, so we don't have to calculate it again in
        // the epilog
        writeEnvToDisk(env)

        // Checkpoint the localized file tables
        LocalDxFiles.freeze()
        DxFunctions.freeze()

        Map.empty
    }

    def epilog(inputSpec: Map[String, IOClass],
               outputSpec: Map[String, IOClass],
               inputs: Map[String, WdlVarLinks]) : Map[String, JsValue] = {
        // Repopulate the localized file tables
        LocalDxFiles.unfreeze()
        DxFunctions.unfreeze()

        val env : Map[String, WdlValue] = readEnvFromDisk()

        // evaluate the output declarations.
        val outputs: Map[DeclarationInterface, WdlValue] = RunnerEval.evalDeclarations(task.outputs, env)

        // Upload any output files to the platform.
        val wvlOutputs:Map[String, WdlVarLinks] = outputs.map{ case (decl, wdlValue) =>
            // The declaration type is sometimes more accurate than the type of the wdlValue
            val wvl = WdlVarLinks.importFromWDL(decl.wdlType,
                                                DeclAttrs.empty, wdlValue, IODirection.Upload)
            decl.unqualifiedName -> wvl
        }.toMap

        // convert the WDL values to JSON
        val outputFields:Map[String, JsValue] = wvlOutputs.map {
            case (key, wvl) => WdlVarLinks.genFields(wvl, key)
        }.toList.flatten.toMap
        outputFields
    }


    // Evaluate the runtime expressions, and figure out which instance type
    // this task requires.
    private def calcInstanceType(taskInputs: Map[String, WdlVarLinks],
                                 instanceTypeDB: InstanceTypeDB) : String = {
        // input variables that were already calculated
        val env = HashMap.empty[String, WdlValue]
        def lookup(varName : String) : WdlValue = {
            env.get(varName) match {
                case Some(x) => x
                case None =>
                    // value not evaluated yet, calculate and keep in cache
                    taskInputs.get(varName) match {
                        case Some(wvl) =>
                            env(varName) = WdlVarLinks.eval(wvl, true, IODirection.Download)
                            env(varName)
                        case None => throw new UnboundVariableException(varName)
                    }
            }
        }
        def evalAttr(attrName: String) : Option[WdlValue] = {
            task.runtimeAttributes.attrs.get(attrName) match {
                case None => None
                case Some(expr) =>
                    Some(expr.evaluate(lookup, DxFunctions).get)
            }
        }

        val dxInstaceType = evalAttr(Utils.DX_INSTANCE_TYPE_ATTR)
        val memory = evalAttr("memory")
        val diskSpace = evalAttr("disks")
        val cores = evalAttr("cpu")
        val iType = instanceTypeDB.apply(dxInstaceType, memory, diskSpace, cores)
        appletLog(s"""|calcInstanceType memory=${memory} disk=${diskSpace}
                      |cores=${cores} instancetype=${iType}"""
                      .stripMargin.replaceAll("\n", " "))
        iType
    }

    private def relaunchBuildInputs(inputWvls: Map[String, WdlVarLinks]) : JsValue = {
        val inputs:Map[String,JsValue] = inputWvls.foldLeft(Map.empty[String, JsValue]) {
            case (accu, (varName, wvl)) =>
                val fields = WdlVarLinks.genFields(wvl, varName)
                accu ++ fields.toMap
        }
        JsObject(inputs.toMap)
    }

    /** The runtime attributes need to be calculated at runtime. Evaluate them,
      *  determine the instance type [xxxx], and relaunch the job on [xxxx]
      */
    def relaunch(inputSpec: Map[String, IOClass],
                 outputSpec: Map[String, IOClass],
                 inputWvls: Map[String, WdlVarLinks]) : Map[String, JsValue] = {
        // Figure out the available instance types, and their prices,
        // by reading the file
        val dbRaw = Utils.readFileContent(Paths.get("/" + Utils.INSTANCE_TYPE_DB_FILENAME))
        val instanceTypeDB = dbRaw.parseJson.convertTo[InstanceTypeDB]

        // evaluate the runtime attributes
        // determine the instance type
        val instanceType:String = calcInstanceType(inputWvls, instanceTypeDB)

        // relaunch the applet on the correct instance type
        val inputs = relaunchBuildInputs(inputWvls)

        // Run a sub-job with the "body" entry point, and the required instance type
        val dxSubJob : DXJob = Utils.runSubJob("body", Some(instanceType), inputs, Vector.empty)

        // Return promises (JBORs) for all the outputs. Since the signature of the sub-job
        // is exactly the same as the parent, we can immediately exit the parent job.
        val outputs: Map[String, JsValue] = task.outputs.map { tso =>
            val wvl = WdlVarLinks(tso.wdlType,
                                  DeclAttrs.empty,
                                  DxlJob(dxSubJob, tso.unqualifiedName))
            WdlVarLinks.genFields(wvl, tso.unqualifiedName)
        }.flatten.toMap
        outputs
    }
}
