package dx.exec

import java.nio.file.{Path, Paths}

import dx.core.languages.wdl.{TypeSerialization, WdlValueSerialization}
import spray.json._
import wdlTools.eval.{Eval, WdlValues, Context => EvalContext}
import wdlTools.types.{WdlTypes, TypedAbstractSyntax => TAT}
import wdlTools.util.{FileSource, FileSourceResolver, RealFileSource, Util}

trait RunnerEnv {
  def env: Map[String, (WdlTypes.T, WdlValues.V)]

  override lazy val toString: String = {
    env
      .map {
        case (inp, (wdlType, wdlValue)) =>
          s"${inp} -> (${wdlType}, ${wdlValue})"
      }
      .mkString("\n")
  }
}

object RunnerEnv {
  // evaluate the declarations using the inputs
  def evalInputsAndDeclarations(inputs: Map[String, (WdlTypes.T, WdlValues.V)],
                                task: TAT.Task,
                                evaluator: Eval): Map[String, (WdlTypes.T, WdlValues.V)] = {

    task.declarations.foldLeft(inputs) {
      case (env, TAT.Declaration(name, wdlType, Some(expr), _)) =>
        val envValues = env.map { case (name, (_, v)) => name -> v }
        val wdlValue =
          evaluator.applyExprAndCoerce(expr, wdlType, EvalContext(envValues))
        env + (name -> (wdlType, wdlValue))
      case (_, TAT.Declaration(name, _, None, _)) =>
        throw new Exception(s"Declaration ${name} has no expression")
    }
  }
}

case class RunnerInputs(env: Map[String, (WdlTypes.T, WdlValues.V)]) extends RunnerEnv

object RunnerInputs {
  def apply(inputs: Map[TAT.InputDefinition, WdlValues.V],
            task: TAT.Task,
            evaluator: Eval): RunnerInputs = {
    val inputsWithTypes: Map[String, (WdlTypes.T, WdlValues.V)] =
      inputs.map {
        case (inpDfn, value) => inpDfn.name -> (inpDfn.wdlType, value)
      }
    RunnerInputs(RunnerEnv.evalInputsAndDeclarations(inputsWithTypes, task, evaluator))
  }
}

case class LocalizedRunnerInputs(env: Map[String, (WdlTypes.T, WdlValues.V)],
                                 fileSourceToPath: Map[FileSource, Path])
    extends RunnerEnv {
  // serialize the task inputs to json, and then write to a file.
  def writeToDisk(path: Path, typeAliases: Map[String, WdlTypes.T]): Unit = {
    val localPathToJs: Map[String, JsValue] = env.map {
      case (name, (t, v)) =>
        val wdlTypeRepr = TypeSerialization(typeAliases).toString(t)
        val value = WdlValueSerialization(typeAliases).toJSON(t, v)
        (name, JsArray(JsString(wdlTypeRepr), value))
    }
    val dxUriToJs: Map[String, JsValue] = fileSourceToPath.map {
      case (fileSource: RealFileSource, path) => fileSource.value -> JsString(path.toString)
      case (other, _) =>
        throw new RuntimeException(s"Can only serialize a RealFileSource, not ${other}")
    }

    // marshal into json, and then to a string
    val json =
      JsObject("localizedInputs" -> JsObject(localPathToJs), "dxUrl2path" -> JsObject(dxUriToJs))
    Util.writeFileContent(path, json.prettyPrint)
  }
}

object LocalizedRunnerInputs {
  def apply(inputs: Map[String, (WdlTypes.T, WdlValues.V)],
            fileSourceToPath: Map[FileSource, Path],
            task: TAT.Task,
            evaluator: Eval): LocalizedRunnerInputs = {
    LocalizedRunnerInputs(RunnerEnv.evalInputsAndDeclarations(inputs, task, evaluator),
                          fileSourceToPath)
  }

  def apply(path: Path,
            typeAliases: Map[String, WdlTypes.T],
            fileResolver: FileSourceResolver): LocalizedRunnerInputs = {
    val buf = Util.readFileContent(path)
    val json: JsValue = buf.parseJson
    val (localPathToJs, dxUriToJs) = json match {
      case JsObject(m) =>
        (m.get("localizedInputs"), m.get("dxUrl2path")) match {
          case (Some(JsObject(env_m)), Some(JsObject(path_m))) =>
            (env_m, path_m)
          case (_, _) =>
            throw new Exception("Malformed environment serialized to disk")
        }
      case _ => throw new Exception("Malformed environment serialized to disk")
    }
    val localizedInputs = localPathToJs.map {
      case (key, JsArray(Vector(JsString(wdlTypeRepr), jsVal))) =>
        val t = TypeSerialization(typeAliases).fromString(wdlTypeRepr)
        val value = WdlValueSerialization(typeAliases).fromJSON(jsVal)
        key -> (t, value)
      case (_, other) =>
        throw new Exception(s"sanity: bad deserialization value ${other}")
    }
    val fileSourceToPath = dxUriToJs.map {
      case (uri, JsString(path)) => fileResolver.resolve(uri) -> Paths.get(path)
      case (_, _)                => throw new Exception("Sanity")
    }
    LocalizedRunnerInputs(localizedInputs, fileSourceToPath)
  }
}
