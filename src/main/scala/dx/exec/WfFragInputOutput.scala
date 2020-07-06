package dx.exec

import dx.api.{DxApi, DxFile, DxProject}
import dx.core.io.{DxFileDescCache, DxPathConfig, ExecLinkInfo}
import dx.core.languages.wdl.{TypeSerialization, WdlVarLinksConverter}
import dx.exec
import spray.json._
import wdlTools.eval.{Eval, WdlValues}
import wdlTools.syntax.WdlVersion
import wdlTools.types.WdlTypes
import wdlTools.util.FileSourceResolver

case class WfFragInput(blockPath: Vector[Int],
                       env: Map[String, (WdlTypes.T, WdlValues.V)],
                       execLinkInfo: Map[String, ExecLinkInfo])

case class WfFragInputOutput(dxPathConfig: DxPathConfig,
                             fileResolver: FileSourceResolver,
                             dxFileDescCache: DxFileDescCache,
                             dxProject: DxProject,
                             typeAliases: Map[String, WdlTypes.T],
                             wdlVersion: WdlVersion,
                             dxApi: DxApi,
                             evaluator: Eval) {
  val jobInputOutput: JobInputOutput =
    exec.JobInputOutput(dxPathConfig,
                        fileResolver,
                        dxFileDescCache,
                        typeAliases,
                        wdlVersion,
                        dxApi,
                        evaluator)

  private def revTransformVarName(varName: String): String = {
    varName.replaceAll("___", "\\.")
  }

  private def loadWorkflowMetaInfo(
      metaInfo: Map[String, JsValue]
  ): (Map[String, ExecLinkInfo], Vector[Int], Map[String, WdlTypes.T]) = {
    // meta information used for running workflow fragments
    val execLinkInfo: Map[String, ExecLinkInfo] = metaInfo.get("execLinkInfo") match {
      case None => Map.empty
      case Some(JsObject(fields)) =>
        fields.map {
          case (key, ali) =>
            key -> ExecLinkInfo.readJson(dxApi, ali, typeAliases)
        }
      case other => throw new Exception(s"Bad value ${other}")
    }
    val blockPath: Vector[Int] = metaInfo.get("blockPath") match {
      case None => Vector.empty
      case Some(JsArray(arr)) =>
        arr.map {
          case JsNumber(n) => n.toInt
          case _           => throw new Exception("Bad value ${arr}")
        }
      case other => throw new Exception(s"Bad value ${other}")
    }
    val fqnDictTypes: Map[String, WdlTypes.T] = metaInfo.get("fqnDictTypes") match {
      case Some(JsObject(fields)) =>
        fields.map {
          case (key, JsString(value)) =>
            // Transform back to a fully qualified name with dots
            val orgKeyName = revTransformVarName(key)
            val wdlType = TypeSerialization(typeAliases).fromString(value)
            orgKeyName -> wdlType
          case other => throw new Exception(s"Bad value ${other}")
        }
      case other => throw new Exception(s"Bad value ${other}")
    }

    (execLinkInfo, blockPath, fqnDictTypes)
  }

  // 1. Convert the inputs to WDL values
  // 2. Setup an environment to evaluate the sub-block. This should
  //    look to the WDL code as if all previous code had been evaluated.
  def loadInputs(inputs: JsValue, metaInfo: JsValue): WfFragInput = {
    val regularFields: Map[String, JsValue] = inputs.asJsObject.fields
      .filter { case (fieldName, _) => !fieldName.endsWith(WdlVarLinksConverter.FLAT_FILES_SUFFIX) }

    // Extract the meta information needed to setup the closure for the subblock
    val (execLinkInfo, blockPath, fqnDictTypes) = loadWorkflowMetaInfo(metaInfo.asJsObject.fields)

    // What remains are inputs from other stages. Convert from JSON to WDL values
    val env: Map[String, (WdlTypes.T, WdlValues.V)] = regularFields.map {
      case (name, jsValue) =>
        val fqn = revTransformVarName(name)
        val wdlType = fqnDictTypes.get(fqn) match {
          case None =>
            throw new Exception(s"Did not find variable ${fqn} (${name}) in the block environment")
          case Some(x) => x
        }
        val value = jobInputOutput.unpackJobInput(fqn, wdlType, jsValue)
        System.err.println(s"${fqn} before ${jsValue} after ${value}")
        fqn -> (wdlType, value)
    }

    WfFragInput(blockPath, env, execLinkInfo)
  }

  // find all the dx:files that are referenced from the inputs
  def findRefDxFiles(inputs: JsValue, metaInfo: JsValue): Vector[DxFile] = {
    val regularFields: Map[String, JsValue] = inputs.asJsObject.fields
      .filter { case (fieldName, _) => !fieldName.endsWith(WdlVarLinksConverter.FLAT_FILES_SUFFIX) }

    val (_, _, fqnDictTypes) = loadWorkflowMetaInfo(metaInfo.asJsObject.fields)

    // Convert from JSON to WDL values
    regularFields
      .map {
        case (name, jsValue) =>
          val fqn = revTransformVarName(name)
          val wdlType = fqnDictTypes.get(fqn) match {
            case None =>
              throw new Exception(
                  s"Did not find variable ${fqn} (${name}) in the block environment"
              )
            case Some(x) => x
          }
          jobInputOutput.unpackJobInputFindRefFiles(wdlType, jsValue)
      }
      .toVector
      .flatten
  }
}
