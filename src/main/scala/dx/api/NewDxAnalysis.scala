package dx.api
import dx.api.DescriptionField.DescriptionField
import spray.json.{JsObject, JsValue}

case class NewDxAnalysis(id: String, dxProject: Option[DxProject], dxApi: DxApi = DxApi.get)
    extends BaseDxDataObjectDescribe(dxProject, dxApi) {

  override protected val otherFields: Set[DescriptionField] = Set(
      DescriptionField.Executable,
      DescriptionField.ExecutableName,
      DescriptionField.BillTo,
      DescriptionField.RootExecution,
      DescriptionField.ParentJob,
      DescriptionField.ParentAnalysis,
      DescriptionField.Analysis,
      DescriptionField.Stage,
      // TODO: right now Workflow just returns the wf ID - should return the full describe instead
      DescriptionField.Workflow,
      // TODO: right now Stages just returns a mapping of stage ID to execution ID - should also
      //  return the full describe for each stage
      DescriptionField.Stages,
      DescriptionField.State,
      DescriptionField.Workspace,
      DescriptionField.LaunchedBy,
      DescriptionField.Tags,
      DescriptionField.Properties,
      DescriptionField.Details,
      DescriptionField.RunInput,
      DescriptionField.OriginalInput,
      DescriptionField.Input,
      DescriptionField.Output,
      DescriptionField.DelayWorkspaceDestruction,
      DescriptionField.TotalPrice,
      DescriptionField.PriceComputedAt,
      DescriptionField.SubtotalPriceInfo
  )

  override protected def callDescribe(request: Map[String, JsValue]): JsObject = {
    dxApi.analysisDescribe(id, request)
  }

  override protected def callSetProperties(request: Map[String, JsValue]): Unit = {
    dxApi.analysisSetProperties(id, request)
  }

  def executableName: String = getField[String](DescriptionField.ExecutableName)

  def input: Map[String, JsValue] = getField[Map[String, JsValue]](DescriptionField.Input)

  def output: Option[Map[String, JsValue]] =
    getField[Option[Map[String, JsValue]]](DescriptionField.Output)
}
