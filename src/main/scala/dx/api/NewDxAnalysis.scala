package dx.api
import spray.json.{JsObject, JsValue}

case class NewDxAnalysis(id: String, dxProject: Option[DxProject], dxApi: DxApi = DxApi.get)
    extends BaseDxDataObjectDescribe(dxProject, dxApi) {

  override protected val otherFields: Set[DescriptionField.DescriptionField] = Set(
      DescriptionField.Executable,
      DescriptionField.ExecutableName,
      DescriptionField.BillTo,
      DescriptionField.RootExecution,
      DescriptionField.ParentJob,
      DescriptionField.ParentAnalysis,
      DescriptionField.Analysis,
      DescriptionField.Stage,
      DescriptionField.Workflow,
      DescriptionField.Stages,
      DescriptionField.ExecutionState,
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

  override protected def callAddTags(request: Map[String, JsValue]): Unit = {
    dxApi.analysisAddTags(id, request)
  }

  override protected def callRemoveTags(request: Map[String, JsValue]): Unit = {
    dxApi.analysisRemoveTags(id, request)
  }

  override protected def callSetProperties(request: Map[String, JsValue]): Unit = {
    dxApi.analysisSetProperties(id, request)
  }

  def executableName: String = getField[String](DescriptionField.ExecutableName)

  def input: Map[String, JsValue] = getField[Map[String, JsValue]](DescriptionField.Input)

  def output: Option[Map[String, JsValue]] =
    getField[Option[Map[String, JsValue]]](DescriptionField.Output)
}

case class NewDxApp(id: String,
                    override val dxProject: Option[DxProject],
                    override val dxApi: DxApi = DxApi.get)
    extends BaseDxObjectDescribe
    with NewDxApplication {

  override protected val defaultFields: Set[DescriptionField.DescriptionField] = {
    super.defaultFields | Set(DescriptionField.InputSpec, DescriptionField.OutputSpec)
  }

  override protected val otherFields: Set[DescriptionField.DescriptionField] = {
    super.otherFields | Set(
        DescriptionField.BillTo,
        DescriptionField.Version,
        DescriptionField.Aliases,
        DescriptionField.AppCreatedBy,
        DescriptionField.Installed,
        DescriptionField.OpenSource,
        DescriptionField.IgnoreReuse,
        DescriptionField.Deleted,
        DescriptionField.Installs,
        DescriptionField.IsDeveloperFor,
        DescriptionField.AuthorizedUsers,
        DescriptionField.RegionalOptions,
        DescriptionField.HttpsApp,
        DescriptionField.Published,
        DescriptionField.Title,
        DescriptionField.Summary,
        DescriptionField.Description,
        DescriptionField.Categories,
        DescriptionField.LineItemPerTest,
        DescriptionField.Access,
        DescriptionField.DxApiVersion,
        DescriptionField.RunSpec
    )
  }

  override protected def callDescribe(request: Map[String, JsValue]): JsObject = {
    dxApi.appDescribe(id, request)
  }

  override protected def callRun(request: Map[String, JsValue]): JsObject = {
    dxApi.appRun(id, request)
  }
}

case class NewDxApplet(id: String,
                       override val dxProject: Option[DxProject],
                       override val dxApi: DxApi = DxApi.get)
    extends BaseDxDataObjectDescribe(dxProject, dxApi)
    with NewDxApplication {

  override protected val defaultFields: Set[DescriptionField.DescriptionField] = {
    super.defaultFields | Set(DescriptionField.InputSpec, DescriptionField.OutputSpec)
  }

  override protected val otherFields: Set[DescriptionField.DescriptionField] = {
    super.otherFields | Set(
        DescriptionField.Types,
        DescriptionField.ObjectState,
        DescriptionField.Hidden,
        DescriptionField.Links,
        DescriptionField.Sponsored,
        DescriptionField.AppletCreatedBy,
        DescriptionField.RunSpec,
        DescriptionField.DxApiVersion,
        DescriptionField.Access,
        DescriptionField.Title,
        DescriptionField.Summary,
        DescriptionField.Description,
        DescriptionField.DeveloperNotes,
        DescriptionField.IgnoreReuse,
        DescriptionField.HttpsApp,
        DescriptionField.SponsoredUntil
    )
  }

  override protected def callAddTags(request: Map[String, JsValue]): Unit = {
    dxApi.appletAddTags(id, request)
  }

  override protected def callRemoveTags(request: Map[String, JsValue]): Unit = {
    dxApi.appletRemoveTags(id, request)
  }

  override protected def callSetProperties(request: Map[String, JsValue]): Unit = {
    dxApi.appletSetProperties(id, request)
  }

  override protected def callDescribe(request: Map[String, JsValue]): JsObject = {
    dxApi.appletGet(id, request)
  }

  override protected def callRun(request: Map[String, JsValue]): JsObject = {
    dxApi.appletRun(id, request)
  }
}
