package dx.api

import spray.json.JsValue
import wdlTools.util.{Enum, JsUtils}

class NewDxObject {}

object DescriptionField extends Enum {
  type DescriptionField = Value
  val BillTo, Created, Executable, ExecutableName, Folder, Modified, Name, Project, RootExecution =
    Value
}

trait DescriptionFieldSpec[T] {
  val key: String

  def parse(jsValue: JsValue): T
}

abstract class StringFieldSpec(val key: String) extends DescriptionFieldSpec[String] {
  def parse(jsValue: JsValue): String = {
    JsUtils.getString(jsValue, Some(key))
  }
}

abstract class LongFieldSpec(val key: String) extends DescriptionFieldSpec[Long] {
  def parse(jsValue: JsValue): Long = {
    JsUtils.getLong(jsValue, Some(key))
  }
}

object BillToFieldSpec extends StringFieldSpec("billTo")
object CreatedFieldSpec extends LongFieldSpec("created")
object ExecutableFieldSpec extends StringFieldSpec("executable")
object ExecutableNameFieldSpec extends StringFieldSpec("executableName")
object FolderFieldSpec extends StringFieldSpec("folder")
object ModifiedFieldSpec extends LongFieldSpec("modified")
object NameFieldSpec extends StringFieldSpec("name")
object ProjectFieldSpec extends StringFieldSpec("project")
object RootExecutionFieldSpec extends StringFieldSpec("rootExecution")

object DescriptionFieldSpec {
  val Specs: Map[DescriptionField.DescriptionField, DescriptionFieldSpec[_]] = Map(
      DescriptionField.Created -> CreatedFieldSpec,
      DescriptionField.Executable -> ExecutableFieldSpec,
      DescriptionField.ExecutableName -> ExecutableNameFieldSpec,
      DescriptionField.Folder -> FolderFieldSpec,
      DescriptionField.Modified -> ModifiedFieldSpec,
      DescriptionField.Name -> NameFieldSpec,
      DescriptionField.Project -> ProjectFieldSpec,
      DescriptionField.RootExecution -> RootExecutionFieldSpec
  )
}
