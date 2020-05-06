package wdlTools.eval

// This is the WDL typesystem
object WdlValues {
  // any WDL value
  sealed trait WV

  // primitive values
  case object WV_Null extends WV
  case class WV_Boolean(value: Boolean) extends WV
  case class WV_Int(value: Int) extends WV
  case class WV_Float(value: Double) extends WV
  case class WV_String(value: String) extends WV
  case class WV_File(value: String) extends WV

  // compound values
  case class WV_Pair(l: WV, r: WV) extends WV
  case class WV_Array(value: Vector[WV]) extends WV
  case class WV_Map(value: Map[WV, WV]) extends WV
  case class WV_Optional(value: WV) extends WV
  case class WV_Struct(name: String, members: Map[String, WV]) extends WV
  case class WV_Object(members: Map[String, WV]) extends WV

  // results from calling a task or workflow
  case class WV_Call(name: String, members: Map[String, WV]) extends WV
}
