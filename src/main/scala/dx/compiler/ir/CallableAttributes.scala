package dx.compiler.ir

import dx.core.ir.{CallableAttribute, Value}

object CallableAttributes {
  final case class TitleAttribute(text: String) extends CallableAttribute
  final case class DescriptionAttribute(text: String) extends CallableAttribute
  final case class SummaryAttribute(text: String) extends CallableAttribute
  final case class DeveloperNotesAttribute(text: String) extends CallableAttribute
  final case class VersionAttribute(text: String) extends CallableAttribute
  final case class DetailsAttribute(details: Map[String, Value]) extends CallableAttribute
  final case class CategoriesAttribute(categories: Vector[String]) extends CallableAttribute
  final case class TypesAttribute(types: Vector[String]) extends CallableAttribute
  final case class TagsAttribute(tags: Vector[String]) extends CallableAttribute
  final case class PropertiesAttribute(properties: Map[String, String]) extends CallableAttribute
  // attributes specific to applications
  final case class OpenSourceAttribute(isOpenSource: Boolean) extends CallableAttribute
  // attributes specific to workflow
  final case class CallNamesAttribute(mapping: Map[String, String]) extends CallableAttribute
  final case class RunOnSingleNodeAttribute(value: Boolean) extends CallableAttribute
}
