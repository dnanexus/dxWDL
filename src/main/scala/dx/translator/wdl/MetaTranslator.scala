package dx.translator.wdl

import dx.api.ConstraintOper
import dx.compiler.ir.ParameterAttributes
import dx.core.ir.Value._
import dx.core.ir.{CallableAttribute, ParameterAttribute, Value}
import dx.core.languages.wdl
import dx.translator.{CallableAttributes, ParameterAttributes}
import wdlTools.eval.Meta
import wdlTools.eval.WdlValues._
import wdlTools.syntax.WdlVersion
import wdlTools.types.WdlTypes._
import wdlTools.types.{TypedAbstractSyntax => TAT}
import wdlTools.util.Adjuncts

private object MetaUtils {
  // strip muli-layer array types and get the member
  //
  // examples:
  // input             output
  // Array[T]          T
  // Array[Array[T]]   T
  @scala.annotation.tailrec
  def unwrapWdlArrayType(wdlType: T): T = {
    wdlType match {
      case T_Array(t, _) => unwrapWdlArrayType(t)
      case x             => x
    }
  }
}

object MetaTranslator {
  val Categories = "categories"
  val Description = "description"
  val Details = "details"
  val DeveloperNotes = "developer_notes"
  val Properties = "properties"
  val Summary = "summary"
  val Tags = "tags"
  val Title = "title"
  val Types = "types"
  val Version = "version"
  val OpenSource = "open_source"
  val CallNames = "call_names"
  val RunOnSingleNode = "run_on_single_node"
}

abstract class MetaTranslator(wdlVersion: WdlVersion,
                              metaSection: Option[TAT.MetaSection],
                              adjunctFiles: Vector[Adjuncts.AdjunctFile]) {
  private lazy val meta: Meta = Meta.create(wdlVersion, metaSection)

  protected def translate(name: String, value: V): Option[CallableAttribute] = {
    (name, value) match {
      case (MetaTranslator.Title, V_String(text)) => Some(CallableAttributes.TitleAttribute(text))
      case (MetaTranslator.Description, V_String(text)) =>
        Some(CallableAttributes.DescriptionAttribute(text))
      case (MetaTranslator.Summary, V_String(text)) =>
        Some(CallableAttributes.SummaryAttribute(text))
      case (MetaTranslator.DeveloperNotes, V_String(text)) =>
        Some(CallableAttributes.DeveloperNotesAttribute(text))
      case (MetaTranslator.Version, V_String(text)) =>
        Some(CallableAttributes.VersionAttribute(text))
      case (MetaTranslator.Details, V_Object(fields)) =>
        Some(CallableAttributes.DetailsAttribute(fields.map {
          case (name, wdlValue) => name -> wdl.Utils.toIRValue(wdlValue)
        }))
      case (MetaTranslator.Categories, V_Array(array)) =>
        Some(CallableAttributes.CategoriesAttribute(array.map {
          case V_String(text) => text
          case other          => throw new Exception(s"Invalid category: ${other}")
        }))
      case (MetaTranslator.Types, V_Array(array)) =>
        Some(CallableAttributes.TypesAttribute(array.map {
          case V_String(text) => text
          case other          => throw new Exception(s"Invalid type: ${other}")
        }))
      case (MetaTranslator.Tags, V_Array(array)) =>
        Some(CallableAttributes.TagsAttribute(array.map {
          case V_String(text) => text
          case other          => throw new Exception(s"Invalid tag: ${other}")
        }))
      case (MetaTranslator.Properties, V_Object(fields)) =>
        Some(CallableAttributes.PropertiesAttribute(fields.view.mapValues {
          case V_String(text) => text
          case other          => throw new Exception(s"Invalid property value: ${other}")
        }.toMap))
      case _ => None
    }
  }

  def translate: Vector[CallableAttribute] = {
    val attrs = metaSection match {
      case None => Vector.empty
      case Some(TAT.MetaSection(kvs, _)) =>
        kvs.keySet
          .collect { name =>
            val value = meta.get(name)
            translate(name, value.get)
          }
          .flatten
          .toVector
    }

    val adjunctAttrs: Vector[CallableAttribute] = adjunctFiles.collect {
      case Adjuncts.Readme(text) if !meta.contains(MetaTranslator.Description) =>
        CallableAttributes.DescriptionAttribute(text)
      case Adjuncts.DeveloperNotes(text) if !meta.contains(MetaTranslator.DeveloperNotes) =>
        CallableAttributes.DeveloperNotesAttribute(text)
    }

    attrs ++ adjunctAttrs
  }
}

case class ApplicationMetaTranslator(wdlVersion: WdlVersion,
                                     metaSection: Option[TAT.MetaSection],
                                     adjunctFiles: Vector[Adjuncts.AdjunctFile] = Vector.empty)
    extends MetaTranslator(wdlVersion, metaSection, adjunctFiles) {
  override protected def translate(name: String, value: V): Option[CallableAttribute] = {
    (name, value) match {
      case (MetaTranslator.OpenSource, V_Boolean(b)) =>
        Some(CallableAttributes.OpenSourceAttribute(b))
      case _ => super.translate(name, value)
    }
  }
}

case class WorkflowMetaTranslator(wdlVersion: WdlVersion,
                                  metaSection: Option[TAT.MetaSection],
                                  adjunctFiles: Vector[Adjuncts.AdjunctFile] = Vector.empty)
    extends MetaTranslator(wdlVersion, metaSection, adjunctFiles) {
  override protected def translate(name: String, value: V): Option[CallableAttribute] = {
    (name, value) match {
      case (MetaTranslator.CallNames, V_Object(fields)) =>
        Some(CallableAttributes.CallNamesAttribute(fields.view.mapValues {
          case V_String(text) => text
          case other          => throw new Exception(s"Invalid call name value: $other")
        }.toMap))
      case (MetaTranslator.RunOnSingleNode, V_Boolean(b)) =>
        Some(CallableAttributes.RunOnSingleNodeAttribute(b))
      case _ =>
        super.translate(name, value)
    }
  }
}

object ParameterMetaTranslator {
  // Keywords for string pattern matching in WDL parameter_meta
  val Choices = "choices"
  val Default = "default"
  val Description = "description" // accepted as a synonym to 'help'
  val Group = "group"
  val Help = "help"
  val Label = "label"
  val Patterns = "patterns"
  val Suggestions = "suggestions"
  val Type = "dx_type"
  val ConstraintAnd = "and"
  val ConstraintOr = "or"

  // Convert a patterns WDL object value to IR
  private def metaPatternsObjToIR(obj: Map[String, V]): ParameterAttributes.Patterns = {
    val name = obj.get("name") match {
      case Some(V_Array(array)) =>
        array.map {
          case V_String(s) => s
          case _           => throw new Exception("Expected MetaValueString")
        }
      case _ =>
        Vector.empty
    }
    val klass = obj.get("class") match {
      case Some(V_String(value)) => Some(value)
      case _                     => None
    }
    val tag = obj.get("tag") match {
      case Some(V_Array(array)) =>
        array.map {
          case V_String(s) => s
          case _           => throw new Exception("Expected MetaValueString")
        }
      case _ =>
        Vector.empty
    }
    // Even if all were None, create the IR.IOAttrPatterns object
    // The all none is handled in the native generation
    ParameterAttributes.PatternsObject(name, klass, tag)
  }

  private def metaChoiceValueToIR(wdlType: T,
                                  value: V,
                                  name: Option[V] = None): ParameterAttributes.Choice = {
    (wdlType, value) match {
      case (T_String, V_String(str)) =>
        ParameterAttributes.SimpleChoice(VString(str))
      case (T_Int, V_Int(i)) =>
        ParameterAttributes.SimpleChoice(VInt(i))
      case (T_Float, V_Float(f)) =>
        ParameterAttributes.SimpleChoice(VFloat(f))
      case (T_Boolean, V_Boolean(b)) =>
        ParameterAttributes.SimpleChoice(VBoolean(b))
      case (T_File, V_String(str)) =>
        val nameStr: Option[String] = name match {
          case Some(V_String(str)) => Some(str)
          case _                   => None
        }
        ParameterAttributes.FileChoice(value = str, name = nameStr)
      case (T_Directory, V_String(str)) =>
        val nameStr: Option[String] = name match {
          case Some(V_String(str)) => Some(str)
          case _                   => None
        }
        ParameterAttributes.DirectoryChoice(value = str, name = nameStr)

      case _ =>
        throw new Exception(
            "Choices keyword is only valid for primitive- and file-type parameters, and types must "
              + "match between parameter and choices"
        )
    }
  }

  // A choices array may contain either raw values or (for data object types) annotated values,
  // which are hashes with required 'value' key and optional 'name' key. Each value must be of the
  // same type as the parameter, unless the parameter is an array, in which case choice values must
  // be of the same type as the array's contained type. For now, we only allow choices for
  // primitive- and file-type parameters, because there could be ambiguity (e.g. if a choice has a
  // 'value' key, should we treat it as a raw map value or as an annotated value?).
  //
  // choices: [true, false]
  // OR
  // choices: [{name: 'file1', value: "dx://file-XXX"}, {name: 'file2', value: "dx://file-YYY"}]
  private def metaChoicesArrayToIR(array: Vector[V],
                                   wdlType: T): Vector[ParameterAttributes.Choice] = {
    if (array.isEmpty) {
      Vector.empty
    } else {
      array.map {
        case V_Object(fields) if !fields.contains("value") =>
          throw new Exception("Annotated choice must have a 'value' key")
        case V_Object(fields) =>
          metaChoiceValueToIR(
              wdlType = wdlType,
              value = fields("value"),
              name = fields.get("name")
          )
        case rawElement: V =>
          metaChoiceValueToIR(wdlType = wdlType, value = rawElement)
        case _ =>
          throw new Exception(
              "Choices array must contain only raw values or annotated values (hash with "
                + "optional 'name' and required 'value' keys)"
          )
      }
    }
  }

  private def metaSuggestionValueToIR(wdlType: T,
                                      value: Option[V],
                                      name: Option[V] = None,
                                      project: Option[V] = None,
                                      path: Option[V] = None): ParameterAttributes.Suggestion = {
    (wdlType, value) match {
      case (T_String, Some(V_String(s)))   => ParameterAttributes.SimpleSuggestion(VString(s))
      case (T_Int, Some(V_Int(i)))         => ParameterAttributes.SimpleSuggestion(VInt(i))
      case (T_Float, Some(V_Float(f)))     => ParameterAttributes.SimpleSuggestion(VFloat(f))
      case (T_Boolean, Some(V_Boolean(b))) => ParameterAttributes.SimpleSuggestion(VBoolean(b))
      case (T_File, file) =>
        val s = ParameterAttributes.FileSuggestion(
            file.map {
              case V_File(path) => path
            },
            name.map {
              case V_String(str) => str
            },
            project.map {
              case V_String(str) => str
            },
            path.map {
              case V_String(str) => str
            }
        )
        if (s.name.isEmpty && (s.project.isEmpty || s.path.isEmpty)) {
          throw new Exception(
              "If 'value' is not defined for a file-type suggestion, then both 'project' and 'path' "
                + "must be defined"
          )
        }
        s
      // TODO: (T_Directory, dir)
      case _ =>
        throw new Exception(
            "Suggestion keyword is only valid for primitive- and file-type parameters, and types "
              + "must match between parameter and suggestions"
        )
    }
  }

  // A suggestions array may contain either raw values or (for data object types) annotated values,
  // which are hashes. Each value must be of the same type as the parameter, unless the parameter
  // is an array, in which case choice values must be of the same type as the array's contained
  // type. For now, we only allow choices for primitive- and file-type parameters, because there
  // could be ambiguity (e.g. if a choice has a 'value' key, should we treat it as a raw map value
  // or as an annotated value?).
  //
  // suggestions: [true, false]
  // OR
  // suggestions: [
  //  {name: 'file1', value: "dx://file-XXX"}, {name: 'file2', value: "dx://file-YYY"}]
  private def metaSuggestionsArrayToIR(array: Vector[V],
                                       wdlType: T): Vector[ParameterAttributes.Suggestion] = {
    if (array.isEmpty) {
      Vector()
    } else {
      array.map {
        case V_Object(fields) =>
          metaSuggestionValueToIR(
              wdlType = wdlType,
              name = fields.get("name"),
              value = fields.get("value"),
              project = fields.get("project"),
              path = fields.get("path")
          )
        case rawElement: V =>
          metaSuggestionValueToIR(wdlType = wdlType, value = Some(rawElement))
        case _ =>
          throw new Exception(
              "Suggestions array must contain only raw values or annotated (hash) values"
          )
      }
    }
  }

  private def metaConstraintToIR(constraint: V): ParameterAttributes.Constraint = {
    constraint match {
      case V_Object(obj) if obj.size != 1 =>
        throw new Exception("Constraint hash must have exactly one 'and' or 'or' key")
      case V_Object(obj) =>
        obj.head match {
          case (ConstraintAnd, V_Array(array)) =>
            ParameterAttributes.CompoundConstraint(ConstraintOper.And,
                                                   array.map(metaConstraintToIR))
          case (ConstraintOr, V_Array(array)) =>
            ParameterAttributes.CompoundConstraint(ConstraintOper.Or, array.map(metaConstraintToIR))
          case _ =>
            throw new Exception(
                "Constraint must have key 'and' or 'or' and an array value"
            )
        }
      case V_String(s) =>
        ParameterAttributes.StringConstraint(s)
      case _ =>
        throw new Exception("'dx_type' constraints must be either strings or hashes")
    }
  }

  private def metaDefaultToIR(value: V, wdlType: T): Value = {
    value match {
      case _: VHash =>
        throw new Exception(
            "Default keyword is only valid for primitive-, file-, and array-type parameters, and "
              + "types must match between parameter and default"
        )
      case _ =>
        wdl.Utils.toIRValue(value, wdlType)
    }
  }

  // Extract the parameter_meta info from the WDL structure
  // The parameter's T is passed in since some parameter metadata values are required to
  // have the same type as the parameter.
  def translate(paramMeta: Option[V], wdlType: T): Vector[ParameterAttribute] = {
    paramMeta match {
      case None => Vector.empty
      // If the parameter metadata is a string, treat it as help
      case Some(V_String(text)) => Vector(ParameterAttributes.HelpAttribute(text))
      case Some(V_Object(obj)) => {
        // Whether to use 'description' in place of help
        val noHelp = !obj.contains(Help)
        // Use flatmap to get the parameter metadata keys if they exist
        obj.flatMap {
          case (Group, V_String(text)) => Some(ParameterAttributes.GroupAttribute(text))
          case (Help, V_String(text))  => Some(ParameterAttributes.HelpAttribute(text))
          // Use 'description' in place of 'help' if the former is present and the latter is not
          case (Description, V_String(text)) if noHelp =>
            Some(ParameterAttributes.HelpAttribute(text))
          case (Label, V_String(text)) => Some(ParameterAttributes.LabelAttribute(text))
          // Try to parse the patterns key
          // First see if it's an array
          case (Patterns, V_Array(array)) =>
            val patterns = array.map {
              case V_String(s) => s
              case _           => throw new Exception("Expected MetaValueString")
            }
            Some(ParameterAttributes.PatternsAttribute(ParameterAttributes.PatternsArray(patterns)))
          // See if it's an object, and if it is, parse out the optional key, class, and tag keys
          // Note all three are optional
          case (Patterns, V_Object(obj)) =>
            Some(ParameterAttributes.PatternsAttribute(metaPatternsObjToIR(obj)))
          // Try to parse the choices key, which will be an array of either values or objects
          case (Choices, V_Array(array)) =>
            val wt = MetaUtils.unwrapWdlArrayType(wdlType)
            Some(ParameterAttributes.ChoicesAttribute(metaChoicesArrayToIR(array, wt)))
          case (Suggestions, V_Array(array)) =>
            val wt = MetaUtils.unwrapWdlArrayType(wdlType)
            Some(ParameterAttributes.SuggestionsAttribute(metaSuggestionsArrayToIR(array, wt)))
          case (Type, dxType: V) =>
            val wt = MetaUtils.unwrapWdlArrayType(wdlType)
            wt match {
              case T_File => Some(ParameterAttributes.TypeAttribute(metaConstraintToIR(dxType)))
              case _      => throw new Exception("'dx_type' can only be specified for File parameters")
            }
          case (Default, default: V) =>
            Some(ParameterAttributes.DefaultAttribute(metaDefaultToIR(default, wdlType)))
          case _ => None
        }.toVector
      }
      case _ =>
        // TODO: or throw exception?
        Vector.empty
    }
  }
}

case class ParameterMetaTranslator(wdlVersion: WdlVersion, metaSection: Option[TAT.MetaSection]) {
  private lazy val meta: Meta = Meta.create(wdlVersion, metaSection)

  def translate(name: String, parameterType: T): Vector[ParameterAttribute] = {
    val metaValue = meta.get(name)
    ParameterMetaTranslator.translate(metaValue, parameterType)
  }
}
