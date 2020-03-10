package dxWDL.compiler

import wom.callable.MetaValueElement._
import wom.callable.MetaValueElement
import wom.types._
import dxWDL.dx._

object ParameterMeta {
  // Convert a WOM Vector[MetaValueElement] of Strings into a Vector of Strings
  // Anything not a string will be filtered out.
  private def unwrapMetaStringArray(array: Vector[MetaValueElement]): Vector[String] = {
    array.flatMap {
      case MetaValueElementString(str) => Some(str)
      case _                           => None
    }.toVector
  }

  private def unwrapWomArrayType(womType: WomType): WomType = {
    var wt = womType
    while (wt.isInstanceOf[WomArrayType]) {
      wt = wt.asInstanceOf[WomArrayType].memberType
    }
    wt
  }

  // Convert a patterns WOM object value to IR
  private def metaPatternsObjToIR(obj: Map[String, MetaValueElement]): IR.IOAttrPatterns = {
    val name = obj.get("name") match {
      case Some(MetaValueElementArray(array)) =>
        Some(unwrapMetaStringArray(array))
      case _ => None
    }
    val klass = obj.get("class") match {
      case Some(MetaValueElementString(value)) => Some(value)
      case _                                   => None
    }
    val tag = obj.get("tag") match {
      case Some(MetaValueElementArray(array)) =>
        Some(unwrapMetaStringArray(array))
      case _ => None
    }
    // Even if all were None, create the IR.IOAttrPatterns object
    // The all none is handled in the native generation
    IR.IOAttrPatterns(IR.PatternsReprObj(name, klass, tag))
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
  private def metaChoicesArrayToIR(array: Vector[MetaValueElement],
                                   womType: WomType): Vector[IR.ChoiceRepr] = {
    if (array.isEmpty) {
      Vector()
    } else {
      array.map {
        case MetaValueElementObject(fields) =>
          if (!fields.contains("value")) {
            throw new Exception("Annotated choice must have a 'value' key")
          } else {
            metaChoiceValueToIR(
                womType = womType,
                value = fields("value"),
                name = fields.get("name")
            )
          }
        case rawElement: MetaValueElement =>
          metaChoiceValueToIR(womType = womType, value = rawElement)
        case _ =>
          throw new Exception(
              "Choices array must contain only raw values or annotated values (hash with "
                + "optional 'name' and required 'value' keys)"
          )
      }
    }
  }

  private def metaChoiceValueToIR(womType: WomType,
                                  value: MetaValueElement,
                                  name: Option[MetaValueElement] = None): IR.ChoiceRepr = {
    (womType, value) match {
      case (WomStringType, MetaValueElementString(str)) =>
        IR.ChoiceReprString(value = str)
      case (WomIntegerType, MetaValueElementInteger(i)) =>
        IR.ChoiceReprInteger(value = i)
      case (WomFloatType, MetaValueElementFloat(f)) =>
        IR.ChoiceReprFloat(value = f)
      case (WomBooleanType, MetaValueElementBoolean(b)) =>
        IR.ChoiceReprBoolean(value = b)
      case (WomSingleFileType, MetaValueElementString(str)) =>
        val nameStr: Option[String] = name match {
          case Some(MetaValueElementString(str)) => Some(str)
          case _                                 => None
        }
        IR.ChoiceReprFile(value = str, name = nameStr)
      case _ =>
        throw new Exception(
            "Choices keyword is only valid for primitive- and file-type parameters, and types must "
              + "match between parameter and choices"
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
  private def metaSuggestionsArrayToIR(array: Vector[MetaValueElement],
                                       womType: WomType): Vector[IR.SuggestionRepr] = {
    if (array.isEmpty) {
      Vector()
    } else {
      array.map {
        case MetaValueElementObject(fields) =>
          metaSuggestionValueToIR(
              womType = womType,
              name = fields.get("name"),
              value = fields.get("value"),
              project = fields.get("project"),
              path = fields.get("path")
          )
        case rawElement: MetaValueElement =>
          metaSuggestionValueToIR(womType = womType, value = Some(rawElement))
        case _ =>
          throw new Exception(
              "Suggestions array must contain only raw values or annotated (hash) values"
          )
      }
    }
  }

  private def metaSuggestionValueToIR(womType: WomType,
                                      value: Option[MetaValueElement],
                                      name: Option[MetaValueElement] = None,
                                      project: Option[MetaValueElement] = None,
                                      path: Option[MetaValueElement] = None): IR.SuggestionRepr = {
    (womType, value) match {
      case (WomStringType, Some(MetaValueElementString(str))) =>
        IR.SuggestionReprString(value = str)
      case (WomIntegerType, Some(MetaValueElementInteger(i))) =>
        IR.SuggestionReprInteger(value = i)
      case (WomFloatType, Some(MetaValueElementFloat(f))) =>
        IR.SuggestionReprFloat(value = f)
      case (WomBooleanType, Some(MetaValueElementBoolean(b))) =>
        IR.SuggestionReprBoolean(value = b)
      case (WomSingleFileType, Some(MetaValueElementString(file))) =>
        createSuggestionFileIR(Some(file), name, project, path)
      case (WomSingleFileType, None) =>
        val s = createSuggestionFileIR(None, name, project, path)
        if (s.project.isEmpty || s.path.isEmpty) {
          throw new Exception(
              "If 'value' is not defined for a file-type suggestion, then both 'project' and 'path' "
                + "must be defined"
          )
        }
        s
      case _ =>
        throw new Exception(
            "Suggestion keyword is only valid for primitive- and file-type parameters, and types "
              + "must match between parameter and suggestions"
        )
    }
  }

  private def createSuggestionFileIR(file: Option[String],
                                     name: Option[MetaValueElement],
                                     project: Option[MetaValueElement],
                                     path: Option[MetaValueElement]): IR.SuggestionReprFile = {
    val nameStr: Option[String] = name match {
      case Some(MetaValueElementString(str)) => Some(str)
      case _                                 => None
    }
    val projectStr: Option[String] = project match {
      case Some(MetaValueElementString(str)) => Some(str)
      case _                                 => None
    }
    val pathStr: Option[String] = path match {
      case Some(MetaValueElementString(str)) => Some(str)
      case _                                 => None
    }
    IR.SuggestionReprFile(file, nameStr, projectStr, pathStr)
  }

  private def metaConstraintToIR(constraint: MetaValueElement): IR.ConstraintRepr = {
    constraint match {
      case MetaValueElementObject(obj: Map[String, MetaValueElement]) =>
        if (obj.size != 1) {
          throw new Exception("Constraint hash must have exactly one 'and' or 'or' key")
        }
        obj.head match {
          case (IR.PARAM_META_CONSTRAINT_AND, MetaValueElementArray(array)) =>
            IR.ConstraintReprOper(ConstraintOper.AND, array.map(metaConstraintToIR))
          case (IR.PARAM_META_CONSTRAINT_OR, MetaValueElementArray(array)) =>
            IR.ConstraintReprOper(ConstraintOper.OR, array.map(metaConstraintToIR))
          case _ =>
            throw new Exception(
                "Constraint must have key 'and' or 'or' and an array value"
            )
        }
      case MetaValueElementString(s) => IR.ConstraintReprString(s)
      case _                         => throw new Exception("'dx_type' constraints must be either strings or hashes")
    }
  }

  private def metaDefaultToIR(value: MetaValueElement, womType: WomType): IR.DefaultRepr = {
    (womType, value) match {
      case (WomStringType, MetaValueElementString(str)) =>
        IR.DefaultReprString(value = str)
      case (WomIntegerType, MetaValueElementInteger(i)) =>
        IR.DefaultReprInteger(value = i)
      case (WomFloatType, MetaValueElementFloat(f)) =>
        IR.DefaultReprFloat(value = f)
      case (WomBooleanType, MetaValueElementBoolean(b)) =>
        IR.DefaultReprBoolean(value = b)
      case (WomSingleFileType, MetaValueElementString(file)) =>
        IR.DefaultReprFile(value = file)
      case (womArrayType: WomArrayType, MetaValueElementArray(array)) =>
        def helper(wt: WomType)(mv: MetaValueElement) = metaDefaultToIR(mv, wt)
        IR.DefaultReprArray(array.map(helper(womArrayType.memberType)))
      case _ =>
        throw new Exception(
            "Default keyword is only valid for primitive-, file-, and array-type parameters, and "
              + "types must match between parameter and default"
        )
    }
  }

  // Extract the parameter_meta info from the WOM structure
  // The parameter's WomType is passed in since some parameter metadata values are required to
  // have the same type as the parameter.
  def unwrap(paramMeta: Option[MetaValueElement], womType: WomType): Option[Vector[IR.IOAttr]] = {
    paramMeta match {
      case None => None
      // If the parameter metadata is a string, treat it as help
      case Some(MetaValueElementString(text)) => Some(Vector(IR.IOAttrHelp(text)))
      case Some(MetaValueElementObject(obj)) => {
        // Whether to use 'description' in place of help
        val noHelp = !obj.contains(IR.PARAM_META_HELP)
        // Use flatmap to get the parameter metadata keys if they exist
        Some(obj.flatMap {
          case (IR.PARAM_META_GROUP, MetaValueElementString(text)) => Some(IR.IOAttrGroup(text))
          case (IR.PARAM_META_HELP, MetaValueElementString(text))  => Some(IR.IOAttrHelp(text))
          // Use 'description' in place of 'help' if the former is present and the latter is not
          case (IR.PARAM_META_DESCRIPTION, MetaValueElementString(text)) if noHelp =>
            Some(IR.IOAttrHelp(text))
          case (IR.PARAM_META_LABEL, MetaValueElementString(text)) => Some(IR.IOAttrLabel(text))
          // Try to parse the patterns key
          // First see if it's an array
          case (IR.PARAM_META_PATTERNS, MetaValueElementArray(array)) =>
            Some(IR.IOAttrPatterns(IR.PatternsReprArray(unwrapMetaStringArray(array))))
          // See if it's an object, and if it is, parse out the optional key, class, and tag keys
          // Note all three are optional
          case (IR.PARAM_META_PATTERNS, MetaValueElementObject(obj)) =>
            Some(metaPatternsObjToIR(obj))
          // Try to parse the choices key, which will be an array of either values or objects
          case (IR.PARAM_META_CHOICES, MetaValueElementArray(array)) =>
            val wt = unwrapWomArrayType(womType)
            Some(IR.IOAttrChoices(metaChoicesArrayToIR(array, wt)))
          case (IR.PARAM_META_SUGGESTIONS, MetaValueElementArray(array)) =>
            val wt = unwrapWomArrayType(womType)
            Some(IR.IOAttrSuggestions(metaSuggestionsArrayToIR(array, wt)))
          case (IR.PARAM_META_TYPE, dx_type: MetaValueElement) =>
            val wt = unwrapWomArrayType(womType)
            wt match {
              case WomSingleFileType => Some(IR.IOAttrType(metaConstraintToIR(dx_type)))
              case _                 => throw new Exception("'dx_type' can only be specified for File parameters")
            }
          case (IR.PARAM_META_DEFAULT, default: MetaValueElement) =>
            Some(IR.IOAttrDefault(metaDefaultToIR(default, womType)))
          case _ => None
        }.toVector)
      }
      case _ => None // TODO: or throw exception?
    }
  }
}
