package dxWDL.compiler

import wdlTools.types.WdlTypes._
import wdlTools.types.{TypedAbstractSyntax => TAT}

import dxWDL.dx._

object ParameterMeta {
  // Convert a WOM Vector[MetaValue] of Strings into a Vector of Strings
  // Anything not a string will be filtered out.
  private def unwrapMetaStringArray(array: Vector[TAT.MetaValue]): Vector[String] = {
    array.flatMap {
      case MetaValueString(str) => Some(str)
      case _                           => None
    }.toVector
  }

  private def unwrapWomArrayType(womType: WomTypes.T): WomTypes.T = {
    var wt = womType
    while (wt.isInstanceOf[WdlTypes.T_Array]) {
      wt = wt.asInstanceOf[WdlTypes.T_Array].memberType
    }
    wt
  }

  // Convert a patterns WOM object value to IR
  private def metaPatternsObjToIR(obj: Map[String, TAT.MetaValue]): IR.IOAttrPatterns = {
    val name = obj.get("name") match {
      case Some(TAT.MetaValueArray(array)) =>
        Some(unwrapMetaStringArray(array))
      case _ => None
    }
    val klass = obj.get("class") match {
      case Some(TAT.MetaValueString(value)) => Some(value)
      case _                                   => None
    }
    val tag = obj.get("tag") match {
      case Some(TAT.MetaValueArray(array)) =>
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
  private def metaChoicesArrayToIR(array: Vector[TAT.MetaValue],
                                   womType: WdlTypes.T): Vector[IR.ChoiceRepr] = {
    if (array.isEmpty) {
      Vector()
    } else {
      array.map {
        case TAT.MetaValueObject(fields) =>
          if (!fields.contains("value")) {
            throw new Exception("Annotated choice must have a 'value' key")
          } else {
            metaChoiceValueToIR(
                womType = womType,
                value = fields("value"),
                name = fields.get("name")
            )
          }
        case rawElement: TAT.MetaValue =>
          metaChoiceValueToIR(womType = womType, value = rawElement)
        case _ =>
          throw new Exception(
              "Choices array must contain only raw values or annotated values (hash with "
                + "optional 'name' and required 'value' keys)"
          )
      }
    }
  }

  private def metaChoiceValueToIR(womType: WdlTypes.T,
                                  value: TAT.MetaValue,
                                  name: Option[TAT.MetaValue] = None): IR.ChoiceRepr = {
    (womType, value) match {
      case (WdlTypes.T_String, TAT.MetaValueString(str)) =>
        IR.ChoiceReprString(value = str)
      case (WdlTypes.T_Int, TAT.MetaValueInteger(i)) =>
        IR.ChoiceReprInteger(value = i)
      case (WdlTypes.T_Float, TAT.MetaValueFloat(f)) =>
        IR.ChoiceReprFloat(value = f)
      case (WdlTypes.T_Boolean, TAT.MetaValueBoolean(b)) =>
        IR.ChoiceReprBoolean(value = b)
      case (WdlTypes.T_File, TAT.MetaValueString(str)) =>
        val nameStr: Option[String] = name match {
          case Some(TAT.MetaValueString(str)) => Some(str)
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
  private def metaSuggestionsArrayToIR(array: Vector[TAT.MetaValue],
                                       womType: WdlTypes.T): Vector[IR.SuggestionRepr] = {
    if (array.isEmpty) {
      Vector()
    } else {
      array.map {
        case TAT.MetaValueObject(fields) =>
          metaSuggestionValueToIR(
              womType = womType,
              name = fields.get("name"),
              value = fields.get("value"),
              project = fields.get("project"),
              path = fields.get("path")
          )
        case rawElement: TAT.MetaValue =>
          metaSuggestionValueToIR(womType = womType, value = Some(rawElement))
        case _ =>
          throw new Exception(
              "Suggestions array must contain only raw values or annotated (hash) values"
          )
      }
    }
  }

  private def metaSuggestionValueToIR(womType: WdlTypes.T,
                                      value: Option[TAT.MetaValue],
                                      name: Option[TAT.MetaValue] = None,
                                      project: Option[TAT.MetaValue] = None,
                                      path: Option[TAT.MetaValue] = None): IR.SuggestionRepr = {
    (womType, value) match {
      case (WdlTypes.T_String, Some(TAT.MetaValueString(str))) =>
        IR.SuggestionReprString(value = str)
      case (WdlTypes.T_Int, Some(TAT.MetaValueInteger(i))) =>
        IR.SuggestionReprInteger(value = i)
      case (WdlTypes.T_Float, Some(TAT.MetaValueFloat(f))) =>
        IR.SuggestionReprFloat(value = f)
      case (WdlTypes.T_Boolean, Some(TAT.MetaValueBoolean(b))) =>
        IR.SuggestionReprBoolean(value = b)
      case (WdlTypes.T_File, Some(TAT.MetaValueString(file))) =>
        createSuggestionFileIR(Some(file), name, project, path)
      case (WdlTypes.T_File, None) =>
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
                                     name: Option[TAT.MetaValue],
                                     project: Option[TAT.MetaValue],
                                     path: Option[TAT.MetaValue]): IR.SuggestionReprFile = {
    val nameStr: Option[String] = name match {
      case Some(TAT.MetaValueString(str)) => Some(str)
      case _                                 => None
    }
    val projectStr: Option[String] = project match {
      case Some(TAT.MetaValueString(str)) => Some(str)
      case _                                 => None
    }
    val pathStr: Option[String] = path match {
      case Some(TAT.MetaValueString(str)) => Some(str)
      case _                                 => None
    }
    IR.SuggestionReprFile(file, nameStr, projectStr, pathStr)
  }

  private def metaConstraintToIR(constraint: TAT.MetaValue): IR.ConstraintRepr = {
    constraint match {
      case TAT.MetaValueObject(obj: Map[String, TAT.MetaValue]) =>
        if (obj.size != 1) {
          throw new Exception("Constraint hash must have exactly one 'and' or 'or' key")
        }
        obj.head match {
          case (IR.PARAM_META_CONSTRAINT_AND, TAT.MetaValueArray(array)) =>
            IR.ConstraintReprOper(ConstraintOper.AND, array.map(metaConstraintToIR))
          case (IR.PARAM_META_CONSTRAINT_OR, TAT.MetaValueArray(array)) =>
            IR.ConstraintReprOper(ConstraintOper.OR, array.map(metaConstraintToIR))
          case _ =>
            throw new Exception(
                "Constraint must have key 'and' or 'or' and an array value"
            )
        }
      case TAT.MetaValueString(s) => IR.ConstraintReprString(s)
      case _                         => throw new Exception("'dx_type' constraints must be either strings or hashes")
    }
  }

  private def metaDefaultToIR(value: TAT.MetaValue, womType: WdlTypes.T): IR.DefaultRepr = {
    (womType, value) match {
      case (WdlTypes.T_String, TAT.MetaValueString(str)) =>
        IR.DefaultReprString(value = str)
      case (WdlTypes.T_Int, TAT.MetaValueInteger(i)) =>
        IR.DefaultReprInteger(value = i)
      case (WdlTypes.T_Float, TAT.MetaValueFloat(f)) =>
        IR.DefaultReprFloat(value = f)
      case (WdlTypes.T_Boolean, TAT.MetaValueBoolean(b)) =>
        IR.DefaultReprBoolean(value = b)
      case (WdlTypes.T_File, TAT.MetaValueString(file)) =>
        IR.DefaultReprFile(value = file)
      case (womArrayType: WdlTypes.T_Array, TAT.MetaValueArray(array)) =>
        def helper(wt: WdlTypes.T)(mv: TAT.MetaValue) = metaDefaultToIR(mv, wt)
        IR.DefaultReprArray(array.map(helper(womArrayType.memberType)))
      case _ =>
        throw new Exception(
            "Default keyword is only valid for primitive-, file-, and array-type parameters, and "
              + "types must match between parameter and default"
        )
    }
  }

  // Extract the parameter_meta info from the WOM structure
  // The parameter's WdlTypes.T is passed in since some parameter metadata values are required to
  // have the same type as the parameter.
  def unwrap(paramMeta: Option[TAT.MetaValue], womType: WdlTypes.T): Option[Vector[IR.IOAttr]] = {
    paramMeta match {
      case None => None
      // If the parameter metadata is a string, treat it as help
      case Some(TAT.MetaValueString(text)) => Some(Vector(IR.IOAttrHelp(text)))
      case Some(TAT.MetaValueObject(obj)) => {
        // Whether to use 'description' in place of help
        val noHelp = !obj.contains(IR.PARAM_META_HELP)
        // Use flatmap to get the parameter metadata keys if they exist
        Some(obj.flatMap {
          case (IR.PARAM_META_GROUP, TAT.MetaValueString(text)) => Some(IR.IOAttrGroup(text))
          case (IR.PARAM_META_HELP, TAT.MetaValueString(text))  => Some(IR.IOAttrHelp(text))
          // Use 'description' in place of 'help' if the former is present and the latter is not
          case (IR.PARAM_META_DESCRIPTION, TAT.MetaValueString(text)) if noHelp =>
            Some(IR.IOAttrHelp(text))
          case (IR.PARAM_META_LABEL, TAT.MetaValueString(text)) => Some(IR.IOAttrLabel(text))
          // Try to parse the patterns key
          // First see if it's an array
          case (IR.PARAM_META_PATTERNS, TAT.MetaValueArray(array)) =>
            Some(IR.IOAttrPatterns(IR.PatternsReprArray(unwrapMetaStringArray(array))))
          // See if it's an object, and if it is, parse out the optional key, class, and tag keys
          // Note all three are optional
          case (IR.PARAM_META_PATTERNS, TAT.MetaValueObject(obj)) =>
            Some(metaPatternsObjToIR(obj))
          // Try to parse the choices key, which will be an array of either values or objects
          case (IR.PARAM_META_CHOICES, TAT.MetaValueArray(array)) =>
            val wt = unwrapWdlTypes.T_Array(womType)
            Some(IR.IOAttrChoices(metaChoicesArrayToIR(array, wt)))
          case (IR.PARAM_META_SUGGESTIONS, TAT.MetaValueArray(array)) =>
            val wt = unwrapWdlTypes.T_Array(womType)
            Some(IR.IOAttrSuggestions(metaSuggestionsArrayToIR(array, wt)))
          case (IR.PARAM_META_TYPE, dx_type: TAT.MetaValue) =>
            val wt = unwrapWdlTypes.T_Array(womType)
            wt match {
              case WdlTypes.T_File => Some(IR.IOAttrType(metaConstraintToIR(dx_type)))
              case _                 => throw new Exception("'dx_type' can only be specified for File parameters")
            }
          case (IR.PARAM_META_DEFAULT, default: TAT.MetaValue) =>
            Some(IR.IOAttrDefault(metaDefaultToIR(default, womType)))
          case _ => None
        }.toVector)
      }
      case _ => None // TODO: or throw exception?
    }
  }
}
