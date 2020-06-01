package dxWDL.compiler

import wdlTools.types.WdlTypes
import wdlTools.types.{TypedAbstractSyntax => TAT}

import dxWDL.dx._

object ParameterMeta {

  // Convert a WOM Vector[MetaValue] of Strings into a Vector of Strings
  // Anything not a string will be filtered out.
  private def unwrapMetaStringArray(array: Vector[TAT.MetaValue]): Vector[String] = {
    array.flatMap {
      case TAT.MetaValueString(str, _) => Some(str)
      case _                           => None
    }.toVector
  }

  // strip muli-layer array types and get the member
  //
  // examples:
  // input             output
  // Array[T]          T
  // Array[Array[T]]   T
  private def unwrapWomArrayType(womType: WdlTypes.T): WdlTypes.T = {
    womType match {
      case WdlTypes.T_Array(t, _) => unwrapWomArrayType(t)
      case x                      => x
    }
  }

  // Convert a patterns WOM object value to IR
  private def metaPatternsObjToIR(obj: Map[String, TAT.MetaValue]): IR.IOAttrPatterns = {
    val name = obj.get("name") match {
      case Some(TAT.MetaValueArray(array, _)) =>
        Some(unwrapMetaStringArray(array))
      case _ => None
    }
    val klass = obj.get("class") match {
      case Some(TAT.MetaValueString(value, _)) => Some(value)
      case _                                   => None
    }
    val tag = obj.get("tag") match {
      case Some(TAT.MetaValueArray(array, _)) =>
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
        case TAT.MetaValueObject(fields, _) =>
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
      case (WdlTypes.T_String, TAT.MetaValueString(str, _)) =>
        IR.ChoiceReprString(value = str)
      case (WdlTypes.T_Int, TAT.MetaValueInt(i, _)) =>
        IR.ChoiceReprInteger(value = i)
      case (WdlTypes.T_Float, TAT.MetaValueFloat(f, _)) =>
        IR.ChoiceReprFloat(value = f)
      case (WdlTypes.T_Boolean, TAT.MetaValueBoolean(b, _)) =>
        IR.ChoiceReprBoolean(value = b)
      case (WdlTypes.T_File, TAT.MetaValueString(str, _)) =>
        val nameStr: Option[String] = name match {
          case Some(TAT.MetaValueString(str, _)) => Some(str)
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
        case TAT.MetaValueObject(fields, _) =>
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
      case (WdlTypes.T_String, Some(TAT.MetaValueString(str, _))) =>
        IR.SuggestionReprString(value = str)
      case (WdlTypes.T_Int, Some(TAT.MetaValueInt(i, _))) =>
        IR.SuggestionReprInteger(value = i)
      case (WdlTypes.T_Float, Some(TAT.MetaValueFloat(f, _))) =>
        IR.SuggestionReprFloat(value = f)
      case (WdlTypes.T_Boolean, Some(TAT.MetaValueBoolean(b, _))) =>
        IR.SuggestionReprBoolean(value = b)
      case (WdlTypes.T_File, Some(TAT.MetaValueString(file, _))) =>
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
      case Some(TAT.MetaValueString(str, _)) => Some(str)
      case _                                 => None
    }
    val projectStr: Option[String] = project match {
      case Some(TAT.MetaValueString(str, _)) => Some(str)
      case _                                 => None
    }
    val pathStr: Option[String] = path match {
      case Some(TAT.MetaValueString(str, _)) => Some(str)
      case _                                 => None
    }
    IR.SuggestionReprFile(file, nameStr, projectStr, pathStr)
  }

  private def metaConstraintToIR(constraint: TAT.MetaValue): IR.ConstraintRepr = {
    constraint match {
      case TAT.MetaValueObject(obj: Map[String, TAT.MetaValue], _) =>
        if (obj.size != 1) {
          throw new Exception("Constraint hash must have exactly one 'and' or 'or' key")
        }
        obj.head match {
          case (IR.PARAM_META_CONSTRAINT_AND, TAT.MetaValueArray(array, _)) =>
            IR.ConstraintReprOper(ConstraintOper.AND, array.map(metaConstraintToIR))
          case (IR.PARAM_META_CONSTRAINT_OR, TAT.MetaValueArray(array, _)) =>
            IR.ConstraintReprOper(ConstraintOper.OR, array.map(metaConstraintToIR))
          case _ =>
            throw new Exception(
                "Constraint must have key 'and' or 'or' and an array value"
            )
        }
      case TAT.MetaValueString(s, _) => IR.ConstraintReprString(s)
      case _                         => throw new Exception("'dx_type' constraints must be either strings or hashes")
    }
  }

  private def metaDefaultToIR(value: TAT.MetaValue, womType: WdlTypes.T): IR.DefaultRepr = {
    (womType, value) match {
      case (WdlTypes.T_String, TAT.MetaValueString(str, _)) =>
        IR.DefaultReprString(value = str)
      case (WdlTypes.T_Int, TAT.MetaValueInt(i, _)) =>
        IR.DefaultReprInteger(value = i)
      case (WdlTypes.T_Float, TAT.MetaValueFloat(f, _)) =>
        IR.DefaultReprFloat(value = f)
      case (WdlTypes.T_Boolean, TAT.MetaValueBoolean(b, _)) =>
        IR.DefaultReprBoolean(value = b)
      case (WdlTypes.T_File, TAT.MetaValueString(file, _)) =>
        IR.DefaultReprFile(value = file)
      case (WdlTypes.T_Array(t, _), TAT.MetaValueArray(array, _)) =>
        def helper(mv: TAT.MetaValue) = metaDefaultToIR(mv, t)
        IR.DefaultReprArray(array.map(helper))
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
      case Some(TAT.MetaValueString(text, _)) => Some(Vector(IR.IOAttrHelp(text)))
      case Some(TAT.MetaValueObject(obj, _)) => {
        // Whether to use 'description' in place of help
        val noHelp = !obj.contains(IR.PARAM_META_HELP)
        // Use flatmap to get the parameter metadata keys if they exist
        Some(obj.flatMap {
          case (IR.PARAM_META_GROUP, TAT.MetaValueString(text, _)) => Some(IR.IOAttrGroup(text))
          case (IR.PARAM_META_HELP, TAT.MetaValueString(text, _))  => Some(IR.IOAttrHelp(text))
          // Use 'description' in place of 'help' if the former is present and the latter is not
          case (IR.PARAM_META_DESCRIPTION, TAT.MetaValueString(text, _)) if noHelp =>
            Some(IR.IOAttrHelp(text))
          case (IR.PARAM_META_LABEL, TAT.MetaValueString(text, _)) => Some(IR.IOAttrLabel(text))
          // Try to parse the patterns key
          // First see if it's an array
          case (IR.PARAM_META_PATTERNS, TAT.MetaValueArray(array, _)) =>
            Some(IR.IOAttrPatterns(IR.PatternsReprArray(unwrapMetaStringArray(array))))
          // See if it's an object, and if it is, parse out the optional key, class, and tag keys
          // Note all three are optional
          case (IR.PARAM_META_PATTERNS, TAT.MetaValueObject(obj, _)) =>
            Some(metaPatternsObjToIR(obj))
          // Try to parse the choices key, which will be an array of either values or objects
          case (IR.PARAM_META_CHOICES, TAT.MetaValueArray(array, _)) =>
            val wt = unwrapWomArrayType(womType)
            Some(IR.IOAttrChoices(metaChoicesArrayToIR(array, wt)))
          case (IR.PARAM_META_SUGGESTIONS, TAT.MetaValueArray(array, _)) =>
            val wt = unwrapWomArrayType(womType)
            Some(IR.IOAttrSuggestions(metaSuggestionsArrayToIR(array, wt)))
          case (IR.PARAM_META_TYPE, dx_type: TAT.MetaValue) =>
            val wt = unwrapWomArrayType(womType)
            wt match {
              case WdlTypes.T_File => Some(IR.IOAttrType(metaConstraintToIR(dx_type)))
              case _               => throw new Exception("'dx_type' can only be specified for File parameters")
            }
          case (IR.PARAM_META_DEFAULT, default: TAT.MetaValue) =>
            Some(IR.IOAttrDefault(metaDefaultToIR(default, womType)))
          case _ => None
        }.toVector)
      }
      case _ => None // TODO: or throw exception?
    }
  }

  // strip the source text token from meta values
  def translateMetaValue(mv: TAT.MetaValue): IR.MetaValue = {
    mv match {
      case TAT.MetaValueNull(_)           => IR.MetaValueNull
      case TAT.MetaValueBoolean(value, _) => IR.MetaValueBoolean(value)
      case TAT.MetaValueInt(value, _)     => IR.MetaValueInt(value)
      case TAT.MetaValueFloat(value, _)   => IR.MetaValueFloat(value)
      case TAT.MetaValueString(value, _)  => IR.MetaValueString(value)
      case TAT.MetaValueObject(value, _) =>
        IR.MetaValueObject(value.map {
          case (k, v) => k -> translateMetaValue(v)
        })
      case TAT.MetaValueArray(value, _) =>
        IR.MetaValueArray(value.map {
          case v => translateMetaValue(v)
        }.toVector)
    }
  }

  def translateMetaKVs(kvs: Map[String, TAT.MetaValue]): Map[String, IR.MetaValue] = {
    kvs.map {
      case (k, v) => k -> translateMetaValue(v)
    }.toMap
  }
}
