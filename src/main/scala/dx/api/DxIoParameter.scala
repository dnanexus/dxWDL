package dx.api

import spray.json._

object DxIOClass extends Enumeration {
  type DxIOClass = Value
  val INT, FLOAT, STRING, BOOLEAN, FILE, ARRAY_OF_INTS, ARRAY_OF_FLOATS, ARRAY_OF_STRINGS,
      ARRAY_OF_BOOLEANS, ARRAY_OF_FILES, HASH, OTHER = Value

  def fromString(s: String): DxIOClass.Value = {
    s match {
      // primitives
      case "int"     => INT
      case "float"   => FLOAT
      case "string"  => STRING
      case "boolean" => BOOLEAN
      case "file"    => FILE

      // arrays of primitives
      case "array:int"     => ARRAY_OF_INTS
      case "array:float"   => ARRAY_OF_FLOATS
      case "array:string"  => ARRAY_OF_STRINGS
      case "array:boolean" => ARRAY_OF_BOOLEANS
      case "array:file"    => ARRAY_OF_FILES

      // hash
      case "hash" => HASH

      // we don't deal with anything else
      case _ => OTHER
    }
  }
}

object DxIOSpec {
  val NAME = "name"
  val CLASS = "class"
  val OPTIONAL = "optional"
  val DEFAULT = "default"
  val CHOICES = "choices"
  val GROUP = "group"
  val HELP = "help"
  val LABEL = "label"
  val PATTERNS = "patterns"
  val SUGGESTIONS = "suggestions"
  val TYPE = "type"
}

// Types for the IO spec pattern section
sealed abstract class IOParameterPattern
case class IOParameterPatternArray(patterns: Vector[String]) extends IOParameterPattern
case class IOParameterPatternObject(name: Option[Vector[String]],
                                    klass: Option[String],
                                    tag: Option[Vector[String]])
    extends IOParameterPattern

// Types for the IO choices section
sealed abstract class IOParameterChoice
final case class IOParameterChoiceString(value: String) extends IOParameterChoice
final case class IOParameterChoiceNumber(value: BigDecimal) extends IOParameterChoice
final case class IOParameterChoiceBoolean(value: Boolean) extends IOParameterChoice
final case class IOParameterChoiceFile(value: DxFile, name: Option[String])
    extends IOParameterChoice

// Types for the IO suggestions section
sealed abstract class IOParameterSuggestion
final case class IOParameterSuggestionString(value: String) extends IOParameterSuggestion
final case class IOParameterSuggestionNumber(value: BigDecimal) extends IOParameterSuggestion
final case class IOParameterSuggestionBoolean(value: Boolean) extends IOParameterSuggestion
final case class IOParameterSuggestionFile(name: Option[String],
                                           value: Option[DxFile],
                                           project: Option[DxProject],
                                           path: Option[String])
    extends IOParameterSuggestion

// Types for the IO 'type' section
object DxConstraint {
  val AND = "$and"
  val OR = "$or"
}

object ConstraintOper extends Enumeration {
  type ConstraintOper = Value
  val AND, OR = Value
}

sealed abstract class IOParameterTypeConstraint
sealed case class IOParameterTypeConstraintString(constraint: String)
    extends IOParameterTypeConstraint
sealed case class IOParameterTypeConstraintOper(oper: ConstraintOper.Value,
                                                constraints: Vector[IOParameterTypeConstraint])
    extends IOParameterTypeConstraint

// Types for the IO 'default' section
sealed abstract class IOParameterDefault
final case class IOParameterDefaultString(value: String) extends IOParameterDefault
final case class IOParameterDefaultNumber(value: BigDecimal) extends IOParameterDefault
final case class IOParameterDefaultBoolean(value: Boolean) extends IOParameterDefault
final case class IOParameterDefaultFile(value: DxFile) extends IOParameterDefault
final case class IOParameterDefaultArray(array: Vector[IOParameterDefault])
    extends IOParameterDefault

// Representation of the IO spec
case class IOParameter(
    name: String,
    ioClass: DxIOClass.Value,
    optional: Boolean,
    group: Option[String] = None,
    help: Option[String] = None,
    label: Option[String] = None,
    patterns: Option[IOParameterPattern] = None,
    choices: Option[Vector[IOParameterChoice]] = None,
    suggestions: Option[Vector[IOParameterSuggestion]] = None,
    dx_type: Option[IOParameterTypeConstraint] = None,
    default: Option[IOParameterDefault] = None
)

object IOParameter {
  def parseIoParam(dxApi: DxApi, jsv: JsValue): IOParameter = {
    val ioParam = jsv.asJsObject.getFields(DxIOSpec.NAME, DxIOSpec.CLASS) match {
      case Seq(JsString(name), JsString(klass)) =>
        val ioClass = DxIOClass.fromString(klass)
        IOParameter(name, ioClass, optional = false)
      case other =>
        throw new Exception(s"Malformed io spec ${other}")
    }

    val optFlag = jsv.asJsObject.fields.get(DxIOSpec.OPTIONAL) match {
      case Some(JsBoolean(b)) => b
      case None               => false
    }

    val group = jsv.asJsObject.fields.get(DxIOSpec.GROUP) match {
      case Some(JsString(s)) => Some(s)
      case _                 => None
    }

    val help = jsv.asJsObject.fields.get(DxIOSpec.HELP) match {
      case Some(JsString(s)) => Some(s)
      case _                 => None
    }

    val label = jsv.asJsObject.fields.get(DxIOSpec.LABEL) match {
      case Some(JsString(s)) => Some(s)
      case _                 => None
    }

    val patterns = jsv.asJsObject.fields.get(DxIOSpec.PATTERNS) match {
      case Some(JsArray(a)) =>
        Some(IOParameterPatternArray(a.flatMap {
          case JsString(s) => Some(s)
          case _           => None
        }))
      case Some(JsObject(obj)) =>
        val name = obj.get("name") match {
          case Some(JsArray(array)) =>
            Some(array.flatMap {
              case JsString(s) => Some(s)
              case _           => None
            })
          case _ => None
        }
        val tag = obj.get("tag") match {
          case Some(JsArray(array)) =>
            Some(array.flatMap {
              case JsString(s) => Some(s)
              case _           => None
            })
          case _ =>
            None
        }
        val klass = obj.get("class") match {
          case Some(JsString(s)) => Some(s)
          case _                 => None
        }
        Some(IOParameterPatternObject(name, klass, tag))
      case _ => None
    }

    val choices = jsv.asJsObject.fields.get(DxIOSpec.CHOICES) match {
      case Some(JsArray(a)) =>
        Some(a.map {
          case JsObject(fields) =>
            val nameStr: Option[String] = fields.get("name") match {
              case Some(JsString(s)) => Some(s)
              case _                 => None
            }
            IOParameterChoiceFile(name = nameStr,
                                  value = DxFile.fromJsValue(dxApi, fields("value")))
          case JsString(s)  => IOParameterChoiceString(s)
          case JsNumber(n)  => IOParameterChoiceNumber(n)
          case JsBoolean(b) => IOParameterChoiceBoolean(b)
          case _            => throw new Exception("Unsupported choice value")
        })
      case _ => None
    }

    val suggestions = jsv.asJsObject.fields.get(DxIOSpec.SUGGESTIONS) match {
      case Some(JsArray(a)) =>
        Some(a.map {
          case JsObject(fields) =>
            val name: Option[String] = fields.get("name") match {
              case Some(JsString(s)) => Some(s)
              case _                 => None
            }
            val value: Option[DxFile] = fields.get("value") match {
              case Some(v: JsValue) => Some(DxFile.fromJsValue(dxApi, v))
              case _                => None
            }
            val project: Option[DxProject] = fields.get("project") match {
              case Some(JsString(p)) => Some(DxProject(dxApi, p))
              case _                 => None
            }
            val path: Option[String] = fields.get("path") match {
              case Some(JsString(s)) => Some(s)
              case _                 => None
            }
            IOParameterSuggestionFile(name, value, project, path)
          case JsString(s)  => IOParameterSuggestionString(s)
          case JsNumber(n)  => IOParameterSuggestionNumber(n)
          case JsBoolean(b) => IOParameterSuggestionBoolean(b)
          case _            => throw new Exception("Unsupported suggestion value")
        })
      case _ => None
    }

    val dx_type = jsv.asJsObject.fields.get(DxIOSpec.TYPE) match {
      case Some(v: JsValue) => Some(ioParamTypeFromJs(v))
      case _                => None
    }

    val default = jsv.asJsObject.fields.get(DxIOSpec.DEFAULT) match {
      case Some(v: JsValue) =>
        try {
          Some(ioParamDefaultFromJs(dxApi, v))
        } catch {
          // Currently, some valid defaults won't parse, so we ignore them for now
          case _: Exception => None
        }
      case _ => None
    }

    ioParam.copy(
        optional = optFlag,
        group = group,
        help = help,
        label = label,
        patterns = patterns,
        choices = choices,
        suggestions = suggestions,
        dx_type = dx_type,
        default = default
    )
  }

  def ioParamTypeFromJs(value: JsValue): IOParameterTypeConstraint = {
    value match {
      case JsString(s) => IOParameterTypeConstraintString(s)
      case JsObject(fields) =>
        if (fields.size != 1) {
          throw new Exception("Constraint hash must have exactly one '$and' or '$or' key")
        }
        fields.head match {
          case (DxConstraint.AND, JsArray(array)) =>
            IOParameterTypeConstraintOper(ConstraintOper.AND, array.map(ioParamTypeFromJs))
          case (DxConstraint.OR, JsArray(array)) =>
            IOParameterTypeConstraintOper(ConstraintOper.OR, array.map(ioParamTypeFromJs))
          case _ =>
            throw new Exception(
                "Constraint must have key '$and' or '$or' and an array value"
            )
        }
      case _ => throw new Exception(s"Invalid paramter type value ${value}")
    }
  }

  def ioParamDefaultFromJs(dxApi: DxApi, value: JsValue): IOParameterDefault = {
    value match {
      case JsString(s)       => IOParameterDefaultString(s)
      case JsNumber(n)       => IOParameterDefaultNumber(n)
      case JsBoolean(b)      => IOParameterDefaultBoolean(b)
      case fileObj: JsObject => IOParameterDefaultFile(DxFile.fromJsValue(dxApi, fileObj))
      case JsArray(array) =>
        IOParameterDefaultArray(array.map(value => ioParamDefaultFromJs(dxApi, value)))
      case other => throw new Exception(s"Unsupported default value type ${other}")
    }
  }

  def parseIOSpec(dxApi: DxApi, specs: Vector[JsValue]): Vector[IOParameter] = {
    specs.map(ioSpec => parseIoParam(dxApi, ioSpec))
  }
}
