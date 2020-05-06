package wdlTools.eval

import java.net.URL
import wdlTools.eval.WdlValues._
import wdlTools.syntax.TextSource
import wdlTools.types.WdlTypes

case class Coercion(docSourceUrl: Option[URL]) {

  private def coerceToStruct(structName: String,
                             memberDefs: Map[String, WdlTypes.WT],
                             members: Map[String, WV],
                             text: TextSource): WV_Struct = {
    if (memberDefs.keys.toSet != members.keys.toSet)
      throw new EvalException(s"struct ${structName} has wrong fields", text, docSourceUrl)

    // coerce each member to the struct type
    val memValues: Map[String, WV] = memberDefs.map {
      case (key, t) =>
        val memVal = coerceTo(t, members(key), text)
        key -> memVal
    }

    WV_Struct(structName, memValues)
  }

  def coerceTo(wdlType: WdlTypes.WT, value: WV, text: TextSource): WV = {
    (wdlType, value) match {
      // primitive types
      case (WdlTypes.WT_Boolean, WV_Boolean(_)) => value
      case (WdlTypes.WT_Int, WV_Int(_))         => value
      case (WdlTypes.WT_Int, WV_Float(x))       => WV_Int(x.toInt)
      case (WdlTypes.WT_Int, WV_String(s)) =>
        val n =
          try {
            s.toInt
          } catch {
            case _: NumberFormatException =>
              throw new EvalException(s"string ${s} cannot be converted into an integer",
                                      text,
                                      docSourceUrl)
          }
        WV_Int(n)
      case (WdlTypes.WT_Float, WV_Int(n))   => WV_Float(n.toFloat)
      case (WdlTypes.WT_Float, WV_Float(_)) => value
      case (WdlTypes.WT_Float, WV_String(s)) =>
        val x =
          try {
            s.toDouble
          } catch {
            case _: NumberFormatException =>
              throw new EvalException(s"string ${s} cannot be converted into an integer",
                                      text,
                                      docSourceUrl)
          }
        WV_Float(x)
      case (WdlTypes.WT_String, WV_Boolean(b)) => WV_String(b.toString)
      case (WdlTypes.WT_String, WV_Int(n))     => WV_String(n.toString)
      case (WdlTypes.WT_String, WV_Float(x))   => WV_String(x.toString)
      case (WdlTypes.WT_String, WV_String(_))  => value
      case (WdlTypes.WT_String, WV_File(s))    => WV_String(s)
      case (WdlTypes.WT_File, WV_String(s))    => WV_File(s)
      case (WdlTypes.WT_File, WV_File(_))      => value

      // compound types
      // recursively descend into the sub structures and coerce them.
      case (WdlTypes.WT_Optional(t), WV_Optional(v)) =>
        WV_Optional(coerceTo(t, v, text))
      case (WdlTypes.WT_Optional(t), v) =>
        WV_Optional(coerceTo(t, v, text))
      case (t, WV_Optional(v)) =>
        coerceTo(t, v, text)

      case (WdlTypes.WT_Array(t), WV_Array(vec)) =>
//        if (nonEmpty && vec.isEmpty)
//          throw new EvalException("array is empty", text, docSourceURL)
        WV_Array(vec.map { x =>
          coerceTo(t, x, text)
        })

      case (WdlTypes.WT_Map(kt, vt), WV_Map(m)) =>
        WV_Map(m.map {
          case (k, v) =>
            coerceTo(kt, k, text) -> coerceTo(vt, v, text)
        })
      case (WdlTypes.WT_Pair(lt, rt), WV_Pair(l, r)) =>
        WV_Pair(coerceTo(lt, l, text), coerceTo(rt, r, text))

      case (WdlTypes.WT_Struct(name1, _), WV_Struct(name2, _)) =>
        if (name1 != name2)
          throw new EvalException(s"cannot coerce struct ${name2} to struct ${name1}",
                                  text,
                                  docSourceUrl)
        value

      // cast of an object to a struct. I think this is legal.
      case (WdlTypes.WT_Struct(name, memberDefs), WV_Object(members)) =>
        coerceToStruct(name, memberDefs, members, text)

      case (WdlTypes.WT_Struct(name, memberDefs), WV_Map(members)) =>
        // convert into a mapping from string to WdlValue
        val members2: Map[String, WV] = members.map {
          case (WV_String(k), v) => k -> v
          case (other, _) =>
            throw new EvalException(s"${other} has to be a string for this to be a struct",
                                    text,
                                    docSourceUrl)
        }
        coerceToStruct(name, memberDefs, members2, text)

      case (WdlTypes.WT_Object, WV_Object(_)) => value

      case (t, other) =>
        throw new EvalException(s"value ${other} cannot be coerced to type ${t}",
                                text,
                                docSourceUrl)
    }
  }
}
