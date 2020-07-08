package dx.core.languages.wdl

import dx.api.DxApi
import dx.core.io.{DxFileAccessProtocol, DxFileDescCache}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import spray.json._
import wdlTools.eval.WdlValues
import wdlTools.types.WdlTypes
import wdlTools.util.{FileSourceResolver, Logger}

class WdlVarLinksTest extends AnyFlatSpec with Matchers {
  private val dxApi = DxApi(Logger.Quiet)
  private val dxProtocol = DxFileAccessProtocol(dxApi)
  private val fileResolver = FileSourceResolver.create(userProtocols = Vector(dxProtocol))

  case class Element(name: String, wdlType: WdlTypes.T, wdlValue: WdlValues.V)

  def makeElement(t: WdlTypes.T, v: WdlValues.V): Element = Element("A", t, v)

  def check(elem: Element, wvlConverter: WdlVarLinksConverter): Unit = {
    val prefix = "XXX_"
    val wvl: WdlVarLinks = wvlConverter.importFromWDL(elem.wdlType, elem.wdlValue)
    val allDxFields1: List[(String, JsValue)] = wvlConverter.genFields(wvl, prefix + elem.name)
    val allDxFields2 = allDxFields1.filter {
      case (key, _) => !key.endsWith(WdlVarLinksConverter.FLAT_FILES_SUFFIX)
    }
    allDxFields2.size should be(1)
    val (name2, jsv) = allDxFields2.head

    name2 should be(prefix + elem.name)
    val wdlValue2 = wvlConverter.unpackJobInput(elem.name, elem.wdlType, jsv)
    wdlValue2 should be(elem.wdlValue)
  }

  it should "handle primitive WDL elements" in {
    val wvlConverter = WdlVarLinksConverter(dxApi, fileResolver, DxFileDescCache.empty, Map.empty)

    val testCases = List(
        // primitives
        makeElement(WdlTypes.T_Boolean, WdlValues.V_Boolean(true)),
        makeElement(WdlTypes.T_Int, WdlValues.V_Int(19)),
        makeElement(WdlTypes.T_Float, WdlValues.V_Float(2.718)),
        makeElement(WdlTypes.T_String, WdlValues.V_String("water and ice")),
        makeElement(WdlTypes.T_File, WdlValues.V_File("/usr/var/local/bin/gcc"))
    )

    testCases.foreach { elem =>
      check(elem, wvlConverter)
    }
  }

  it should "handle compound WDL types" in {
    val wvlConverter = WdlVarLinksConverter(dxApi, fileResolver, DxFileDescCache.empty, Map.empty)

    def makePair(x: Double, s: String): WdlValues.V = {
      WdlValues.V_Pair(WdlValues.V_Float(x), WdlValues.V_String(s))
    }

    val testCases = List(
        // pairs
        makeElement(WdlTypes.T_Pair(WdlTypes.T_Float, WdlTypes.T_String),
                    makePair(24.1, "Fiji is an island in the pacific ocean")),
        makeElement(
            WdlTypes.T_Array(WdlTypes.T_Boolean, nonEmpty = false),
            WdlValues.V_Array(Vector(WdlValues.V_Boolean(true), WdlValues.V_Boolean(false)))
        ),
        makeElement(WdlTypes.T_Optional(WdlTypes.T_File),
                    WdlValues.V_Optional(WdlValues.V_File("ddd"))),
        // maps
        makeElement(
            WdlTypes.T_Map(WdlTypes.T_String, WdlTypes.T_Boolean),
            WdlValues.V_Map(
                Map(
                    WdlValues.V_String("A") -> WdlValues.V_Boolean(true),
                    WdlValues.V_String("C") -> WdlValues.V_Boolean(false),
                    WdlValues.V_String("G") -> WdlValues.V_Boolean(true),
                    WdlValues.V_String("H") -> WdlValues.V_Boolean(false)
                )
            )
        ),
        makeElement(
            WdlTypes.T_Map(WdlTypes.T_Int, WdlTypes.T_Pair(WdlTypes.T_Float, WdlTypes.T_String)),
            WdlValues.V_Map(
                Map(WdlValues.V_Int(1) -> makePair(1.3, "triangle"),
                    WdlValues.V_Int(11) -> makePair(3.14, "pi"))
            )
        )
    )

    testCases.foreach { elem =>
      check(elem, wvlConverter)
    }
  }

  it should "handle structs" in {
    val personType =
      WdlTypes.T_Struct("Person", Map("name" -> WdlTypes.T_String, "age" -> WdlTypes.T_Int))

    val jeff = WdlValues.V_Struct(
        "Person",
        Map("name" -> WdlValues.V_String("Jeoffrey"), "age" -> WdlValues.V_Int(16))
    )
    val janice =
      WdlValues.V_Struct("Person",
                         Map("name" -> WdlValues.V_String("Janice"), "age" -> WdlValues.V_Int(25)))

    val testCases = List(makeElement(personType, jeff), makeElement(personType, janice))

    // no definitions for struct Person, should fail
    // val wvlConverterEmpty = new WdlVarLinksConverter(verbose, Map.empty, Map.empty)
    // testCases.foreach{ elem =>
    // assertThrows[Exception] {
//                check(elem, wvlConverterEmpty)
//            }
//        }

    val typeAliases: Map[String, WdlTypes.T] = Map("Person" -> personType)
    val wvlConverter = WdlVarLinksConverter(dxApi, fileResolver, DxFileDescCache.empty, typeAliases)
    testCases.foreach { elem =>
      check(elem, wvlConverter)
    }
  }

  it should "handle nested structs" taggedAs EdgeTest in {
    // People
    val personType =
      WdlTypes.T_Struct("Person", Map("name" -> WdlTypes.T_String, "age" -> WdlTypes.T_Int))
    val houseType = WdlTypes.T_Struct(
        "House",
        Map("person" -> personType, "zipcode" -> WdlTypes.T_Int, "type" -> WdlTypes.T_String)
    )

    // people
    val lucy =
      WdlValues.V_Struct("Person",
                         Map("name" -> WdlValues.V_String("Lucy"), "age" -> WdlValues.V_Int(37)))
    val lear = WdlValues.V_Struct(
        "Person",
        Map("name" -> WdlValues.V_String("King Lear"), "age" -> WdlValues.V_Int(41))
    )

    // Houses
    val learCastle =
      WdlValues.V_Struct("House",
                         Map("person" -> lear,
                             "zipcode" -> WdlValues.V_Int(1),
                             "type" -> WdlValues.V_String("Castle")))

    val lucyHouse =
      WdlValues.V_Struct("House",
                         Map("person" -> lucy,
                             "zipcode" -> WdlValues.V_Int(94043),
                             "type" -> WdlValues.V_String("town house")))

    val testCases = List(makeElement(houseType, learCastle), makeElement(houseType, lucyHouse))

    val typeAliases: Map[String, WdlTypes.T] = Map("Person" -> personType, "House" -> houseType)
    val wvlConverter = WdlVarLinksConverter(dxApi, fileResolver, DxFileDescCache.empty, typeAliases)
    testCases.foreach { elem =>
      check(elem, wvlConverter)
    }
  }
}
