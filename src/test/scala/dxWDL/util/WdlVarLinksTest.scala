package dxWDL.util

import org.scalatest.{FlatSpec, Matchers}
import spray.json._
import wom.types._
import wom.values._

import dxWDL.base.{Utils, Verbose}

class WdlVarLinksTest extends FlatSpec with Matchers {

  private val verbose = Verbose(false, false, Set.empty)

  case class Element(name: String, womType: WomType, womValue: WomValue)

  def makeElement(womValue: WomValue): Element =
    Element("A", womValue.womType, womValue)

  def check(elem: Element, wvlConverter: WdlVarLinksConverter): Unit = {
    val prefix = "XXX_"
    val wvl: WdlVarLinks =
      wvlConverter.importFromWDL(elem.womType, elem.womValue)
    val allDxFields1: List[(String, JsValue)] =
      wvlConverter.genFields(wvl, prefix + elem.name)
    val allDxFields2 = allDxFields1.filter {
      case (key, v) => !key.endsWith(Utils.FLAT_FILES_SUFFIX)
    }
    allDxFields2.size should be(1)
    val (name2, jsv) = allDxFields2.head

    name2 should be(prefix + elem.name)
    val (womValue2, _) =
      wvlConverter.unpackJobInput(elem.name, elem.womType, jsv)
    womValue2 should be(elem.womValue)
  }

  it should "handle primitive WDL elements" in {
    val wvlConverter = new WdlVarLinksConverter(verbose, Map.empty, Map.empty)

    val testCases = List(
        // primitives
        makeElement(WomBoolean(true)),
        makeElement(WomInteger(19)),
        makeElement(WomFloat(2.718)),
        makeElement(WomString("water and ice")),
        makeElement(WomSingleFile("/usr/var/local/bin/gcc"))
    )

    testCases.foreach { elem =>
      check(elem, wvlConverter)
    }
  }

  it should "handle compound WDL types" in {
    val wvlConverter = new WdlVarLinksConverter(verbose, Map.empty, Map.empty)

    def makePair(x: Double, s: String): WomValue = {
      WomPair(WomFloat(x), WomString(s))
    }

    val testCases = List(
        // pairs
        makeElement(makePair(24.1, "Fiji is an island in the pacific ocean")),
        makeElement(
            WomArray(
                WomArrayType(WomBooleanType),
                Vector(WomBoolean(true), WomBoolean(false))
            )
        ),
        makeElement(
            WomOptionalValue(WomSingleFileType, Some(WomSingleFile("ddd")))
        ),
        // maps
        makeElement(
            WomMap(
                WomMapType(WomStringType, WomBooleanType),
                Map(
                    WomString("A") -> WomBoolean(true),
                    WomString("C") -> WomBoolean(false),
                    WomString("G") -> WomBoolean(true),
                    WomString("H") -> WomBoolean(false)
                )
            )
        ),
        makeElement(
            WomMap(
                WomMapType(WomIntegerType, WomPairType(WomFloatType, WomStringType)),
                Map(
                    WomInteger(1) -> makePair(1.3, "triangle"),
                    WomInteger(11) -> makePair(3.14, "pi")
                )
            )
        )
    )

    testCases.foreach { elem =>
      check(elem, wvlConverter)
    }
  }

  it should "handle structs" in {
    val personType =
      WomCompositeType(
          Map("name" -> WomStringType, "age" -> WomIntegerType),
          Some("Person")
      )

    val jeff = WomObject(
        Map("name" -> WomString("Jeoffrey"), "age" -> WomInteger(16)),
        personType
    )
    val janice = WomObject(
        Map("name" -> WomString("Janice"), "age" -> WomInteger(25)),
        personType
    )

    val testCases = List(makeElement(jeff), makeElement(janice))

    // no definitions for struct Person, should fail
    // val wvlConverterEmpty = new WdlVarLinksConverter(verbose, Map.empty, Map.empty)
    // testCases.foreach{ elem =>
    // assertThrows[Exception] {
//                check(elem, wvlConverterEmpty)
//            }
//        }

    val typeAliases: Map[String, WomType] = Map("Person" -> personType)
    val wvlConverter = new WdlVarLinksConverter(verbose, Map.empty, typeAliases)
    testCases.foreach { elem =>
      check(elem, wvlConverter)
    }
  }

  it should "handle nested structs" taggedAs (EdgeTest) in {
    // People
    val personType =
      WomCompositeType(
          Map("name" -> WomStringType, "age" -> WomIntegerType),
          Some("Person")
      )

    val lucy = WomObject(
        Map("name" -> WomString("Lucy"), "age" -> WomInteger(37)),
        personType
    )
    val lear = WomObject(
        Map("name" -> WomString("King Lear"), "age" -> WomInteger(41)),
        personType
    )

    // Houses
    val houseType = WomCompositeType(
        Map(
            "person" -> personType,
            "zipcode" -> WomIntegerType,
            "type" -> WomStringType
        ),
        Some("House")
    )

    val learCastle = WomObject(
        Map(
            "person" -> lear,
            "zipcode" -> WomInteger(1),
            "type" -> WomString("Castle")
        ),
        houseType
    )

    val lucyHouse = WomObject(
        Map(
            "person" -> lucy,
            "zipcode" -> WomInteger(94043),
            "type" -> WomString("town house")
        ),
        houseType
    )

    val testCases = List(makeElement(learCastle), makeElement(lucyHouse))

    val typeAliases: Map[String, WomType] =
      Map("Person" -> personType, "House" -> houseType)
    val wvlConverter = new WdlVarLinksConverter(verbose, Map.empty, typeAliases)
    testCases.foreach { elem =>
      check(elem, wvlConverter)
    }
  }
}
