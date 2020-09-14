package dx.core.ir

import dx.Tags.EdgeTest
import dx.api.DxApi
import dx.core.io.{DxFileAccessProtocol, DxFileDescCache}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import spray.json._
import wdlTools.util.{FileSourceResolver, Logger}

class ParameterLinkTest extends AnyFlatSpec with Matchers {
  private val dxApi = DxApi(Logger.Quiet)
  private val dxProtocol = DxFileAccessProtocol(dxApi)
  private val fileResolver = FileSourceResolver.create(userProtocols = Vector(dxProtocol))
  private val parameterLinkSerializer = ParameterLinkSerializer(fileResolver, dxApi)
  private val parameterLinkDeserializer = ParameterLinkDeserializer(DxFileDescCache.empty, dxApi)

  case class Element(name: String, irType: Type, irValue: Value)

  def makeElement(t: Type, v: Value): Element = Element("A", t, v)

  def check(elem: Element): Unit = {
    val prefix = "XXX_"
    val link = parameterLinkSerializer.createLink(elem.irType, elem.irValue)
    val allDxFields1: Vector[(String, JsValue)] =
      parameterLinkSerializer.createFieldsFromLink(link, prefix + elem.name)
    val allDxFields2 = allDxFields1.filter {
      case (key, _) => !key.endsWith(ParameterLink.FlatFilesSuffix)
    }
    allDxFields2.size should be(1)
    val (name2, jsv) = allDxFields2.head

    name2 should be(prefix + elem.name)
    val wdlValue2 = parameterLinkDeserializer.deserializeInputWithType(jsv, elem.irType)
    wdlValue2 should be(elem.irValue)
  }

  it should "handle primitive WDL elements" in {
    val testCases = Vector(
        // primitives
        makeElement(Type.TBoolean, Value.VBoolean(true)),
        makeElement(Type.TInt, Value.VInt(19)),
        makeElement(Type.TFloat, Value.VFloat(2.718)),
        makeElement(Type.TString, Value.VString("water and ice")),
        makeElement(Type.TFile, Value.VFile("/usr/var/local/bin/gcc"))
    )
    testCases.foreach(check)
  }

  it should "handle compound WDL types" in {
    val testCases = Vector(
        // pairs
        makeElement(
            Type.TArray(Type.TBoolean),
            Value.VArray(Vector(Value.VBoolean(true), Value.VBoolean(false)))
        ),
        makeElement(Type.TOptional(Type.TFile), Value.VFile("ddd")),
        // maps
        makeElement(
            Type.TMap(Type.TString, Type.TBoolean),
            Value.VMap(
                Map(
                    Value.VString("A") -> Value.VBoolean(true),
                    Value.VString("C") -> Value.VBoolean(false),
                    Value.VString("G") -> Value.VBoolean(true),
                    Value.VString("H") -> Value.VBoolean(false)
                )
            )
        )
    )
    testCases.foreach(check)
  }

  it should "handle structs" in {
    val personType =
      Type.TSchema("Person", Map("name" -> Type.TString, "age" -> Type.TInt))
    val jeff = Value.VHash(Map("name" -> Value.VString("Jeoffrey"), "age" -> Value.VInt(16)))
    val janice = Value.VHash(Map("name" -> Value.VString("Janice"), "age" -> Value.VInt(25)))
    val testCases = Vector(makeElement(personType, jeff), makeElement(personType, janice))

    // no definitions for struct Person, should fail
    // val wvlConverterEmpty = new WdlVarLinksConverter(verbose, Map.empty, Map.empty)
    // testCases.foreach{ elem =>
    // assertThrows[Exception] {
//                check(elem, wvlConverterEmpty)
//            }
//        }

    testCases.foreach(check)
  }

  it should "handle nested structs" taggedAs EdgeTest in {
    // People
    val personType =
      Type.TSchema("Person", Map("name" -> Type.TString, "age" -> Type.TInt))
    val houseType = Type.TSchema(
        "House",
        Map("person" -> personType, "zipcode" -> Type.TInt, "type" -> Type.TString)
    )

    // people
    val lucy = Value.VHash(Map("name" -> Value.VString("Lucy"), "age" -> Value.VInt(37)))
    val lear = Value.VHash(Map("name" -> Value.VString("King Lear"), "age" -> Value.VInt(41)))

    // Houses
    val learCastle =
      Value.VHash(
          Map("person" -> lear, "zipcode" -> Value.VInt(1), "type" -> Value.VString("Castle"))
      )

    val lucyHouse =
      Value.VHash(
          Map("person" -> lucy,
              "zipcode" -> Value.VInt(94043),
              "type" -> Value.VString("town house"))
      )

    val testCases = Vector(makeElement(houseType, learCastle), makeElement(houseType, lucyHouse))
    testCases.foreach(check)
  }
}
