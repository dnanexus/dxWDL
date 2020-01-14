package dxWDL.compiler

import org.scalatest.{FlatSpec, Matchers}
import wom.types._

import dxWDL.base._

class WdlCodeGenTest extends FlatSpec with Matchers {
  private val verbose = Verbose(false, true, Set.empty)
  private val wdlCodeGen = WdlCodeGen(verbose, Map.empty, Language.WDLv1_0)

  private def genDefaultValue(t: WomType): String = {
    val defVal = wdlCodeGen.genDefaultValueOfType(t)
    defVal.toWomString
  }

  it should "Handle primitive types" in {
    genDefaultValue(WomIntegerType) shouldBe ("0")
    genDefaultValue(WomFloatType) shouldBe ("0.0")
    genDefaultValue(WomStringType) shouldBe ("""""""")
    genDefaultValue(WomSingleFileType) shouldBe (""""dummy.txt"""")
  }

  it should "Handle array" in {
    genDefaultValue(WomArrayType(WomIntegerType)) shouldBe ("[]")
    genDefaultValue(WomArrayType(WomFloatType)) shouldBe ("[]")
  }

  it should "handle maps, pairs, optionals" in {
    genDefaultValue(WomMapType(WomIntegerType, WomStringType)) shouldBe ("""{0: ""}""")

    genDefaultValue(WomPairType(WomIntegerType, WomSingleFileType)) shouldBe ("""(0, "dummy.txt")""")
    genDefaultValue(WomOptionalType(WomFloatType)) shouldBe ("0.0")
  }

  it should "handle structs" in {
    val houseType = WomCompositeType(Map("height" -> WomIntegerType,
                                         "num_floors" -> WomIntegerType,
                                         "street" -> WomStringType,
                                         "city" -> WomStringType),
                                     Some("House"))
    genDefaultValue(houseType) shouldBe ("""object {height: 0, num_floors: 0, street: "", city: ""}""")
  }
}
