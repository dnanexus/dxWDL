package dxWDL.compiler

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import wdlTools.types.WdlTypes._

import dxWDL.base._

class WdlCodeGenTest extends AnyFlatSpec with Matchers {
  private val verbose = Verbose(false, true, Set.empty)
  private val wdlCodeGen = WdlCodeGen(verbose, Map.empty, Language.WDLv1_0)

  private def genDefaultValue(t: T): String = {
    val defVal = wdlCodeGen.genDefaultValueOfType(t)
    wdlCodeGen.wdlString(defVal)
  }

  it should "Handle primitive types" in {
    genDefaultValue(T_Int) shouldBe ("0")
    genDefaultValue(T_Float) shouldBe ("0.0")
    genDefaultValue(T_String) shouldBe ("""""""")
    genDefaultValue(T_File) shouldBe (""""dummy.txt"""")
  }

  it should "Handle array" in {
    genDefaultValue(T_Array(T_Int, false)) shouldBe ("[]")
    genDefaultValue(T_Array(T_Float, false)) shouldBe ("[]")
  }

  it should "handle maps, pairs, optionals" in {
    genDefaultValue(T_Map(T_Int, T_String)) shouldBe ("""{0 : ""}""")
    genDefaultValue(T_Pair(T_Int, T_File)) shouldBe ("""(0 , "dummy.txt")""")
    genDefaultValue(T_Optional(T_Float)) shouldBe ("0.0")
  }

  it should "handle structs" in {
    val houseType = T_Struct("House",
                             Map("height" -> T_Int,
                                 "num_floors" -> T_Int,
                                 "street" -> T_String,
                                 "city" -> T_String))
    genDefaultValue(houseType) shouldBe """object { height : 0, num_floors : 0, street : "", city : "" }"""
  }
}
