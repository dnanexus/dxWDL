package dx.compiler

import dx.core.languages.Language
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import wdlTools.types.Util
import wdlTools.types.WdlTypes._
import wdlTools.util.Logger

class WdlCodeGenTest extends AnyFlatSpec with Matchers {
  private val logger = Logger.Quiet
  private val wdlCodeGen = WdlCodeGen(logger, Map.empty, Language.WDLv1_0)

  private def genDefaultValue(t: T): String = {
    val defVal = wdlCodeGen.genDefaultValueOfType(t)
    Util.exprToString(defVal)
  }

  it should "Handle primitive types" in {
    genDefaultValue(T_Int) shouldBe "0"
    genDefaultValue(T_Float) shouldBe "0.0"
    genDefaultValue(T_String) shouldBe """"""""
    genDefaultValue(T_File) shouldBe """"dummy.txt""""
  }

  it should "Handle array" in {
    genDefaultValue(T_Array(T_Int, nonEmpty = false)) shouldBe "[]"
    genDefaultValue(T_Array(T_Float, nonEmpty = false)) shouldBe "[]"
  }

  it should "handle maps, pairs, optionals" in {
    genDefaultValue(T_Map(T_Int, T_String)) shouldBe """{0 : ""}"""
    genDefaultValue(T_Pair(T_Int, T_File)) shouldBe """(0, "dummy.txt")"""
    genDefaultValue(T_Optional(T_Float)) shouldBe "0.0"
  }

  it should "handle structs" in {
    val houseType = T_Struct(
        "House",
        Map("height" -> T_Int, "num_floors" -> T_Int, "street" -> T_String, "city" -> T_String)
    )
    genDefaultValue(houseType) shouldBe """object {height : 0, num_floors : 0, street : "", city : ""}"""
  }
}
