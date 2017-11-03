package dxWDL

import org.scalatest.{FlatSpec, Matchers}
import scala.collection.mutable.HashMap
import wdl4s.wdl._
import wdl4s.wdl.expression._
import wdl4s.wdl.types._
import wdl4s.wdl.values._

class BValuesTest extends FlatSpec with Matchers {

    private def make(wdlValue: WdlValue) : BValue = {
        val wvl = WdlVarLinks.importFromWDL(wdlValue.wdlType,
                                            DeclAttrs.empty,
                                            wdlValue)
        BValue(wvl, wdlValue)
    }

    def bValueFrom(n: Int): BValue = {
        make(WdlInteger(n))
    }
    def bValueFrom(s:String): BValue = {
        make(WdlString(s))
    }
    def bValueFrom(arr: Vector[String]): BValue = {
        val v:Vector[WdlValue] = arr.map(s => WdlString(s))
        make(WdlArray(WdlArrayType(WdlStringType), v))
    }

    it should "support primitive values" in {
        val values: List[BValue] = List( bValueFrom(4), bValueFrom(10),
                                         bValueFrom("hello"), bValueFrom("nice"))
        values.foreach{ bv =>
            bv should equal(BValue.fromJSON(BValue.toJSON(bv)))
        }
    }

    it should "support arrays of primitives" in {
        val values: List[BValue] = List( bValueFrom(Vector("1", "2", "3")),
                                         bValueFrom(Vector("Cheshire", "cat", "grins")) )
        values.foreach{ bv =>
            bv should equal(BValue.fromJSON(BValue.toJSON(bv)))
        }
    }

    it should "support maps" in {
        val code=
            """|task t {
               |    Map[String, Int] mSI = {"Apple": 1, "Mellon": 2}
               |    Map[String, Int] mSI2 = {"Watermellon": 4, "Pear": 3}
               |    Map[String, Float] mSF = {"Apple": 1.0, "Mellon": 2.3}
               |
               |    Map[String, Map[String, Int]] mmSI = {
               |      "cheap": mSI,
               |      "expensive": mSI2
               |    }
               |    command {}
               |    output {}
               |}
               |
               |workflow w {}
               |""".stripMargin.trim

        val ns = WdlNamespaceWithWorkflow.load(code, Seq.empty).get
        val task = ns.findTask("t").get

        val env = HashMap.empty[String, WdlValue]
        def lookup(varName : String) : WdlValue = {
            env(varName)
        }

        task.declarations.foreach{ decl =>
            val e:WdlExpression = decl.expression.get
            val w:WdlValue = e.evaluate(lookup, NoFunctions).get

            val bv = make(w)
            bv should equal(BValue.fromJSON(BValue.toJSON(bv)))

            // add to environment
            env(decl.unqualifiedName) = w
        }
    }
}
