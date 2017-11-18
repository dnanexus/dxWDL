package dxWDL

import org.scalatest.{FlatSpec, Matchers}
import scala.collection.mutable.HashMap
import wdl4s.wdl._
import wdl4s.wdl.expression._
import wdl4s.wdl.types._
import wdl4s.wdl.values._

class BValueTest extends FlatSpec with Matchers {

    private def make(wdlValue: WdlValue) : BValue = {
        val wvl = WdlVarLinks.importFromWDL(wdlValue.wdlType,
                                            DeclAttrs.empty,
                                            wdlValue,
                                            IODirection.Zero)
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

    it should "support maps and objects" in {
        val code=
            """|task t {
               |    Map[String, Int] mSI = {"Apple": 1, "Mellon": 2}
               |    Map[String, Int] mSI2 = {"Watermellon": 4, "Pear": 3}
               |    Map[String, Float] mSF = {"Apple": 1.0, "Mellon": 2.3}
               |
               |    Map[String, Map[String, Int]] mmSI = {
               |     "cheap": mSI,
               |     "expensive": mSI2
               |    }
               |
               |    Object z1 = {"a": 1, "b": 2}
               |    Object z2 = {"a": 0, "b": 3}
               |
               |    Pair[Int, String] p1 = (1, "snail")
               |
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

            // Coerce to the right type if needed
            val wdlValue = Utils.cast(decl.wdlType, w, decl.unqualifiedName)

            val bv = make(wdlValue)
            val bvJs = BValue.toJSON(bv)
            //System.err.println(bvJs.prettyPrint)
            val bv2 = BValue.fromJSON(bvJs)
            bv should equal(bv2)
            //System.err.println(s"${decl.unqualifiedName} -> ${bv}")

            // add to environment
            env(decl.unqualifiedName) = w
        }
    }
}
