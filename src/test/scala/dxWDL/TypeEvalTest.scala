package dxWDL

import org.scalatest.{FlatSpec, Matchers}
import scala.util.{Failure, Success}
import wdl4s.wdl._
import wdl4s.wdl.expression._
import wdl4s.wdl.types._

class TypeEvalTest extends FlatSpec with Matchers {

    val wdlCode =
        """|task Add {
           |  Int a
           |  Int b
           |
           |  command {
           |    echo $((${a} + ${b}))
           |  }
           |  output {
           |    Int result = read_int(stdout())
           |  }
           |}
           |
           |workflow dict {
           |  Map[Int, Int] mII = {1: 10, 2: 11}
           |  Map[Int, Float]  mIF = {1: 1.2, 10: 113.0}
           |
           |  scatter(pair in mII) {
           |    Int valueII = pair.right
           |  }
           |
           |  scatter(pair in mIF) {
           |    call Add {
           |      input: a=pair.left, b=5
           |    }
           |  }
           |
           |  output {
           |    valueII
           |  }
           |}
           |""".stripMargin


    // Figure out the type of an expression
    def evalType(expr: WdlExpression, parent: Scope) : WdlType = {
/*        expr.evaluateType(Utils.lookupType(parent),
                          new WdlStandardLibraryFunctionsType,
                          Some(parent)) match {*/
        dxWDL.TypeEvaluator(Utils.lookupType(parent),
                            new WdlStandardLibraryFunctionsType,
                            Some(parent)).evaluate(expr.ast) match {
            case Success(wdlType) => wdlType
            case Failure(f) =>
                System.err.println(s"could not evaluate type of expression ${expr.toWdlString}")
                throw f
        }
    }

    it should "correctly evaluate expression types" in {
        val ns = WdlNamespaceWithWorkflow.load(wdlCode, Seq.empty).get
        val wf = ns.workflow

        val call:WdlCall = wf.findCallByName("Add") match {
            case None => throw new AppInternalException(s"Call Add not found in WDL file")
            case Some(call) => call
        }
        val ssc:Scatter = wf.scatters.head

        call.inputMappings.foreach { case (_, expr) =>
            val t:WdlType = evalType(expr, ssc)
            t should equal(WdlIntegerType)
        }
    }


    val wdlCode2 =
        """|task Copy {
           |  File src
           |  String basename
           |
           |  command <<<
           |    cp ${src} ${basename}.txt
           |    sort ${src} > ${basename}.sorted.txt
           |  >>>
           |  output {
           |    File outf = "${basename}.txt"
           |    File outf_sorted = "${basename}.sorted.txt"
           |  }
           |}
           |
           |
           |workflow files {
           |  File f
           |
           |  call Copy { input : src=f, basename="tearFrog" }
           |  call Copy as Copy2 { input : src=Copy.outf, basename="mixing" }
           |
           |  output {
           |    Copy2.outf_sorted
           |  }
           |}
           |""".stripMargin

    it should "correctly evaluate Strings and File types" in {
        val ns = WdlNamespaceWithWorkflow.load(wdlCode2, Seq.empty).get
        val wf = ns.workflow

        val copy2call:WdlCall = wf.findCallByName("Copy2") match {
            case None => throw new AppInternalException(s"Call Add not found in WDL file")
            case Some(call) => call
        }

        val e1 = copy2call.inputMappings("src")
        val t1 = evalType(e1, copy2call)
        //System.err.println(s"type(${e1.toWdlString}) = ${t1.toWdlString}")
        t1 should equal(WdlFileType)

        val e2 = copy2call.inputMappings("basename")
        evalType(e2, copy2call) should equal(WdlStringType)
    }


    val wdlCode3 =
    """|
       |task Inc {
       |  Int i
       |
       |  command <<<
       |    python -c "print(${i} + 1)"
       |  >>>
       |  output {
       |    Int result = read_int(stdout())
       |  }
       |}
       |
       |task Mod7 {
       |  Int i
       |
       |  command <<<
       |    python -c "print(${i} % 7)"
       |  >>>
       |  output {
       |    Int result = read_int(stdout())
       |  }
       |}
       |
       |workflow math {
       |  Int n = 5
       |
       |  scatter (k in range(length(n))) {
       |    call Mod7 {input: i=k}
       |  }
       |
       |  scatter (k in Mod7.result) {
       |    call Inc {input: i=k}
       |  }
       |
       |  output {
       |    Inc.result
       |  }
       |}
       |""".stripMargin

    it should "take context into account" in {
        val ns = WdlNamespaceWithWorkflow.load(wdlCode3, Seq.empty).get
        val wf = ns.workflow

        val incCall:WdlCall = wf.findCallByName("Inc") match {
            case None => throw new AppInternalException(s"Call Add not found in WDL file")
            case Some(call) => call
        }

        val e1 = incCall.inputMappings("i")
        val t1 = evalType(e1, incCall)
        //System.err.println(s"type(${e1.toWdlString}) = ${t1.toWdlString}")
        t1 should equal(WdlIntegerType)

        val output = wf.outputs.head
        //System.err.println(output)
        val e2 = output.requiredExpression
        val t2 = evalType(e2, wf)
        //System.err.println(s"type(${e2.toWdlString}) = ${t2.toWdlString}")
        t2 should equal(WdlMaybeEmptyArrayType(WdlIntegerType))
    }
}
