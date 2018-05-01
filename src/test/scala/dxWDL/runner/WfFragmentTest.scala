package dxWDL.runner

import dxWDL._
import java.nio.file.{Path, Paths}
import org.scalatest.{FlatSpec, Matchers}
import spray.json._
import wdl._
import wom.types._
import wom.values._

class WfFragmentTest extends FlatSpec with Matchers {

    lazy val currentWorkDir:Path = Paths.get(System.getProperty("user.dir"))
    private def pathFromBasename(basename: String) : Path = {
        currentWorkDir.resolve(s"src/test/resources/runner/${basename}")
    }

    private def wdlValueFromWVL(wvl: WdlVarLinks) : WomValue =
        WdlVarLinks.eval(wvl, IOMode.Remote, IODirection.Zero)

    private def wdlValueToWVL(value: WomValue) : WdlVarLinks =
        WdlVarLinks.importFromWDL(value.womType, DeclAttrs.empty, value, IODirection.Zero)

    private def makeOptional(value: WomValue) : WomValue = {
        WomOptionalValue(value.womType, Some(value))
    }
    private def makeOptionalNone(t: WomType) : WomValue = {
        WomOptionalValue(t, None)
    }

    private def evalWorkflow(filename: String,
                             inputs: Map[String, WdlVarLinks]) : Map[String, WomValue] = {
        val srcPath = pathFromBasename(filename)
        val wdlCode = Utils.readFileContent(srcPath)
        val ns = WdlNamespace.loadUsingSource(
            wdlCode, None, None
        ).get
        val cef = new CompilerErrorFormatter(srcPath.toString, ns.terminalMap)
        val mw = new WfFragment(Map.empty,
                                Map.empty,
                                cef,
                                JsNull,
                                RunnerWfFragmentMode.Launch,
                                false)

        val wf = ns match {
            case nswf:WdlNamespaceWithWorkflow => nswf.workflow
            case _ => throw new Exception("sanity")
        }
        val wvlOutputVars = mw.apply(wf, inputs)
        val results: Map[String, WomValue] = wvlOutputVars.map{
            case (name, wvl) =>
                name -> wdlValueFromWVL(wvl)
        }.toMap
        results
    }

    it should "evaluate workflow A" in {
        val results: Map[String, WomValue] = evalWorkflow("A.wdl", Map.empty)

        // result validation
        results.keys.toVector should contain("k")
        results.keys.toVector should contain("s")
        results("k") should equal(WomArray(
                                      WomArrayType(WomIntegerType),
                                      Vector(WomInteger(2), WomInteger(4), WomInteger(6))))
        results("s") should equal(WomOptionalValue(WomStringType,
                                                   Some(WomString("hello class of 2017"))))
    }

    it should "evaluate conditional inside scatter" in {
        val results: Map[String, WomValue] = evalWorkflow("B.wdl", Map.empty)

        results.keys.toVector should contain("square")
        results.keys.toVector should contain("numberedFruit")
        results("square") should equal(WomArray(WomArrayType(WomIntegerType),
                                                Vector(WomInteger(0),
                                                       WomInteger(1),
                                                       WomInteger(4),
                                                       WomInteger(9))))
        results("numberedFruit") should equal(WomArray(WomArrayType(WomOptionalType(WomStringType)),
                                                       Vector(makeOptionalNone(WomStringType),
                                                              makeOptionalNone(WomStringType),
                                                              makeOptional(WomString("mango_2")),
                                                              makeOptional(WomString("pear_3")))))
    }

    it should "evaluate nested scatters" in {
        val results: Map[String, WomValue] = evalWorkflow("C.wdl", Map.empty)

        results.keys.toVector should contain("numberedStreets")


        def makeStringArray(values: Vector[String]) : WomArray = {
            WomArray(WomArrayType(WomStringType),
                     values.map{ x => WomString(x) })
        }

        val a1:WomArray = makeStringArray(Vector("leghorn_1", "leghorn_2"))
        val a2:WomArray = makeStringArray(Vector("independence_1", "independence_2"))
        val a3:WomArray = makeStringArray(Vector("blossom_1", "blossom_2"))

        results("numberedStreets") should equal(WomArray(WomArrayType(WomArrayType(WomStringType)),
                                                         Vector(a1, a2, a3)))
    }

    it should "evaluate nested ifs without creating a double optional " in {
        val results: Map[String, WomValue] = evalWorkflow("D.wdl", Map.empty)

        results.keys.toVector should contain("chosen")

        val noneInt: WomValue = makeOptionalNone(WomIntegerType)
        results("chosen") should equal (WomArray(WomArrayType(WomOptionalType(WomIntegerType)),
                                                 Vector(noneInt,
                                                        makeOptional(WomInteger(2)), makeOptional(WomInteger(3)),
                                                        noneInt, noneInt)))
    }

    it should "evaluate scatter inside a conditional" in {
        val results: Map[String, WomValue] = evalWorkflow("E.wdl", Map.empty)

        results.keys.toVector should contain("cube")
        results.keys.toVector should contain("square")

        val cube: WomValue = (WomArray(WomArrayType(WomIntegerType),
                                       Vector(WomInteger(1),
                                              WomInteger(8),
                                              WomInteger(27),
                                              WomInteger(125))))
        results("cube") should equal(makeOptional(cube))

        val nn = makeOptionalNone(WomArrayType(WomIntegerType))
        results("square") should equal(nn)
    }

    it should "handle missing values in select_first/defined" in {
        val inputs = Map("flag" ->
                             wdlValueToWVL(makeOptionalNone(WomBooleanType)))
        val results: Map[String, WomValue] = evalWorkflow("select.wdl", inputs)

        results.keys.toVector should contain("x")
        results.keys.toVector should contain("y")

        results("x") should equal(WomString("FAIL"))
        results("y") should equal(WomString("FAIL"))
    }

    it should "handle missing values in select_first" in {
        val inputs = Map("x" ->
                             wdlValueToWVL(makeOptionalNone(WomIntegerType)))
        val results: Map[String, WomValue] = evalWorkflow("select2.wdl", inputs)

        results.keys.toVector should contain("y")

        results("y") should equal(WomInteger(20))
    }
}
