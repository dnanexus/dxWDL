package dxWDL.runner

import com.dnanexus.{IOClass}
import dxWDL._
import java.nio.file.{Path, Paths}
import org.scalatest.{FlatSpec, Matchers}
import spray.json._
import wdl.draft2.model._
import wom.types._
import wom.values._

class TaskTest extends FlatSpec with Matchers {
    lazy val currentWorkDir:Path = Paths.get(System.getProperty("user.dir"))
    private def pathFromBasename(basename: String) : Path = {
        currentWorkDir.resolve(s"src/test/resources/runner/${basename}")
    }
    private val instanceTypeDB = InstanceTypeDB.genTestDB(true)


    private def wdlValueToWVL(value: WomValue) : WdlVarLinks =
        WdlVarLinks.importFromWDL(value.womType, DeclAttrs.empty, value, IODirection.Zero)

    private def makeOptional(value: WomValue) : WomValue =
        WomOptionalValue(value.womType, Some(value))

//    private def makeOptionalNone(t: WomType) : WomValue = {
//        WomOptionalValue(t, None)
//    }

    it should "serialize correctly" in {

        val wdlValues = List(
            WomBoolean(true),
            WomInteger(3),
            WomFloat(4.5),
            WomString("flute"),
            WomSingleFile("invalid-file"),

            WomArray(WomArrayType(WomIntegerType),
                     List(WomInteger(3), WomInteger(15))),
            WomArray(WomArrayType(WomStringType),
                     List(WomString("r"), WomString("l"))),

            WomOptionalValue(WomStringType, Some(WomString("french horm"))),
            WomOptionalValue(WomStringType, None),

            WomArray(WomArrayType(WomOptionalType(WomIntegerType)),
                     List(WomOptionalValue(WomIntegerType, Some(WomInteger(1))),
                          WomOptionalValue(WomIntegerType, None),
                          WomOptionalValue(WomIntegerType, Some(WomInteger(2)))))
        )

        wdlValues.foreach{ w =>
            val jsv:JsValue = TaskSerialization.toJSON(w)
            val w2:WomValue = TaskSerialization.fromJSON(jsv)
            w2 should equal(w)
        }
    }

    private def makeTaskRunner(filename: String) : Task = {
        val srcPath = pathFromBasename(filename)
        val wdlCode = Utils.readFileContent(srcPath)
        val ns = WdlNamespace.loadUsingSource(
            wdlCode, None, None
        ).get
        val task = ns.tasks.head
        val cef = new CompilerErrorFormatter(srcPath.toString, ns.terminalMap)
        new Task(task, instanceTypeDB, cef, false)
    }

    it should "calculate instance types" in {
        val taskRunner = makeTaskRunner("TA.wdl")

        val inputs = Map("cpu" -> wdlValueToWVL(makeOptional(WomInteger(4))),
                         "disks" -> wdlValueToWVL(makeOptional(WomString("50"))))
        val instanceType = taskRunner.calcInstanceType(inputs)
        instanceType should equal("mem1_ssd1_x4")
    }

    it should "evaluate a task with an empty command" in {
        val taskRunner = makeTaskRunner("TB.wdl")

        val inputs = Map("hotel" -> wdlValueToWVL(WomString("Westin")),
                         "city" -> wdlValueToWVL(WomString("Seattle")),
                         "state" -> wdlValueToWVL(WomString("Washington")))
        val inputSpec = Map("hotel" -> DXIOParam(IOClass.STRING, false),
                           "city" -> DXIOParam(IOClass.STRING, false),
                            "state" -> DXIOParam(IOClass.STRING, false))
        val outputSpec = Map("retval" -> DXIOParam(IOClass.ARRAY_OF_STRINGS, false))

        taskRunner.prolog(inputSpec, outputSpec, inputs)
        val results = taskRunner.epilog(inputSpec, outputSpec, inputs)

        results("retval") should equal(JsArray(JsString("Westin"),
                                               JsString("Seattle"),
                                               JsString("Washington")))
    }

}
