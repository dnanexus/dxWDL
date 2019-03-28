package dxWDL.runner

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


    private def wdlValueToWVL(varName: String, value: WomValue) : WdlVarLinks =
        WdlVarLinks.importFromWDL(varName, value.womType, DeclAttrs.empty, value, IODirection.Zero)

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
        new Task(task, instanceTypeDB, cef, 0)
    }

    it should "calculate instance types" in {
        val taskRunner = makeTaskRunner("TA.wdl")

        val inputs = Map("cpu" -> wdlValueToWVL("cpu", makeOptional(WomInteger(4))),
                         "disks" -> wdlValueToWVL("disks", makeOptional(WomString("50"))))
        val instanceType = taskRunner.calcInstanceType(inputs)
        instanceType should equal("mem1_ssd1_x4")
    }

    it should "calculate instance types with task declarations" in {
        val taskRunner = makeTaskRunner("TC.wdl")

        val inputs = Map("base_mem_mb" -> wdlValueToWVL("base_mem_mb", WomInteger(5 * 1024)),
                         "disk_gb" -> wdlValueToWVL("disk_gb", WomInteger(2)))
        val instanceType = taskRunner.calcInstanceType(inputs)
        instanceType should equal("mem2_ssd1_x8")
    }

    it should "evaluate a task with an empty command" in {
        val taskRunner = makeTaskRunner("TB.wdl")

        val inputs = Map("hotel" -> wdlValueToWVL("hotel", WomString("Westin")),
                         "city" -> wdlValueToWVL("city", WomString("Seattle")),
                         "state" -> wdlValueToWVL("state", WomString("Washington")))
        taskRunner.prolog(inputs)
        val results = taskRunner.epilog(inputs)

        results("retval") should equal(JsArray(JsString("Westin"),
                                               JsString("Seattle"),
                                               JsString("Washington")))
    }

    it should "correctly call the {floor, ceil, round} stdlib function" in {
        val taskRunner = makeTaskRunner("TD.wdl")

        val inputs = Map("x" -> wdlValueToWVL("x", WomFloat(1.4)))
        taskRunner.prolog(inputs)
        val results = taskRunner.epilog(inputs)

        results("x_floor") should equal(JsNumber(1))
        results("x_ceil") should equal(JsNumber(2))
        results("x_round") should equal(JsNumber(1))
    }

    it should "read a docker manifest file" in {
        val buf = """|[
                     |{"Config":"4b778ee055da936b387080ba034c05a8fad46d8e50ee24f27dcd0d5166c56819.json",
                     |"RepoTags":["ubuntu_18_04_minimal:latest"],
                     |"Layers":[
                     |  "1053541ae4c67d0daa87babb7fe26bf2f5a3b29d03f4af94e9c3cb96128116f5/layer.tar",
                     |  "fb1542f1963e61a22f9416077bf5f999753cbf363234bf8c9c5c1992d9a0b97d/layer.tar",
                     |  "2652f5844803bcf8615bec64abd20959c023d34644104245b905bb9b08667c8d/layer.tar",
                     |  "386aac21291d1f58297bc7951ce00b4ff7485414d6a8e146d9fedb73e0ebfa5b/layer.tar",
                     |  "10d19fb34e1db6a5abf4a3c138dc21f67ef94c272cf359349da18ffa973b7246/layer.tar",
                     |  "c791705472caccd6c011326648cc9748bd1465451cd1cd28a809b0a7f4e8b671/layer.tar",
                     |  "d6cc894526fdfac9112633719d63806117b44cc7302f2a7ed6599b1a32f7c43a/layer.tar",
                     |  "3fbb031ee57d2a8b4b6615e540f55f9af88e88cdbceeffdac7033ec5c8ee327d/layer.tar"
                     |  ]
                     |}
                     |]""".stripMargin.trim

        val taskRunner = makeTaskRunner("TD.wdl")
        val repo = taskRunner.readManifestGetDockerImageName(buf)
        repo should equal("ubuntu_18_04_minimal:latest")
    }
}
