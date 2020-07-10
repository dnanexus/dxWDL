package dx.compiler

import java.nio.file.{Path, Paths}

import dx.api.DxApi
import dx.core.languages.wdl
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import wdlTools.types.WdlTypes
import wdlTools.util.{FileUtils, Logger}

class SortTypeAliasesTest extends AnyFlatSpec with Matchers {
  private def pathFromBasename(dir: String, basename: String): Path = {
    val p = getClass.getResource(s"/${dir}/${basename}").getPath
    Paths.get(p)
  }

  private val logger = Logger.Quiet
  private val dxApi = DxApi(logger)

  it should "sort type aliases properly" in {
    val path = pathFromBasename("struct", "many_structs.wdl")
    val wfSourceCode = FileUtils.readFileContent(path)
    val (_, _, typeAliases: Map[String, WdlTypes.T], _) =
      wdl.ParseSource(dxApi).parseWdlWorkflow(wfSourceCode)

    val defs: Vector[(String, WdlTypes.T)] = SortTypeAliases(logger).apply(typeAliases.toVector)
    val defNames = defs.map { case (name, _) => name }
    defNames shouldBe Vector("Coord", "Bunk", "Foo", "SampleReports", "SampleReportsArray")
  }
}
