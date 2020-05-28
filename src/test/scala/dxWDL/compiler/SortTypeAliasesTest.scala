package dxWDL.compiler

import java.nio.file.{Path, Paths}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import wdlTools.types.WdlTypes

import dxWDL.base.{Utils, Verbose}
import dxWDL.base.ParseWomSourceFile

class SortTypeAliasesTest extends AnyFlatSpec with Matchers {
  private def pathFromBasename(dir: String, basename: String): Path = {
    val p = getClass.getResource(s"/${dir}/${basename}").getPath
    Paths.get(p)
  }

  val verbose = Verbose(false, true, Set.empty)

  it should "sort type aliases properly" in {
    val path = pathFromBasename("struct", "many_structs.wdl")
    val wfSourceCode = Utils.readFileContent(path)
    val (_, _, typeAliases: Map[String, WdlTypes.T], _) =
      ParseWomSourceFile(false).parseWdlWorkflow(wfSourceCode)

    val defs: Vector[(String, WdlTypes.T)] = SortTypeAliases(verbose).apply(typeAliases.toVector)
    val defNames = defs.map { case (name, _) => name }.toVector
    defNames shouldBe (Vector("Coord", "Bunk", "Foo", "SampleReports", "SampleReportsArray"))
  }
}
