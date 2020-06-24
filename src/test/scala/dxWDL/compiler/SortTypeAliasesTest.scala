package dxWDL.compiler

import java.nio.file.{Path, Paths}

import dx.compiler.SortTypeAliases
import dx.util.Verbose
import dx.core.languages.wdl.ParseSource
import dx.core.util.SysUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import wdlTools.types.WdlTypes

class SortTypeAliasesTest extends AnyFlatSpec with Matchers {
  private def pathFromBasename(dir: String, basename: String): Path = {
    val p = getClass.getResource(s"/${dir}/${basename}").getPath
    Paths.get(p)
  }

  private val verbose = Verbose(on = false, quiet = true, Set.empty)

  it should "sort type aliases properly" in {
    val path = pathFromBasename("struct", "many_structs.wdl")
    val wfSourceCode = SysUtils.readFileContent(path)
    val (_, _, typeAliases: Map[String, WdlTypes.T], _) =
      ParseSource(false).parseWdlWorkflow(wfSourceCode)

    val defs: Vector[(String, WdlTypes.T)] = SortTypeAliases(verbose).apply(typeAliases.toVector)
    val defNames = defs.map { case (name, _) => name }
    defNames shouldBe Vector("Coord", "Bunk", "Foo", "SampleReports", "SampleReportsArray")
  }
}
