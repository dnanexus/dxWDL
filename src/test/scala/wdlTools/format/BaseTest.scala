package wdlTools.format

import java.net.URL
import java.nio.file.{Path, Paths}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import wdlTools.formatter.WdlV1Formatter
import wdlTools.syntax.{WdlVersion, v1}
import wdlTools.util.{Options, Util}

class BaseTest extends AnyFlatSpec with Matchers {
  private lazy val opts = Options()
  private lazy val parser = v1.ParseAll(opts)

  def getWdlPath(fname: String, subdir: String): Path = {
    Paths.get(getClass.getResource(s"/wdlTools/format/${subdir}/${fname}").getPath)
  }

  private def getWdlUrl(fname: String, subdir: String): URL = {
    Util.pathToUrl(getWdlPath(fname, subdir))
  }

  it should "handle the runtime section correctly" in {
    val doc = parser.parseDocument(getWdlUrl(fname = "simple.wdl", subdir = "after"))
    doc.version.value shouldBe WdlVersion.V1
  }

  def getWdlSource(fname: String, subdir: String): String = {
    Util.readFromFile(getWdlPath(fname, subdir))
  }

  it should "reformat simple WDL" in {
    val beforeURL = getWdlUrl(fname = "simple.wdl", subdir = "before")
    val expected = getWdlSource(fname = "simple.wdl", subdir = "after")
    val formatter = WdlV1Formatter(opts)
    formatter.formatDocuments(beforeURL)
    formatter.documents(beforeURL).mkString("\n") shouldBe expected
  }
}
