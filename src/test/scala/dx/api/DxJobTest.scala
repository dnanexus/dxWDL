package dx.api

import dx.Assumptions.isLoggedIn
import dx.Tags.ApiTest
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import wdlTools.util.Logger

class DxJobTest extends AnyFlatSpec with Matchers {
  assume(isLoggedIn)
  private val dxApi: DxApi = DxApi(Logger.Quiet)
  private val PlaygroundProject = Some(dxApi.project("project-FGpfqjQ0ffPF1Q106JYP2j3v"))
  private val AppId = "app-Fqy8Xx00zZvZ503g5gJgq3Gb"
  private val AppJobId = "job-Fqy8YF00ffPB148G5f490Y25"
  private val AppletId = "applet-Fqy8Vy80ffP3b1j75bvVFX3Q"
  private val AppletJobId = "job-Fqy8XV00ffP3b1j75bvVFX3b"

  it should "describe job which ran app" taggedAs ApiTest in {
    val dxAppJob = dxApi.job(AppJobId, PlaygroundProject)
    val dxAppJobDescription = dxAppJob.describe(Set(Field.Executable))
    dxAppJobDescription.executable.get.id shouldBe AppId
  }

  it should "describe job which ran applet" taggedAs ApiTest in {
    val dxAppletJob = dxApi.job(AppletJobId, PlaygroundProject)
    val dxAppletJobDescription = dxAppletJob.describe(Set(Field.Executable))
    dxAppletJobDescription.executable.get.id shouldBe AppletId
  }
}
