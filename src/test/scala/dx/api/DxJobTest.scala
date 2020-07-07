package dx.api

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import wdlTools.util.Logger

class DxJobTest extends AnyFlatSpec with Matchers {
  private val dxApi: DxApi = DxApi(Logger.Quiet)
  private val dxWDLPlaygroundProject = Some(dxApi.project("project-FGpfqjQ0ffPF1Q106JYP2j3v"))

  it should "describe job which ran app" in {
    val dxAppJob = dxApi.job("job-Fqy8YF00ffPB148G5f490Y25", dxWDLPlaygroundProject)
    val dxAppJobDescription = dxAppJob.describe()
    dxAppJobDescription.executable.getId shouldBe "app-Fqy8Xx00zZvZ503g5gJgq3Gb"
  }

  it should "describe job which ran applet" in {
    val dxAppletJob = dxApi.job("job-Fqy8XV00ffP3b1j75bvVFX3b", dxWDLPlaygroundProject)
    val dxAppletJobDescription = dxAppletJob.describe()
    dxAppletJobDescription.executable.getId shouldBe "applet-Fqy8Vy80ffP3b1j75bvVFX3Q"
  }
}
