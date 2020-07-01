package dxWDL.dx

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DxJobTest extends AnyFlatSpec with Matchers {

  it should "describe job which ran app" in {
    val dxAppJob = DxJob("job-Fqy8YF00ffPB148G5f490Y25", Some(DxProject("project-FGpfqjQ0ffPF1Q106JYP2j3v")))
    val dxAppJobDescription = dxAppJob.describe()
    dxAppJobDescription.executable.getId shouldBe "app-Fqy8Xx00zZvZ503g5gJgq3Gb"
  }

  it should "describe job which ran applet" in {
    val dxAppletJob = DxJob("job-Fqy8XV00ffP3b1j75bvVFX3b", Some(DxProject("project-FGpfqjQ0ffPF1Q106JYP2j3v")))
    val dxAppletJobDescription = dxAppletJob.describe()
    dxAppletJobDescription.executable.getId shouldBe "applet-Fqy8Vy80ffP3b1j75bvVFX3Q"
  }
}
