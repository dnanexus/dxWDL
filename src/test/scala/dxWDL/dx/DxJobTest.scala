package dxWDL.dx

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DxJobTest extends AnyFlatSpec with Matchers  {

  it should "describe job" in {
    val dxAppJob = DxJob("job-FqpyZP00k25Q2Kp5B0JV8Yv4", Some(DxProject("project-FpGpg8Q0k25YQ1k53YF8YxkG")))
    val dxAppJobDescription = dxAppJob.describe()
    dxAppJobDescription.executable.getId shouldBe "app-Fqppx0j0JBgZ4XbQB6Q6jVQG"

    val dxAppletJob = DxJob("job-Fqpyv100k25Q2Kp5B0JV8Z1f", Some(DxProject("project-FpGpg8Q0k25YQ1k53YF8YxkG")))
    val dxAppletJobDescription = dxAppletJob.describe()
    dxAppletJobDescription.executable.getId shouldBe "applet-Fqp89780k25vBB5J6x2jbJvJ"
  }

}
