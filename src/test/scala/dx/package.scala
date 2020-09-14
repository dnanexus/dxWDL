package dx

import dx.api.DxApi
import org.scalatest.Tag
import wdlTools.util.SysUtils

// test that requires being logged into DNAnexus
class DxTag(name: String = "dx") extends Tag(name)

object Tags {
  // a test that calls a DNAnexus API method (and thus requires being
  // logged in), but doesn't create any objects
  object ApiTest extends DxTag("dxApi")
  // test that builds native applets/workflows
  object NativeTest extends DxTag("native")
  // test that rqeuires being logged into a DNAnexus production account
  object ProdTest extends DxTag("prod")
  // marker for an edge case
  object EdgeTest extends Tag("edge")
}

object Assumptions {
  private val dxApi = DxApi.get

  /**
    * Tests that the user is logged in.
    * @return
    */
  lazy val isLoggedIn: Boolean = {
    try {
      dxApi.whoami() != null
    } catch {
      case _: Throwable => false
    }
  }

  /**
    * Tests that the dx toolkit is installed and in the path.
    * @return
    */
  lazy val toolkitCallable: Boolean = {
    try {
      val (retcode, _, _) = SysUtils.execCommand("dx whoami")
      retcode == 0
    } catch {
      case _: Throwable => false
    }
  }
}
