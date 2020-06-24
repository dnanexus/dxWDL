package dx

import com.typesafe.config.ConfigFactory

package object util {
  // The version lives in application.conf
  def getVersion: String = {
    val config = ConfigFactory.load("application.conf")
    config.getString("dxWDL.version")
  }
}
