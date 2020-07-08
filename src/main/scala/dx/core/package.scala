package dx

import com.typesafe.config.ConfigFactory

package object core {
  // TODO: these belong somewhere else
  val REORG_STATUS = "reorg_status___"
  val REORG_STATUS_COMPLETE = "completed"

  // The version lives in application.conf
  def getVersion: String = {
    val config = ConfigFactory.load("application.conf")
    config.getString("dxWDL.version")
  }
}
