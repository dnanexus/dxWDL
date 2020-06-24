package dx

import java.nio.file.Path

import com.typesafe.config.{Config, ConfigFactory}
import dx.util.Logger

import scala.jdk.CollectionConverters._

package object compiler {
  // default values
  val DEFAULT_APPLET_TIMEOUT_IN_DAYS = 2
  val DEFAULT_RUNTIME_DEBUG_LEVEL = 1
  val DEFAULT_UBUNTU_VERSION = "16.04"

  // common property names
  val CHECKSUM_PROP = "dxWDL_checksum"
  val VERSION_PROP = "dxWDL_version"

  // other constants
  val DX_WDL_ASSET = "dxWDLrt"
  val REORG_CONFIG = "reorg_conf___"
  val DX_WDL_RUNTIME_CONF_FILE = "dxWDL_runtime.conf"

  // the regions live in dxWDL.conf
  def getRegions: Map[String, String] = {
    val config = ConfigFactory.load(DX_WDL_RUNTIME_CONF_FILE)
    val l: List[Config] = config.getConfigList("dxWDL.region2project").asScala.toList
    val region2project: Map[String, String] = l.map { pair =>
      val r = pair.getString("region")
      val projName = pair.getString("path")
      r -> projName
    }.toMap
    region2project
  }

  object CompilerFlag extends Enumeration {
    type CompilerFlag = Value
    val All, IR, NativeWithoutRuntimeAsset = Value
  }

  // Tree printer types for the execTree option
  sealed trait TreePrinter

  case object JsonTreePrinter extends TreePrinter

  case object PrettyTreePrinter extends TreePrinter

  // Packing of all compiler flags in an easy to digest format
  case class CompilerOptions(archive: Boolean,
                             compileMode: CompilerFlag.Value,
                             defaults: Option[Path],
                             extras: Option[Extras],
                             fatalValidationWarnings: Boolean,
                             force: Boolean,
                             importDirs: List[Path],
                             inputs: List[Path],
                             leaveWorkflowsOpen: Boolean,
                             locked: Boolean,
                             projectWideReuse: Boolean,
                             reorg: Boolean,
                             streamAllFiles: Boolean,
                             execTree: Option[TreePrinter],
                             runtimeDebugLevel: Option[Int],
                             logger: Logger)

}
