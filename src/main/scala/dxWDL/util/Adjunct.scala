/** dxWDL supports reading some metadata from "adjunct" files - i.e. files in the same directory
  * as the WDL file.
**/
package dxWDL.util

import java.nio.file.Path
import dxWDL.base.Utils

object Adjuncts {
  sealed abstract class AdjunctFile
  final case class Readme(text: String) extends AdjunctFile
  final case class DeveloperNotes(text: String) extends AdjunctFile

  def findAdjunctFiles(path: Path): Map[String, Vector[AdjunctFile]] = {
    val file = path.toFile
    val parentDir = file.getParentFile
    var wdlName = file.getName

    if (wdlName.endsWith(".wdl")) {
      wdlName = wdlName.dropRight(4)
    }

    val readmeRegexp = s"(?i)readme.${wdlName}.(.*)(?:.md)".r
    val developerNotesRegexp = s"(?i)readme.developer.${wdlName}.(.*)(?:.md)".r

    parentDir.listFiles
      .flatMap { file =>
        {
          file.getName match {
            case readmeRegexp(target) =>
              Some(target -> Readme(Utils.readFileContent2(file.toPath)))
            case developerNotesRegexp(target) =>
              Some(target -> DeveloperNotes(Utils.readFileContent2(file.toPath)))
            case _ => None
          }
        }
      }
      .groupBy(_._1)
      .mapValues(_.map(_._2).toVector) // handle list that might have duplicates
  }
}
