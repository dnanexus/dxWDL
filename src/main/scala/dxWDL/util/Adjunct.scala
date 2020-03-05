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

  // Given the path to a primary (WDL file), search in the same directory (the path's parent)
  // for adjunct files. The currently recognizd adjuncts are:
  // * readme.<wdlname>.<task|workflow>.md
  // * readme.developer.<wdlname>.<task|workflow>.md
  //
  // Note: we rely on the restriction, imposed by Top.mergeIntoOneBundle, that all task/workflow
  // names are unique; otherwise, we would need for the key in the returned map to be
  // (file_name, callable_name) to ensure uniqueness.
  def findAdjunctFiles(path: Path): Map[String, Vector[AdjunctFile]] = {
    if (!path.isAbsolute) {
      throw new Exception(s"path is not absolute: ${path}")
    }

    val file = path.toFile
    val parentDir = file.getParentFile
    var wdlName = file.getName

    if (wdlName.endsWith(".wdl")) {
      wdlName = wdlName.dropRight(4)
    }

    val readmeRegexp = s"(?i)readme\\.${wdlName}\\.(.+)\\.md".r
    val developerNotesRegexp = s"(?i)readme\\.developer\\.${wdlName}\\.(.+)\\.md".r

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
