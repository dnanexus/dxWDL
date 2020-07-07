package dx.core.util

import java.nio.file.Path

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

    lazy val readmeRegexp = s"(?i)readme\\.${wdlName}\\.(.+)\\.md".r
    lazy val developerNotesRegexp = s"(?i)readme\\.developer\\.${wdlName}\\.(.+)\\.md".r

    val v: Array[(String, AdjunctFile)] = parentDir.listFiles
      .flatMap { file =>
        {
          file.getName match {
            case readmeRegexp(target) =>
              Some(target -> Readme(SysUtils.readFileContent(file.toPath)))
            case developerNotesRegexp(target) =>
              Some(target -> DeveloperNotes(SysUtils.readFileContent(file.toPath)))
            case _ => None
          }
        }
      }
    // handle list that might have duplicates
    val m: Map[String, Array[(String, AdjunctFile)]] = v.groupBy(_._1)

    // get rid of the extra copy of the filename
    m.map {
      case (name, fileTuples) =>
        name -> fileTuples.map(_._2).toVector
    }
  }
}
