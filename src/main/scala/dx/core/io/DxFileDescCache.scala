package dx.core.io

import dx.api.{DxFile, DxFileDescribe, DxProject}

case class DxFileDescCache(files: Vector[DxFile] = Vector.empty) {
  private lazy val fileAndProjectIdToDescCache: Map[(String, String), DxFileDescribe] =
    files
      .filter(f => f.hasCachedDesc && f.project.isDefined)
      .map(f => (f.id, f.project.get.id) -> f.describe())
      .toMap
  private lazy val fileIdToDescCache: Map[String, DxFileDescribe] =
    files.filter(_.hasCachedDesc).groupBy(_.id).flatMap {
      case (id, files) if files.size == 1 => Some(id -> files.head.describe())
      case (id, files) if files.size > 1 =>
        var filesWithProj = files.filter(_.project.isDefined)
        if (filesWithProj.size > 1) {
          filesWithProj = filesWithProj.filter(_.project.get.id.startsWith("project-"))
        }
        if (filesWithProj.size == 1) {
          Some(id -> filesWithProj.head.describe())
        } else {
          None
        }
      case _ => None
    }

  def get(fileId: String, projectId: Option[String] = None): Option[DxFileDescribe] = {
    projectId
      .flatMap(projId => fileAndProjectIdToDescCache.get(fileId, projId))
      .orElse(fileIdToDescCache.get(fileId))
  }

  def getCached(file: DxFile): Option[DxFileDescribe] = {
    file.project
      .flatMap(proj => fileAndProjectIdToDescCache.get((file.id, proj.id)))
      .orElse(fileIdToDescCache.get(file.id))
  }

  def updateFileFromCache(file: DxFile): DxFile = {
    if (file.hasCachedDesc) {
      file
    } else {
      getCached(file)
        .map { desc =>
          file.cacheDescribe(desc)
          // make sure the DxFile and DxFileDescribe project IDs are in sync,
          // or leave DxFile.project as None if it's not a 'project-XXX' ID
          file.project match {
            case Some(DxProject(_, id)) if id == desc.project =>
              file
            case None if !desc.project.startsWith("project-") =>
              file
            case _ =>
              DxFile(file.dxApi, file.id, Some(DxProject(file.dxApi, desc.project)))
          }
        }
        .getOrElse(file)
    }
  }
}

object DxFileDescCache {
  lazy val empty: DxFileDescCache = DxFileDescCache()
}
