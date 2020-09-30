package dx.api

object DxPath {
  val DxUriPrefix = "dx://"
  private val pathRegex = "(.*)/(.+)".r

  case class DxPathComponents(name: String,
                              folder: Option[String],
                              projName: Option[String],
                              objFullName: String,
                              sourcePath: String)

  def parse(dxPath: String): DxPathComponents = {
    // strip the prefix
    val s = if (dxPath.startsWith(DxUriPrefix)) {
      dxPath.substring(DxUriPrefix.length)
    } else {
      dxPath
    }

    // take out the project, if it is specified
    val components = s.split(":").toList
    val (projName, dxObjectPath) = components match {
      case Nil =>
        throw new Exception(s"Path ${dxPath} is invalid")
      case List(objName) =>
        (None, objName)
      case projName :: tail =>
        val rest = tail.mkString(":")
        (Some(projName), rest)
    }

    projName match {
      case None => ()
      case Some(proj) =>
        if (proj.startsWith("file-"))
          throw new Exception("""|Path ${dxPath} does not look like: dx://PROJECT_NAME:/FILE_PATH
                                 |For example:
                                 |   dx://dxWDL_playground:/test_data/fileB
                                 |""".stripMargin)
    }

    val (folder, name) = dxObjectPath match {
      case pathRegex(_, name) if DxUtils.isDataObjectId(name) => (None, name)
      case pathRegex(folder, name) if folder == ""            => (Some("/"), name)
      case pathRegex(folder, name)                            => (Some(folder), name)
      case _ if DxUtils.isDataObjectId(dxObjectPath)          => (None, dxObjectPath)
      case _                                                  => (Some("/"), dxObjectPath)
    }

    DxPathComponents(name, folder, projName, dxObjectPath, dxPath)
  }
}
