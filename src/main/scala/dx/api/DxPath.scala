package dx.api

object DxPath {
  val DX_URL_PREFIX = "dx://"

  case class DxPathComponents(name: String,
                              folder: Option[String],
                              projName: Option[String],
                              objFullName: String,
                              sourcePath: String)

  // TODO: use RuntimeExceptions for assertions
  def parse(dxPath: String): DxPathComponents = {
    // strip the prefix
    if (!dxPath.startsWith(DX_URL_PREFIX)) {
      throw new Exception(s"Path ${dxPath} does not start with prefix ${DX_URL_PREFIX}")
    }
    val s = dxPath.substring(DX_URL_PREFIX.length)

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

    // split the object path into folder/name
    val index = dxObjectPath.lastIndexOf('/')
    val (folderRaw, name) =
      if (index == -1) {
        ("/", dxObjectPath)
      } else {
        (dxObjectPath.substring(0, index), dxObjectPath.substring(index + 1))
      }

    // We don't want a folder if this is a dx-data-object (file-xxxx, record-yyyy)
    val folder =
      if (DxUtils.isDataObjectId(name)) None
      else if (folderRaw == "") Some("/")
      else Some(folderRaw)

    DxPathComponents(name, folder, projName, dxObjectPath, dxPath)
  }
}
