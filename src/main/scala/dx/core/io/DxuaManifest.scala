package dx.core.io

import java.nio.file.Path

import dx.api.DxApi
import spray.json._

case class DxuaManifest(value: JsObject)

/**
  * Creates a DxuaManifest. Assumes all files are going to the root
  * directory of the current project.
  * @param dxApi DxApi
  */
case class DxuaManifestBuilder(dxApi: DxApi) {
  def apply(files: IterableOnce[Path]): DxuaManifest = {
    val entries = files.iterator.map { path =>
      val name = path.getFileName.toString
      JsObject("path" -> JsString(path.toString),
               "name" -> JsString(name),
               "folder" -> JsString("/"))
    }.toVector
    DxuaManifest(JsObject(dxApi.currentProject.id -> JsArray(entries)))
  }
}
