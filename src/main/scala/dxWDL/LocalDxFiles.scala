// A dictionary of all WDL files that are also
// platform files. These are files that have been uploaded or
// downloaded. The WDL representation is (essentially) a local filesystem path.
package dxWDL

import com.dnanexus.DXFile
import java.nio.file.{Files, Path}
import scala.collection.mutable.HashMap
import spray.json._
import Utils.{dxFileOfJsValue, downloadFile, uploadFile}
import wdl4s.wdl.values._

object LocalDxFiles {
    private val bound = HashMap.empty[Path, DXFile]
    private val reverseLookup = HashMap.empty[DXFile, Path]

    def get(dxFile: DXFile) : Option[Path] = {
        reverseLookup.get(dxFile)
    }

    def get(path: Path) : Option[DXFile] = {
        bound.get(path)
    }

    private def download(path:Path, dxFile:DXFile, force: Boolean) : Unit = {
        if (force) {
            // Download right now
            downloadFile(path, dxFile)
        } else {
            // Create an empty file, to mark the fact that the path and
            // file name are in use. We may not end up downloading the
            // file, and accessing the data, however, we need to keep
            // the path in the WdlFile value unique.
            Files.createFile(path)
            DxFunctions.registerRemoteFile(path.toString, dxFile)
        }
        bound(path) = dxFile
        reverseLookup(dxFile) = path
    }

    def upload(path: Path) : JsValue =  {
        bound.get(path) match {
            case None =>
                val jsv:JsValue = uploadFile(path)
                val dxFile = dxFileOfJsValue(jsv)
                bound(path) = dxFile
                reverseLookup(dxFile) = path
                jsv
            case Some(dxFile) =>
                Utils.jsValueOfJsonNode(dxFile.getLinkAsJson)
        }
    }

    def delete(path: Path) : Unit = {
        bound.get(path) match {
            case None => ()
            case Some(dxFile) =>
                Files.delete(path)
                bound.remove(path)
                reverseLookup.remove(dxFile)
        }
        DxFunctions.unregisterRemoteFile(path.toString)
    }

    def wdlFileOfDxLink(jsValue: JsValue, force: Boolean) : WdlValue = {
        // Download the file, and place it in a local file, with the
        // same name as the platform. All files have to be downloaded
        // into the same directory; the only exception we make is for
        // disambiguation purposes.
        val dxFile = dxFileOfJsValue(jsValue)

        val path = reverseLookup.get(dxFile) match {
            case Some(path) =>
                // We already have a local copy
                path
            case None =>
                // Need to download it
                val fName = dxFile.describe().getName()
                val shortPath = Utils.inputFilesDirPath.resolve(fName)
                val path : Path =
                    if (Files.exists(shortPath)) {
                        // Short path already exists. Note: this check is brittle in the case
                        // of concurrent downloads.
                        val fid = dxFile.getId()
                        System.err.println(s"Disambiguating file ${fid} with name ${fName}")
                        val dir:Path = Utils.inputFilesDirPath.resolve(fid)
                        Utils.safeMkdir(dir)
                        Utils.inputFilesDirPath.resolve(fid).resolve(fName)
                    } else {
                        shortPath
                    }
                download(path, dxFile, force)
                path
        }
        WdlSingleFile(path.toString)
    }
}
