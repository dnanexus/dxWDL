// A dictionary of all WDL files that are also
// platform files. These are files that have been uploaded or
// downloaded. The WDL representation is (essentially) a local filesystem path.
package dxWDL

import com.dnanexus.DXFile
import java.nio.file.{Files, Path, Paths}
import scala.collection.mutable.HashMap
import spray.json._
import Utils.{dxFileOfJsValue, downloadFile, getMetaDirPath, jsValueOfJsonNode,
    LOCAL_DX_FILES_CHECKPOINT_FILE, uploadFile, readFileContent, writeFileContent}
import wdl4s.wdl.values._

object LocalDxFiles {
    // Cloud files that have been localized to this instance
    private val localized = HashMap.empty[Path, DXFile]
    private val reverseLookup = HashMap.empty[DXFile, Path]

    // Checkpoint the mapping tables
    //
    // This is done when closing the Runner. For example, between
    // the task-runner prolog and epilog.
    //
    // Convert the [localized] table to JSON, and write it
    // out to a checkpoint file.
    def freeze() : Unit = {
        val m:Map[String, JsValue] = localized.map{ case (path, dxFile) =>
            path.toString -> jsValueOfJsonNode(dxFile.getLinkAsJson)
        }.toMap
        val buf = JsObject(m).prettyPrint
        val path = getMetaDirPath().resolve(LOCAL_DX_FILES_CHECKPOINT_FILE)
        writeFileContent(path, buf)
    }

    // Read the checkpoint, and repopulate the tables
    def unfreeze() : Unit = {
        val path = getMetaDirPath().resolve(LOCAL_DX_FILES_CHECKPOINT_FILE)
        val buf = readFileContent(path)
        val m: Map[String, DXFile] = buf.parseJson match {
            case JsObject(fields) =>
                fields.map{
                    case (k,vJs) => k -> dxFileOfJsValue(vJs)
                }.toMap
            case other =>
                throw new Exception(s"Deserialization error, checkpoint=${other.prettyPrint}")
        }

        // repopulate tables.
        m.foreach{ case(p, dxFile) =>
            val path = Paths.get(p)
            localized(path) = dxFile
            reverseLookup(dxFile) = path
        }
    }

    def get(dxFile: DXFile) : Option[Path] = {
        reverseLookup.get(dxFile)
    }

    def get(path: Path) : Option[DXFile] = {
        localized.get(path)
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
        localized(path) = dxFile
        reverseLookup(dxFile) = path
    }

    def upload(path: Path) : JsValue =  {
        localized.get(path) match {
            case None =>
                val jsv:JsValue = uploadFile(path)
                val dxFile = dxFileOfJsValue(jsv)
                localized(path) = dxFile
                reverseLookup(dxFile) = path
                jsv
            case Some(dxFile) =>
                jsValueOfJsonNode(dxFile.getLinkAsJson)
        }
    }

    def delete(path: Path) : Unit = {
        localized.get(path) match {
            case None => ()
            case Some(dxFile) =>
                Files.delete(path)
                localized.remove(path)
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
                if (DxFunctions.isRemote(dxFile)) {
                    // We haven't downloaded the file data itself
                    download(path, dxFile, force)
                }
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
