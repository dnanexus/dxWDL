// A dictionary of all WDL files that are also
// platform files. These are files that have been uploaded or
// downloaded. The WDL representation is (essentially) a local filesystem path.
package dxWDL

import com.dnanexus.DXFile
import java.nio.file.{Files, Path, Paths}
import scala.collection.mutable.HashMap
import spray.json._
import Utils.{dxFileFromJsValue, getMetaDirPath, jsValueOfJsonNode,
    LOCAL_DX_FILES_CHECKPOINT_FILE, readFileContent, writeFileContent}
import wdl4s.wdl.values._

object LocalDxFiles {
    // A file can be in two possible states
    //  Local: has been downloaded in full to the local machine
    //  Remote: an empty file has been created locally in the correct
    //     filesystem path. The data has not been downloaded. Metadata
    //     operations such as "get file size" are performed via API
    //     calls.
    object FileState extends Enumeration {
        val Local, Remote = Value
    }
    case class FileInfo(state: FileState.Value, dxFile: DXFile)

    object FileInfo {
        def toJSON(fInfo: FileInfo) : JsValue = {
            JsObject(
                "state" -> JsString(fInfo.state.toString),
                "dxFile" -> jsValueOfJsonNode(fInfo.dxFile.getLinkAsJson)
            )
        }

        def fromJSON(jsv: JsValue) : FileInfo = {
            jsv.asJsObject.getFields("state", "dxFile") match {
                case Seq(JsString(s), dxLink) =>
                    val state = FileState.values find (_.toString == s)
                    val state2 = state match {
                        case None => throw new Exception(s"Could not unmarshal ${jsv}")
                        case Some(x) => x
                    }
                    FileInfo(state2, dxFileFromJsValue(dxLink))
                case _ =>
                    throw new Exception(s"malformed FileInfo structure ${jsv}")
            }
        }
    }

    // Cloud files that have been localized to this instance
    private val localized = HashMap.empty[Path, FileInfo]

    // An efficent index to get from a [dx:file] to its localized
    // path
    private val reverseLookup = HashMap.empty[DXFile, Path]

    // Checkpoint the mapping tables
    //
    // This is done when closing the Runner. For example, between
    // the task-runner prolog and epilog.
    //
    // Convert the [localized] table to JSON, and write it
    // out to a checkpoint file.
    def freeze() : Unit = {
        val m:Map[String, JsValue] = localized.map{ case (path, fInfo) =>
            path.toString -> FileInfo.toJSON(fInfo)
        }.toMap
        val buf = JsObject(m).prettyPrint
        val path = getMetaDirPath().resolve(LOCAL_DX_FILES_CHECKPOINT_FILE)
        writeFileContent(path, buf)
    }

    // Read the checkpoint, and repopulate the tables
    def unfreeze() : Unit = {
        val path = getMetaDirPath().resolve(LOCAL_DX_FILES_CHECKPOINT_FILE)
        val buf = readFileContent(path)
        val m: Map[String, FileInfo] = buf.parseJson match {
            case JsObject(fields) =>
                fields.map{
                    case (k,vJs) => k -> FileInfo.fromJSON(vJs)
                }.toMap
            case other =>
                throw new Exception(s"Deserialization error, checkpoint=${other.prettyPrint}")
        }

        // repopulate tables.
        m.foreach{ case(p, fInfo) =>
            val path = Paths.get(p)
            localized(path) = fInfo
            reverseLookup(fInfo.dxFile) = path
        }
    }

    def get(dxFile: DXFile) : Option[Path] = {
        reverseLookup.get(dxFile)
    }

    def get(path: Path) : Option[DXFile] = {
        localized.get(path).map{ fInfo => fInfo.dxFile }
    }

    def upload(path: Path) : JsValue =  {
        localized.get(path) match {
            case None =>
                // Upload a local file to the cloud. This is
                // not a file we downloaded previously.
                val jsv:JsValue = Utils.uploadFile(path)
                val dxFile = dxFileFromJsValue(jsv)
                val fInfo = FileInfo(FileState.Local, dxFile)
                localized(path) = fInfo
                reverseLookup(dxFile) = path
                jsv
            case Some(FileInfo(_,dxFile)) =>
                // The file already exists in the cloud, there
                // is no need to upload again.
                jsValueOfJsonNode(dxFile.getLinkAsJson)
        }
    }

    def delete(path: Path) : Unit = {
        localized.get(path) match {
            case None => ()
            case Some(FileInfo(_,dxFile)) =>
                Files.delete(path)
                localized.remove(path)
                reverseLookup.remove(dxFile)
        }
        DxFunctions.unregisterRemoteFile(path.toString)
    }

    def download(jsValue: JsValue, force: Boolean) : WdlValue = {
        // Download the file, and place it in a local file, with the
        // same name as the platform. All files have to be downloaded
        // into the same directory; the only exception we make is for
        // disambiguation purposes.
        val dxFile = dxFileFromJsValue(jsValue)

        val path = reverseLookup.get(dxFile) match {
            case Some(path) =>
                val fInfo:FileInfo = localized.get(path).get
                fInfo.state match {
                    case FileState.Local =>
                        // the file has already been downloaded
                        ()
                    case FileState.Remote if force =>
                        // We haven't downloaded the file data itself, but
                        // we do have a sentinal in place (empty file).
                        //
                        // First, remove the path because otherwise,
                        // the download will fail.
                        Files.delete(path)
                        Utils.downloadFile(path, dxFile)
                        localized(path) = FileInfo(FileState.Local, dxFile)
                    case FileState.Remote if !force =>
                        // the file is remote, but we don't want to download it
                        // right now
                        ()
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
                val fState = if (force) {
                    // Download right now
                    Utils.downloadFile(path, dxFile)
                    FileState.Local
                } else {
                    // Create an empty file, to mark the fact that the path and
                    // file name are in use. We may not end up downloading the
                    // file, and accessing the data, however, we need to keep
                    // the path in the WdlFile value unique.
                    Files.createFile(path)
                    DxFunctions.registerRemoteFile(path.toString, dxFile)
                    FileState.Remote
                }
                localized(path) = FileInfo(fState, dxFile)
                reverseLookup(dxFile) = path
                path
        }
        WdlSingleFile(path.toString)
    }
}
