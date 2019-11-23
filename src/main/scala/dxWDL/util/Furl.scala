// FURL:  File Uniform Resource Locator
//
// In wom, a file is represented by a string. This encodes
// several different access protocols, filesystems, and clouds.
//
//  file name                              location
//  ---------                              --------
//  gs://bucket/foo.bin                    google cloud
//  http://A/B/C.html                      URL
//  /foo/bar.bam                           local file, absolute path
//  ../foo/bar.bam                         local file, relative path
//  dx://proj-xxxx:file-yyyy               DNAx file, no filename provided
//  dx://proj-xxxx:file-yyyy::/A/B/C.txt   DNAx file, including path and filename
//  dx://proj-xxxx:record-yyyy             DNAx record-id

package dxWDL.util

import dxWDL.base.Utils
import dxWDL.base.Utils.DX_URL_PREFIX
import dxWDL.dx._

sealed trait Furl

case class FurlLocal(path : String) extends Furl
case class FurlDx(value : String,
                  dxProj : Option[DxProject],
                  dxFile: DxFile) extends Furl

object Furl {
    // From a string such as:
    //    dx://proj-xxxx:file-yyyy::/A/B/C.txt
    //    dx://proj-xxxx:file-yyyy
    //
    // Get the project and file.
    private def parseDxFurl(buf: String) : FurlDx = {
        val s = buf.substring(DX_URL_PREFIX.length)
        val proj_file = s.split("::")(0)
        val dxFile = DxPath.resolveDxURLFile(DX_URL_PREFIX + proj_file)
        dxFile.project match {
            case None =>
                FurlDx(buf, None, dxFile)
            case Some(DxProject(proj)) if proj.startsWith("project-") =>
                FurlDx(buf, Some(DxProject(proj)), dxFile)
            case Some(DxProject(proj)) if proj.startsWith("container-") =>
                FurlDx(buf, None, dxFile)
            case _ =>
                throw new Exception(s"Invalid path ${buf}")
        }
    }

    def parse(path: String) : Furl = {
        path match {
            case _ if path.startsWith(Utils.DX_URL_PREFIX) =>
                parseDxFurl(path)
            case _ if path contains "://" =>
                throw new Exception(s"protocol not supported, cannot access ${path}")
            case _ =>
                // A local file
                FurlLocal(path)
        }
    }

    // Convert a dx-file to a string with the format:
    //   dx://proj-xxxx:file-yyyy::/A/B/C.txt
    //
    // This is needed for operations like:
    //     File filename
    //     String  = sub(filename, ".txt", "") + ".md"
    // The standard library functions requires the file name to
    // end with a posix-like name. It can't just be:
    // "dx://file-xxxx", or "dx://proj-xxxx:file-yyyy". It needs
    // to be something like:  dx://xxxx:yyyy:genome.txt, so that
    // we can change the suffix.
    //
    // We need to change the standard so that the conversion from file to
    // string is well defined, and requires an explicit conversion function.
    //
    def dxFileToFurl(dxFile: DxFile,
                     fileInfoDir: Map[DxFile, DxDescribe]) : FurlDx = {
        // Try the cache first; if the file isn't there, submit an API call.
        val (folder, name) = fileInfoDir.get(dxFile) match {
            case None =>
                val desc = dxFile.describe
                (desc.folder, desc.name)
            case Some(miniDesc) =>
                (miniDesc.folder, miniDesc.name)
        }
        val logicalName = s"${folder}/${name}"
        val fid = dxFile.getId
        dxFile.project match {
            case None =>
                FurlDx(s"${DX_URL_PREFIX}${fid}::${logicalName}",
                       None,
                       dxFile)
            case Some(proj) =>
                val projId = proj.getId
                FurlDx(s"${DX_URL_PREFIX}${projId}:${fid}::${logicalName}",
                       Some(DxProject.getInstance(projId)),
                       dxFile)
        }
    }
}
