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

import com.dnanexus.{DXFile, DXProject}
import Utils.DX_URL_PREFIX
import dxWDL.util.DxBulkDescribe.MiniDescribe

sealed trait Furl

case class FurlLocal(path : String) extends Furl
case class FurlDx(value : String,
                  dxProj : Option[DXProject],
                  dxFile: DXFile) extends Furl

object Furl {
    // From a string such as:
    //    dx://proj-xxxx:file-yyyy::/A/B/C.txt
    //    dx://proj-xxxx:file-yyyy
    //
    // Get the project and file.
    private def parseDxFurl(buf: String) : FurlDx = {
        val s = buf.substring(DX_URL_PREFIX.length)
        val proj_file = s.split("::")(0)
        val words = proj_file.split(":")
        words.size match {
            case 1 =>
                val dxFile = DXFile.getInstance(words(0))
                FurlDx(buf, None, dxFile)
            case 2 =>
                val dxProj = DXProject.getInstance(words(0))
                val dxFile = DXFile.getInstance(words(1))
                FurlDx(buf, Some(dxProj), dxFile)
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
    def dxFileToFurl(dxFile: DXFile,
                     infoCache: Map[DXFile, MiniDescribe]) : FurlDx = {
        // Try to do a cache lookup, perform an API call only
        // if the cache doesn't have the file
        val (folder, name) = infoCache.get(dxFile) match {
            case None =>
                val desc = dxFile.describe
                (desc.getFolder, desc.getName)
            case Some(miniDesc) =>
                (miniDesc.folder, miniDesc.name)
        }
        val logicalName = s"${folder}/${name}"
        val fid = dxFile.getId
        val proj = dxFile.getProject
        if (proj == null) {
            FurlDx(s"${DX_URL_PREFIX}${fid}::${logicalName}",
                   None,
                   dxFile)
        } else {
            val projId = proj.getId
            FurlDx(s"${DX_URL_PREFIX}${projId}:${fid}::${logicalName}",
                   Some(DXProject.getInstance(projId)),
                   dxFile)
        }
    }
}
