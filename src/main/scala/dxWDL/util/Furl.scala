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

import com.dnanexus.DXFile
import Utils.DX_URL_PREFIX


sealed trait Furl

case class FurlLocal(path : String) extends Furl
case class FurlDx(value : String) extends Furl

object Furl {
    def parse(path: String) : Furl = {
        path match {
            case _ if path.startsWith(Utils.DX_URL_PREFIX) =>
                FurlDx(path)
            case _ if path contains "://" =>
                throw new Exception(s"protocol not supported, cannot access ${path}")
            case _ =>
                // A local file
                FurlLocal(path)
        }
    }
}

object FurlDx {
    def getDxFile(furlDx: FurlDx) : DXFile =
        DxPath.lookupDxURLFile(furlDx.value)

    def components(furlDx: FurlDx) : (String, DXFile) = {
        val dxFile = DxPath.lookupDxURLFile(furlDx.value)

        // strip the 'dx://' prefix
        assert(furlDx.value.startsWith(DX_URL_PREFIX))
        val s = furlDx.value.substring(DX_URL_PREFIX.length)
        val index = s.lastIndexOf("::")
        val basename =
            if (index == -1) {
                // We don't have the file name, we need to perform an API call
                dxFile.describe.getName
            } else {
                // From a string such as: dx://proj-xxxx:file-yyyy::/A/B/C.txt
                // extract /A/B/C.txt, and then C.txt.
                val fullName = s.substring(index + 2)
                fullName.substring(fullName.lastIndexOf("/") + 1)
            }
        (basename, dxFile)
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
    def dxFileToFurl(dxFile: DXFile) : FurlDx = {
        val desc = dxFile.describe
        val logicalName = s"${desc.getFolder}/${desc.getName}"
        val fid = dxFile.getId
        val proj = dxFile.getProject
        val strRepr = if (proj == null) {
            s"${DX_URL_PREFIX}${fid}::${logicalName}"
        } else {
            val projId = proj.getId
            s"${DX_URL_PREFIX}${projId}:${fid}::${logicalName}"
        }
        FurlDx(strRepr)
    }
}
