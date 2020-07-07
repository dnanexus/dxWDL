// FURL:  File Uniform Resource Locator
//
// In WDL, a file is represented by a string. This encodes
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

package dx.core.io

import java.nio.file.{Path, Paths}

import dx.api.{DxApi, DxFile, DxFileDescribe, DxPath, DxProject}

sealed trait Furl {
  def toString: String
}

case class FurlLocal(path: Path) extends Furl {
  override def toString: String = path.toString
}
case class FurlDx(value: String, dxProj: Option[DxProject], dxFile: DxFile) extends Furl {
  override def toString: String = value
}

object Furl {
  // From a string such as:
  //    dx://proj-xxxx:file-yyyy::/A/B/C.txt
  //    dx://proj-xxxx:file-yyyy
  //
  // Get the project and file.

  private def resolveFileUrl(buf: String, dxApi: DxApi): FurlDx = {
    val s = buf.substring(DxPath.DX_URL_PREFIX.length)
    val proj_file = s.split("::")(0)
    val dxFile = dxApi.resolveDxUrlFile(s"${DxPath.DX_URL_PREFIX}${proj_file}")
    dxFile.project match {
      case None =>
        FurlDx(buf, None, dxFile)
      case Some(proj: DxProject) if proj.id.startsWith("project-") =>
        FurlDx(buf, Some(proj), dxFile)
      case Some(DxProject(_, id)) if id.startsWith("container-") =>
        FurlDx(buf, None, dxFile)
      case _ =>
        throw new Exception(s"Invalid path ${buf}")
    }
  }

  def fromUrl(url: String, dxApi: DxApi): Furl = {
    url match {
      case _ if url.startsWith(DxPath.DX_URL_PREFIX) =>
        resolveFileUrl(url, dxApi)
      case _ if url contains "://" =>
        throw new Exception(s"protocol not supported, cannot access ${url}")
      case _ =>
        // A local file
        FurlLocal(Paths.get(url))
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
  def fromDxFile(dxFile: DxFile, fileInfoDir: Map[String, (DxFile, DxFileDescribe)]): FurlDx = {
    // Try the cache first; if the file isn't there, submit an API call.
    val (folder, name) = fileInfoDir.get(dxFile.id) match {
      case None =>
        val desc = dxFile.describe()
        (desc.folder, desc.name)
      case Some((_, miniDesc)) =>
        (miniDesc.folder, miniDesc.name)
    }
    val logicalName = s"${folder}/${name}"
    val fid = dxFile.getId
    dxFile.project match {
      case None =>
        FurlDx(s"${DxPath.DX_URL_PREFIX}${fid}::${logicalName}", None, dxFile)
      case Some(proj) =>
        val projId = proj.getId
        FurlDx(s"${DxPath.DX_URL_PREFIX}${projId}:${fid}::${logicalName}", Some(proj), dxFile)
    }
  }
}
