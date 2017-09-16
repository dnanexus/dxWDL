/** Efficient lookup for dx:applets in a platform directory
  */
package dxWDL

import com.dnanexus.{DXApplet, DXDataObject, DXProject, DXSearch}
import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import Utils.CHECKSUM_PROP

// Keep all the information about an applet in packaged form
case class AppletInfo(name:String, applet:DXApplet, digest: String)

// Take a snapshot of the platform target path before the build starts.
// Make an efficient directory of all the applets that exist there. Update
// the directory when an applet is compiled.
case class AppletDirectory(ns: IR.Namespace,
                           dxProject:DXProject,
                           folder: String,
                           verbose: Utils.Verbose) {
    private lazy val appletDir : HashMap[String, Vector[AppletInfo]] = bulkAppletLookup()

    // Instead of looking applets one by one, perform a bulk lookup, and
    // find all the applets in the target directory. Setup an easy to
    // use map with information on each applet name.
    private def bulkAppletLookup() : HashMap[String, Vector[AppletInfo]] = {
        // get all the applet names
        val allAppletNames: Vector[String] = ns.applets.map{ case (k,_) => k }.toVector

        val dxAppletsInFolder: List[DXApplet] = DXSearch.findDataObjects()
            .inFolder(dxProject, folder)
            .withClassApplet()
            .includeDescribeOutput(DXDataObject.DescribeOptions.get().withProperties())
            .execute().asList().asScala.toList

        // Leave only applets that could belong to the workflow
        val dxApplets = dxAppletsInFolder.filter{ dxApl =>
            val name = dxApl.getCachedDescribe().getName
            allAppletNames contains name
        }

        // filter out an
        val aplInfoList: List[AppletInfo] = dxApplets.map{ dxApl =>
            val desc = dxApl.getCachedDescribe()
            val name = desc.getName()
            val props: Map[String, String] = desc.getProperties().asScala.toMap
            val digest:String = props.get(CHECKSUM_PROP) match {
                case None =>
                    System.err.println(
                        s"""|Applet ${name} has no checksum, and is invalid. It was probably
                            |not created with dxWDL. Please remove it with:
                            |dx rm ${dxProject}:${folder}/${name}
                            |""".stripMargin.trim)
                    throw new Exception("Encountered invalid applet, not created with dxWDL")
                case Some(x) => x
            }
            AppletInfo(name, dxApl, digest)
        }

        // There could be multiple versions of the same applet, collect their
        // information in vectors
        val hm = HashMap.empty[String, Vector[AppletInfo]]
        aplInfoList.foreach{ case aplInfo =>
            val name = aplInfo.name
            hm.get(name) match {
                case None =>
                    // first time we have seen this applet
                    hm(name) = Vector(aplInfo)
                case Some(vec) =>
                    // there is already at least one applet by this name
                    hm(name) = hm(name) :+ aplInfo
            }
        }
        hm
    }

    def lookup(aplName: String) : Vector[AppletInfo] = {
        appletDir.get(aplName) match {
            case None => Vector.empty
            case Some(v) => v
        }
    }

    def insert(name:String, applet:DXApplet, digest: String) : Unit = {
        val aInfo = AppletInfo(name, applet, digest)
        appletDir(name) = Vector(aInfo)
    }
}
