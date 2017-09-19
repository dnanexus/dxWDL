/** Efficient lookup for dx:applets in a platform directory
  */
package dxWDL

import com.dnanexus.{DXApplet, DXDataObject, DXProject, DXSearch, DXWorkflow}
import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import Utils.CHECKSUM_PROP

// Keep all the information about an applet in packaged form
case class DxObjectInfo(name:String,
                        dxObj:DXDataObject,
                        digest: String) {
    lazy val kind = dxObj.getClass.getSimpleName
}

// Take a snapshot of the platform target path before the build starts.
// Make an efficient directory of all the applets that exist there. Update
// the directory when an applet is compiled.
case class DxObjectDirectory(ns: IR.Namespace,
                             dxProject:DXProject,
                             folder: String,
                             verbose: Utils.Verbose) {
    private lazy val objDir : HashMap[String, Vector[DxObjectInfo]] = bulkLookup()

    // Instead of looking applets/workflows one by one, perform a bulk lookup, and
    // find all the objects in the target directory. Setup an easy to
    // use map with information on each name.
    private def bulkLookup() : HashMap[String, Vector[DxObjectInfo]] = {
        // get all the applet names
        val allAppletNames: Set[String] = ns.applets.map{ case (k,_) => k }.toSet

        // all workflow names: currently just the top one (later, sub-workflows)
        val allWorkflowNames: Set[String] = ns.workflow match {
            case None => Set.empty
            case Some(x) => Set(x.name)
        }
        val bothAplWf = allAppletNames.intersect(allWorkflowNames)
        if (!bothAplWf.isEmpty)
            throw new Exception(s"Illegal IR namespace, applet and workflow share names ${bothAplWf}")
        val allNames = allAppletNames ++ allWorkflowNames

        val dxAppletsInFolder: List[DXApplet] = DXSearch.findDataObjects()
            .inFolder(dxProject, folder)
            .withClassApplet
            .includeDescribeOutput(DXDataObject.DescribeOptions.get().withProperties())
            .execute().asList().asScala.toList
        val dxWorkflowsInFolder: List[DXWorkflow] = DXSearch.findDataObjects()
            .inFolder(dxProject, folder)
            .withClassWorkflow
            .includeDescribeOutput(DXDataObject.DescribeOptions.get().withProperties())
            .execute().asList().asScala.toList

        // Leave only dx:objects that could belong to the workflow
        val dxObjects = (dxAppletsInFolder ++ dxWorkflowsInFolder).filter{ dxObj =>
            val name = dxObj.getCachedDescribe().getName
            allNames contains name
        }

        val dxObjectList: List[DxObjectInfo] = dxObjects.map{ dxObj =>
            val desc = dxObj.getCachedDescribe()
            val name = desc.getName()
            val props: Map[String, String] = desc.getProperties().asScala.toMap
            val digest:String = props.get(CHECKSUM_PROP) match {
                case None =>
                    System.err.println(
                        s"""|object ${name} has no checksum, and is invalid. It was probably
                            |not created with dxWDL. Please remove it with:
                            |dx rm ${dxProject}:${folder}/${name}
                            |""".stripMargin.trim)
                    throw new Exception("Encountered invalid applet, not created with dxWDL")
                case Some(x) => x
            }
            DxObjectInfo(name, dxObj, digest)
        }

        // There could be multiple versions of the same applet/workflow, collect their
        // information in vectors
        val hm = HashMap.empty[String, Vector[DxObjectInfo]]
        dxObjectList.foreach{ case dxObjInfo =>
            val name = dxObjInfo.name
            hm.get(name) match {
                case None =>
                    // first time we have seen this dx:object
                    hm(name) = Vector(dxObjInfo)
                case Some(vec) =>
                    // there is already at least one dx:object by this name
                    hm(name) = hm(name) :+ dxObjInfo
            }
        }
        hm
    }

    def lookup(aplName: String) : Vector[DxObjectInfo] = {
        objDir.get(aplName) match {
            case None => Vector.empty
            case Some(v) => v
        }
    }

    def insert(name:String, dxObj:DXDataObject, digest: String) : Unit = {
        val aInfo = DxObjectInfo(name, dxObj, digest)
        objDir(name) = Vector(aInfo)
    }
}
