/** Efficient lookup for dx:applets in a platform directory
  */
package dxWDL.compiler

import dxWDL.{Utils, Verbose}
import com.dnanexus.{DXApplet, DXDataObject, DXProject, DXSearch, DXWorkflow}
import java.time.{LocalDateTime, ZoneId}
import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import Utils.CHECKSUM_PROP

// Keep all the information about an applet in packaged form
case class DxObjectInfo(name:String,
                        crDate: LocalDateTime,
                        dxObj:DXDataObject,
                        digest: String) {
    lazy val dxClass:String =
        dxObj.getClass.getSimpleName match {
            case "DXWorkflow" => "Workflow"
            case "DXApplet" => "Applet"
            case other => other
        }
}

// Take a snapshot of the platform target path before the build starts.
// Make an efficient directory of all the applets that exist there. Update
// the directory when an applet is compiled.
case class DxObjectDirectory(ns: IR.Namespace,
                             dxProject:DXProject,
                             folder: String,
                             verbose: Verbose) {
    private lazy val objDir : HashMap[String, Vector[DxObjectInfo]] = bulkLookup()
    private val folders = HashSet.empty[String]

    // Instead of looking applets/workflows one by one, perform a bulk lookup, and
    // find all the objects in the target directory. Setup an easy to
    // use map with information on each name.
    private def bulkLookup() : HashMap[String, Vector[DxObjectInfo]] = {
        // make a list of all expected workflow and applet names
        val allAppletNames: Set[String] = IR.listApplets(ns).map(_.name).toSet
        val allWorkflowNames: Set[String] = IR.listWorkflows(ns).map(_.name).toSet

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
            val crDate = desc.getCreationDate
            val crLdt:LocalDateTime = LocalDateTime.ofInstant(crDate.toInstant(), ZoneId.systemDefault())

            val props: Map[String, String] = desc.getProperties().asScala.toMap
            props.get(CHECKSUM_PROP) match {
                case None =>
                    None
                case Some(digest) =>
                    Some(DxObjectInfo(name, crLdt, dxObj, digest))
            }
        }.flatten

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
        val aInfo = DxObjectInfo(name, LocalDateTime.now, dxObj, digest)
        objDir(name) = Vector(aInfo)
    }

    // create a folder, if it does not already exist.
    def newFolder(fullPath:String) : Unit = {
        if (!(folders contains fullPath)) {
            dxProject.newFolder(fullPath, true)
            folders.add(fullPath)
        }
    }
}
