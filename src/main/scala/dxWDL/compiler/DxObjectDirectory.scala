/** Efficient lookup for dx:applets in a platform directory
  */
package dxWDL.compiler

import com.fasterxml.jackson.databind.JsonNode
import dxWDL.{Utils, Verbose}
import com.dnanexus._
import java.time.{LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import spray.json._
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
    // A map from an applet/workflow that is part of the namespace to its dx:object
    // on the target path (project/folder)
    private lazy val objDir : HashMap[String, Vector[DxObjectInfo]] = bulkLookup()

    // A map from checksum to dx:executable, across the entire project.
    private lazy val projectWideExecutableDir :
            Map[String, Vector[(DXDataObject, DXDataObject.Describe)]] = projectBulkLookup()

    private val folders = HashSet.empty[String]

    // a list of all dx:workflow and dx:applet names used in this WDL workflow
    private def allExecutableNames: Set[String] = {
        val allAppletNames: Set[String] = ns.applets.keys.toSet
        val allWorkflowNames: Set[String] = ns.listWorkflows.map(_.name).toSet

        val bothAplWf = allAppletNames.intersect(allWorkflowNames)
        if (!bothAplWf.isEmpty)
            throw new Exception(s"Illegal IR namespace, applet and workflow share names ${bothAplWf}")
        allAppletNames ++ allWorkflowNames
    }

    // Instead of looking up applets/workflows one by one, perform a bulk lookup, and
    // find all the objects in the target directory. Setup an easy to
    // use map with information on each name.
    private def bulkLookup() : HashMap[String, Vector[DxObjectInfo]] = {
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
            allExecutableNames contains name
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


    // Scan the entire project for dx:workflows and dx:applets that we
    // already created, and may be reused, instead of recompiling.
    private def projectBulkLookup() : Map[String, Vector[(DXDataObject, DXDataObject.Describe)]] = {
        val dxAppletsInProj: List[DXApplet] = DXSearch.findDataObjects()
            .inProject(dxProject)
            .withClassApplet
            .includeDescribeOutput(DXDataObject.DescribeOptions.get().withProperties())
            .execute().asList().asScala.toList
        val dxWorkflowsInProj: List[DXWorkflow] = DXSearch.findDataObjects()
            .inProject(dxProject)
            .withClassWorkflow
            .includeDescribeOutput(DXDataObject.DescribeOptions.get().withProperties())
            .execute().asList().asScala.toList

        // Leave only dx:objects that could belong to the workflow
        val dxObjects = (dxAppletsInProj ++ dxWorkflowsInProj)
            /*.filter{ dxObj =>
            val name = dxObj.getCachedDescribe().getName
            allExecutableNames contains name
        }*/

        val hm = HashMap.empty[String, Vector[(DXDataObject, DXDataObject.Describe)]]
        dxObjects.foreach{ dxObj =>
            val desc = dxObj.getCachedDescribe()
            val props: Map[String, String] = desc.getProperties().asScala.toMap
            props.get(CHECKSUM_PROP) match {
                case None => ()
                case Some(digest) =>
                    if (hm contains digest)
                        // Digest collision
                        hm(digest) :+ (dxObj, desc)
                    else
                        hm(digest) = Vector((dxObj, desc))
            }
        }
        hm.toMap
    }

    def lookup(execName: String) : Vector[DxObjectInfo] = {
        objDir.get(execName) match {
            case None => Vector.empty
            case Some(v) => v
        }
    }

    // Search for an executable named [execName], with a specific
    // checksum anywhere in the project. This could save recompilation.
    //
    // Note: in case of checksum collision, there could be several hits.
    // Return only the one that starts with the name we are looking for.
    def lookupOtherVersions(execName: String, digest: String)
            : Option[(DXDataObject, DXDataObject.Describe)] = {
        val checksumMatches = projectWideExecutableDir.get(digest) match {
            case None => return None
            case Some(vec) => vec
        }
        val checksumAndNameMatches = checksumMatches.filter{ case (dxObj, dxDesc) =>
            dxDesc.getName.startsWith(execName)
        }
        if (checksumAndNameMatches.isEmpty)
            return None
        return Some(checksumAndNameMatches.head)
    }

    def insert(name:String, dxObj:DXDataObject, digest: String) : Unit = {
        val aInfo = DxObjectInfo(name, LocalDateTime.now, dxObj, digest)
        objDir(name) = Vector(aInfo)
    }

    // create a folder, if it does not already exist.
    private def newFolder(fullPath:String) : Unit = {
        if (!(folders contains fullPath)) {
            dxProject.newFolder(fullPath, true)
            folders.add(fullPath)
        }
    }

    // Move an object into an archive directory. If the object
    // is an applet, for example /A/B/C/GLnexus, move it to
    //     /A/B/C/Applet_archive/GLnexus (Day Mon DD hh:mm:ss year)
    // If the object is a workflow, move it to
    //     /A/B/C/Workflow_archive/GLnexus (Day Mon DD hh:mm:ss year)
    //
    // Examples:
    //   GLnexus (Fri Aug 19 18:01:02 2016)
    //   GLnexus (Mon Mar  7 15:18:14 2016)
    //
    // Note: 'dx build' does not support workflow archiving at the moment.
    def archiveDxObject(objInfo:DxObjectInfo) : Unit = {
        Utils.trace(verbose.on, s"Archiving ${objInfo.name} ${objInfo.dxObj.getId}")
        val dxClass:String = objInfo.dxClass
        val destFolder = folder ++ "/." ++ dxClass + "_archive"

        // move the object to the new location
        newFolder(destFolder)
        dxProject.move(List(objInfo.dxObj).asJava, List.empty[String].asJava, destFolder)

        // add the date to the object name
        val formatter = DateTimeFormatter.ofPattern("EE MMM dd kk:mm:ss yyyy")
        val crDateStr = objInfo.crDate.format(formatter)
        val req = JsObject(
            "project" -> JsString(dxProject.getId),
            "name" -> JsString(s"${objInfo.name} ${crDateStr}")
        )

        dxClass match {
            case "Workflow" =>
                DXAPI.workflowRename(objInfo.dxObj.getId,
                                     Utils.jsonNodeOfJsValue(req),
                                     classOf[JsonNode])
            case "Applet" =>
                DXAPI.appletRename(objInfo.dxObj.getId,
                                   Utils.jsonNodeOfJsValue(req),
                                   classOf[JsonNode])
            case other => throw new Exception(s"Unkown class ${other}")
        }
    }
}
