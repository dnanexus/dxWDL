/** Efficient lookup for dx:applets in a platform directory
  */
package dxWDL.compiler

import com.fasterxml.jackson.databind.JsonNode
import com.dnanexus._
import java.time.{LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import spray.json._

import dxWDL.base._
import dxWDL.base.Utils.CHECKSUM_PROP
import dxWDL.dx.{DxUtils, DxDescribe, DxFindDataObjects}

// Keep all the information about an applet in packaged form
case class DxObjectInfo(name :String,
                        crDate : LocalDateTime,
                        dxObj : DXDataObject,
                        digest : Option[String]) {
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
case class DxObjectDirectory(ns: IR.Bundle,
                             dxProject:DXProject,
                             folder: String,
                             projectWideReuse: Boolean,
                             verbose: Verbose) {
    // A map from an applet/workflow that is part of the namespace to its dx:object
    // on the target path (project/folder)
    private val objDir : HashMap[String, Vector[DxObjectInfo]] = bulkLookup()

    // A map from checksum to dx:executable, across the entire
    // project.  It allows reusing dx:executables across the entire
    // project, at the cost of a potentially expensive API call. It is
    // not clear this is useful to the majority of users, so it is
    // gated by the [projectWideReuse] flag.
    private val projectWideExecutableDir :
            Map[String, Vector[(DXDataObject, DxDescribe)]] =
        if (projectWideReuse) projectBulkLookup()
        else Map.empty

    private val folders = HashSet.empty[String]

    // a list of all dx:workflow and dx:applet names used in this WDL workflow
    private def allExecutableNames: Set[String] = {
        ns.allCallables.keys.toSet
    }

    // Instead of looking up applets/workflows one by one, perform a bulk lookup, and
    // find all the objects in the target directory. Setup an easy to
    // use map with information on each name.
    //
    // findDataObjects can be an expensive call, both on the server and client sides.
    // We could limit it by filtering on the CHECKSUM property, which is attached only to generated
    // applets and workflows. However, this would miss cases where an applet name is already in
    // use by a regular dnanexus applet/workflow.
    private def bulkLookup() : HashMap[String, Vector[DxObjectInfo]] = {
        val t0 = System.nanoTime()
        val dxObjectsInFolder: Map[DXDataObject, DxDescribe] =
            DxFindDataObjects(None, verbose).apply(dxProject, Some(folder), false, None)
        val nrApplets = dxObjectsInFolder.count{ case (dxobj, _) => dxobj.isInstanceOf[DXApplet] }
        val nrWorkflows = dxObjectsInFolder.count{ case (dxobj, _) => dxobj.isInstanceOf[DXWorkflow] }
        val t1 = System.nanoTime()
        val diffMSec = (t1 -t0) / (1000 * 1000)
        Utils.trace(verbose.on,
                    s"""|Found ${nrApplets} applets and ${nrWorkflows}
                        |workflows in ${dxProject.getId} folder=${folder} (${diffMSec} millisec)"""
                        .stripMargin.replaceAll("\n", " "))

        // Leave only dx:objects that could belong to the workflow
        val dxObjects = dxObjectsInFolder.filter{ case (dxObj, desc) =>
            val appletOrWorkflow = dxObj.isInstanceOf[DXApplet] || dxObj.isInstanceOf[DXWorkflow]
            (allExecutableNames contains desc.name) && appletOrWorkflow
        }

        val dxObjectList: List[DxObjectInfo] = dxObjects.map{
            case (dxObj, desc) =>
                val crLdt:LocalDateTime = LocalDateTime.ofInstant(desc.creationDate.toInstant(),
                                                                  ZoneId.systemDefault())
                val chksum = desc.properties.get(CHECKSUM_PROP)
                DxObjectInfo(desc.name, crLdt, dxObj, chksum)
        }.toList

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
    //
    // Note: This could be expensive, and the maximal number of replies
    // is (by default) 1000. Since the index is limited, we may miss
    // miss matches when we search. The cost would be creating a
    // dx:executable again, which is acceptable.
    //
    // findDataObjects can be an expensive call, both on the server and client sides.
    // We limit it by filtering on the CHECKSUM property, which is attached only to generated
    // applets and workflows.
    private def projectBulkLookup() : Map[String, Vector[(DXDataObject, DxDescribe)]] = {
        val t0 = System.nanoTime()
        val dxAppletsInProject: Map[DXDataObject, DxDescribe] =
            DxFindDataObjects(None, verbose).apply(dxProject, None, true, Some("applet"))
        val nrApplets = dxAppletsInProject.size
        val t1 = System.nanoTime()
        val diffMSec = (t1 -t0) / (1000 * 1000)
        Utils.trace(verbose.on,
                    s"Found ${nrApplets} applets in project ${dxProject.getId} (${diffMSec} millisec)")

        val hm = HashMap.empty[String, Vector[(DXDataObject, DxDescribe)]]
        dxAppletsInProject.foreach{ case (dxObj, desc) =>
            desc.properties.get(CHECKSUM_PROP) match {
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
            : Option[(DXDataObject, DxDescribe)] = {
        val checksumMatches = projectWideExecutableDir.get(digest) match {
            case None => return None
            case Some(vec) => vec
        }
        val checksumAndNameMatches = checksumMatches.filter{ case (dxObj, dxDesc) =>
            dxDesc.name.startsWith(execName)
        }
        if (checksumAndNameMatches.isEmpty)
            return None
        return Some(checksumAndNameMatches.head)
    }

    def insert(name:String, dxObj:DXDataObject, digest: String) : Unit = {
        val aInfo = DxObjectInfo(name, LocalDateTime.now, dxObj, Some(digest))
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
                                     DxUtils.jsonNodeOfJsValue(req),
                                     classOf[JsonNode])
            case "Applet" =>
                DXAPI.appletRename(objInfo.dxObj.getId,
                                   DxUtils.jsonNodeOfJsValue(req),
                                   classOf[JsonNode])
            case other => throw new Exception(s"Unkown class ${other}")
        }
    }
}
