package dx.compiler

import java.time.{LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter

import dx.api.{
  DxApi,
  DxApp,
  DxApplet,
  DxDataObject,
  DxFindDataObjects,
  DxObjectDescribe,
  DxProject,
  DxWorkflow
}
import dx.core.ir.Bundle
import spray.json.JsString
import wdlTools.util.Logger

trait DxDataObjectInfo {
  val dataObj: DxDataObject
  val desc: Option[DxObjectDescribe]
  val digest: Option[String]
  val createdDate: Option[LocalDateTime]
  def name: String

  lazy val dxClass: String = {
    dataObj match {
      case _: DxApp      => "App"
      case _: DxApplet   => "Applet"
      case _: DxWorkflow => "Workflow"
      case _             => dataObj.getClass.getSimpleName
    }
  }
}

/**
  * Takes a snapshot of the platform target path before the build starts.
  * Makes an efficient directory of all the applets that exist there. Update
  * the directory when an applet is compiled.
  *
  * @param bundle an IR bundle
  * @param project the project to search
  * @param folder the folder to search
  * @param projectWideReuse whether to allow project-wide reuse
  * @param dxApi the Dx API
  */
case class DxDataObjectDirectory(bundle: Bundle,
                                 project: DxProject,
                                 folder: String,
                                 projectWideReuse: Boolean = false,
                                 dxApi: DxApi = DxApi.get,
                                 logger: Logger = Logger.get) {

  // a list of all dx:workflow and dx:applet names used in this WDL workflow
  private lazy val allExecutableNames: Set[String] = bundle.allCallables.keySet

  private lazy val dxFind = DxFindDataObjects(dxApi)

  /**
    * Information about a Dx data object.
    * @param dataObj the actual data object
    * @param dxDesc the object description
    * @param digest the object checksum
    * @param createdDate created date
    */
  case class DxDataObjectWithDesc(dataObj: DxDataObject,
                                  dxDesc: DxObjectDescribe,
                                  digest: Option[String],
                                  createdDate: Option[LocalDateTime] = None)
      extends DxDataObjectInfo {
    def name: String = dxDesc.name
    val desc: Option[DxObjectDescribe] = Some(dxDesc)
  }

  private def lookup(folder: Option[String]): Vector[(DxDataObject, DxObjectDescribe)] = {
    Vector("applets", "workflows")
      .flatMap { dxClass =>
        val t0 = System.nanoTime()
        val dxObjectsInFolder: Map[DxDataObject, DxObjectDescribe] =
          dxFind.apply(
              Some(project),
              folder,
              recurse = false,
              Some(dxClass),
              Vector(ChecksumProperty),
              allExecutableNames.toVector,
              withInputOutputSpec = false,
              Vector.empty,
              Set.empty
          )
        val t1 = System.nanoTime()
        val diffMSec = (t1 - t0) / (1000 * 1000)
        dxApi.logger.trace(
            s"""|Found ${dxObjectsInFolder.size} ${dxClass}
                |in ${project.getId} folder=${folder} (${diffMSec} millisec)""".stripMargin
              .replaceAll("\n", " ")
        )
        dxObjectsInFolder.toVector
      }
  }

  /**
    * Instead of looking up applets/workflows one by one, perform a bulk lookup, and
    * find all the objects in the target directory. Setup an easy to
    * use map with information on each name.
    *
    * findDataObjects can be an expensive call, both on the server and client sides.
    * We limit it by filtering on the CHECKSUM property, which is attached only to
    * generated applets and workflows. This runs the risk of missing cases where an
    * applet name is already in use by a regular dnanexus applet/workflow.
    *
    * @return
    */
  private def bulkLookup(): Map[String, Vector[DxDataObjectInfo]] = {
    lookup(Some(folder))
      .map {
        case (dxObj, desc) =>
          val creationDate = new java.util.Date(desc.created)
          val creationTime: LocalDateTime =
            LocalDateTime.ofInstant(creationDate.toInstant, ZoneId.systemDefault())
          val checksum = desc.properties.flatMap(_.get(ChecksumProperty))
          DxDataObjectWithDesc(dxObj, desc, checksum, Some(creationTime))
      }
      .groupBy(_.name)
  }

  // A map from an applet/workflow that is part of the namespace to its dx:object
  // on the target path (project/folder)
  private lazy val initialObjDir: Map[String, Vector[DxDataObjectInfo]] = bulkLookup()
  private var objDir: Option[Map[String, Vector[DxDataObjectInfo]]] = None

  /**
    * Scan the entire project for dx:workflows and dx:applets that we already created,
    * and may be reused, instead of recompiling.
    *
    * Note: This could be expensive, and the maximal number of replies is (by default)
    * 1000. Since the index is limited, we may miss matches when we search. The cost
    * would be creating a dx:executable again, which is acceptable.
    *
    * findDataObjects can be an expensive call, both on the server and client sides.
    * We limit it by filtering on the CHECKSUM property, which is attached only to
    * generated applets and workflows.
    *
    * @return
    */
  private def projectBulkLookup(): Map[String, Vector[DxDataObjectInfo]] = {
    lookup(None)
      .collect {
        case (obj, desc) if desc.properties.exists(_.contains(ChecksumProperty)) =>
          DxDataObjectWithDesc(obj, desc, Some(desc.properties.get(ChecksumProperty)))
      }
      .groupBy(_.digest.get)
  }

  // A map from checksum to dx:executable, across the entire
  // project.  It allows reusing dx:executables across the entire
  // project, at the cost of a potentially expensive API call. It is
  // not clear this is useful to the majority of users, so it is
  // gated by the [projectWideReuse] flag.
  private lazy val projectWideObjDir: Map[String, Vector[DxDataObjectInfo]] = {
    if (projectWideReuse) {
      projectBulkLookup()
    } else {
      Map.empty
    }
  }

  def lookup(execName: String): Vector[DxDataObjectInfo] = {
    objDir.getOrElse(initialObjDir).getOrElse(execName, Vector.empty)
  }

  /**
    * Search for an executable named [execName], with a specific checksum anywhere
    * in the project. This could save recompilation. In case of checksum collision,
    * there could be several hits; returns only the one that starts with the name we
    * are looking for.
    * @param execName the executable name to look up
    * @param digest the executable's checksum
    * @return
    */
  def lookupOtherVersions(execName: String, digest: String): Option[DxDataObjectInfo] = {
    projectWideObjDir.get(digest) match {
      case None => None
      case Some(checksumMatches) =>
        checksumMatches.find(_.name.startsWith(execName))
    }
  }

  case class DxDataObjectInserted(
      name: String,
      dataObj: DxDataObject,
      digest: Option[String],
      createdDate: Option[LocalDateTime] = Some(LocalDateTime.now)
  ) extends DxDataObjectInfo {
    override val desc: Option[DxObjectDescribe] = None
  }

  def insert(name: String, dxObj: DxDataObject, digest: String): Unit = {
    val info = DxDataObjectInserted(name, dxObj, Some(digest))
    objDir = objDir.getOrElse(initialObjDir) match {
      case d if d.contains(name) =>
        Some(d + (name -> (d(name) :+ info)))
      case d =>
        Some(d + (name -> Vector(info)))
    }
  }

  private lazy val dateFormatter = DateTimeFormatter.ofPattern("EE MMM dd kk:mm:ss yyyy")
  private var folders: Set[String] = Set.empty

  // create a folder, if it does not already exist.
  private def ensureFolder(fullPath: String): Unit = {
    if (!folders.contains(fullPath)) {
      project.newFolder(fullPath, parents = true)
      folders += fullPath
    }
  }

  /**
    * Move an object into an archive directory. If the object
    * is an applet, for example /A/B/C/GLnexus, move it to
    *     /A/B/C/Applet_archive/GLnexus (Day Mon DD hh:mm:ss year)
    * If the object is a workflow, move it to
    *     /A/B/C/Workflow_archive/GLnexus (Day Mon DD hh:mm:ss year)
    *
    * Examples:
    *   GLnexus (Fri Aug 19 18:01:02 2016)
    *   GLnexus (Mon Mar  7 15:18:14 2016)
    *
    * Note: 'dx build' does not support workflow archiving at the moment.
    *
    * @param objInfo the object to archive
    */
  def archiveDxObject(objInfo: DxDataObjectInfo): Unit = {
    dxApi.logger.trace(s"Archiving ${objInfo.name} ${objInfo.dataObj.getId}")
    val dxClass: String = objInfo.dxClass
    // move the object to the new location
    val destFolder = s"${folder}/${dxClass}_archive"
    ensureFolder(destFolder)
    project.moveObjects(Vector(objInfo.dataObj), destFolder)
    // add the date to the object name
    val name = objInfo.createdDate match {
      case None     => objInfo.name
      case Some(dt) => s"${objInfo.name} ${dt.format(dateFormatter)}"
    }
    val request = Map("project" -> JsString(project.getId), "name" -> JsString(name))
    logger.ignore(dxClass match {
      case "Workflow" =>
        dxApi.workflowRename(objInfo.dataObj.getId, request)
      case "Applet" =>
        dxApi.appletRename(objInfo.dataObj.getId, request)
      case other =>
        throw new Exception(s"Cannot archive object ${objInfo} with class ${other}")
    })
  }
}
