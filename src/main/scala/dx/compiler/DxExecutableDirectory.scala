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
  DxWorkflow,
  Field
}
import dx.core.Constants
import dx.core.ir.Bundle
import spray.json.JsString
import wdlTools.util.{JsUtils, Logger}

trait DxExecutableInfo {
  val dataObj: DxDataObject
  val desc: Option[DxObjectDescribe]
  val checksum: Option[String]
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
case class DxExecutableDirectory(bundle: Bundle,
                                 project: DxProject,
                                 folder: String,
                                 projectWideReuse: Boolean = false,
                                 dxApi: DxApi = DxApi.get,
                                 logger: Logger = Logger.get) {

  // a list of all dx:workflow and dx:applet names used in this WDL workflow
  private lazy val allExecutableNames: Set[String] = bundle.allCallables.keySet
  // API interface
  private lazy val dxFind = DxFindDataObjects(dxApi)

  /**
    * Information about a Dx data object.
    * @param dataObj the actual data object
    * @param dxDesc the object description
    * @param checksum the object checksum
    * @param createdDate created date
    */
  case class DxExecutableWithDesc(dataObj: DxDataObject,
                                  dxDesc: DxObjectDescribe,
                                  checksum: Option[String],
                                  createdDate: Option[LocalDateTime] = None)
      extends DxExecutableInfo {
    def name: String = dxDesc.name
    val desc: Option[DxObjectDescribe] = Some(dxDesc)
  }

  private def findExecutables(folder: Option[String]): Vector[(DxDataObject, DxObjectDescribe)] = {
    Vector("applet", "workflow")
      .flatMap { dxClass =>
        val t0 = System.nanoTime()
        val dxObjectsInFolder: Map[DxDataObject, DxObjectDescribe] =
          dxFind.apply(
              Some(project),
              folder,
              recurse = false,
              Some(dxClass),
              Vector(Constants.CompilerTag),
              allExecutableNames.toVector,
              withInputOutputSpec = false,
              Vector.empty,
              Set(Field.Details)
          )
        val t1 = System.nanoTime()
        val diffMSec = (t1 - t0) / (1000 * 1000)
        logger.trace(
            s"Found ${dxObjectsInFolder.size} ${dxClass} in ${project.id} folder=${folder} (${diffMSec} millisec)"
        )
        dxObjectsInFolder.toVector
      }
  }

  private def getChecksum(desc: DxObjectDescribe): Option[String] = {
    desc.details
      .flatMap(_.asJsObject.fields.get(Constants.Checksum))
      .map(JsUtils.getString(_))
      .orElse(desc.properties.flatMap(_.get(Constants.ChecksumPropertyDeprecated)))
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
  private def findExecutablesInFolder(): Map[String, Vector[DxExecutableInfo]] = {
    findExecutables(Some(folder))
      .map {
        case (dxObj, desc) =>
          val creationDate = new java.util.Date(desc.created)
          val creationTime: LocalDateTime =
            LocalDateTime.ofInstant(creationDate.toInstant, ZoneId.systemDefault())
          // checksum is stored in details, but used to be stored as a property, so
          // look in both places
          val checksum = getChecksum(desc)
          DxExecutableWithDesc(dxObj, desc, checksum, Some(creationTime))
      }
      .groupBy(_.name)
  }

  // A map from an applet/workflow that is part of the namespace to its dx:object
  // on the target path (project/folder)
  private lazy val initialExecDir: Map[String, Vector[DxExecutableInfo]] = findExecutablesInFolder()
  private var execDir: Option[Map[String, Vector[DxExecutableInfo]]] = None

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
  private def findExecutablesInProject(): Map[String, Vector[DxExecutableInfo]] = {
    findExecutables(None)
      .flatMap {
        case (obj, desc) =>
          getChecksum(desc) match {
            case Some(checksum) => Some(DxExecutableWithDesc(obj, desc, Some(checksum)))
            case None           => None
          }
      }
      .groupBy(_.checksum.get)
  }

  // A map from checksum to dx:executable, across the entire
  // project.  It allows reusing dx:executables across the entire
  // project, at the cost of a potentially expensive API call. It is
  // not clear this is useful to the majority of users, so it is
  // gated by the [projectWideReuse] flag.
  private lazy val projectWideExecDir: Map[String, Vector[DxExecutableInfo]] = {
    if (projectWideReuse) {
      findExecutablesInProject()
    } else {
      Map.empty
    }
  }

  /**
    * Gets information about all executables with the given name.
    * @param name the executable name
    * @return
    */
  def lookup(name: String): Vector[DxExecutableInfo] = {
    execDir.getOrElse(initialExecDir).getOrElse(name, Vector.empty)
  }

  /**
    * Searches for an executable with a specific checksum anywhere in the project.
    * In case of checksum collision (i.e. multiple results for the same checksum),
    * returns only the executable that starts with the name we are looking for.
    * @param name the executable name to look up
    * @param digest the executable's checksum
    * @return
    */
  def lookupInProject(name: String, digest: String): Option[DxExecutableInfo] = {
    projectWideExecDir.get(digest) match {
      case None => None
      case Some(checksumMatches) =>
        checksumMatches.find(_.name.startsWith(name))
    }
  }

  case class DxExecutableInserted(
      name: String,
      dataObj: DxDataObject,
      checksum: Option[String],
      createdDate: Option[LocalDateTime] = Some(LocalDateTime.now)
  ) extends DxExecutableInfo {
    override val desc: Option[DxObjectDescribe] = None
  }

  /**
    * Insert an executable into the directory.
    * @param name the executable name
    * @param dxExec the data object
    * @param digest the checksum
    */
  def insert(name: String, dxExec: DxDataObject, digest: String): Unit = {
    val info = DxExecutableInserted(name, dxExec, Some(digest))
    execDir = execDir.getOrElse(initialExecDir) match {
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
    * Moves an object into an archive directory. If the object
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
    * TODO: need to update the execInfo in the directory
    *
    * @param execInfo the object to archive
    */
  def archive(execInfo: DxExecutableInfo): Unit = {
    logger.trace(s"Archiving ${execInfo.name} ${execInfo.dataObj.id}")
    val dxClass: String = execInfo.dxClass
    // move the object to the new location
    val destFolder = s"${folder}/${dxClass}_archive"
    ensureFolder(destFolder)
    project.moveObjects(Vector(execInfo.dataObj), destFolder)
    // add the date to the object name
    val name = execInfo.createdDate match {
      case None     => execInfo.name
      case Some(dt) => s"${execInfo.name} ${dt.format(dateFormatter)}"
    }
    val request = Map("project" -> JsString(project.id), "name" -> JsString(name))
    logger.ignore(dxClass match {
      case "Workflow" =>
        dxApi.workflowRename(execInfo.dataObj.id, request)
      case "Applet" =>
        dxApi.appletRename(execInfo.dataObj.id, request)
      case other =>
        throw new Exception(s"Cannot archive object ${execInfo} with class ${other}")
    })
  }

  /**
    * Archive multiple executables.
    * @param execInfos the executables to archive
    */
  def archive(execInfos: Vector[DxExecutableInfo]): Unit = {
    execInfos.foreach(archive)
  }

  /**
    * Remove executables from the project and update the directory.
    * TODO: need to remove the execInfos in the directory
    * @param execInfos the executables to remove
    */
  def remove(execInfos: Vector[DxExecutableInfo]): Unit = {
    val objs = execInfos.map(_.dataObj)
    logger.trace(s"Removing old ${execInfos.head.name} ${objs.map(_.id)}")
    project.removeObjects(objs)
  }
}
