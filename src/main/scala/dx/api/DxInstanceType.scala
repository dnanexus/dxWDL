/**
Match the runtime WDL requirements to an machine instance supported by the platform.

There there may be requirements for memory, disk-space, and number of
cores. We interpret these as minimal requirements and try to choose a
good and inexpensive instance from the available list.

For example, a WDL task could have a runtime section like the following:
runtime {
    memory: "14 GB"
    cpu: "16"
    disks: "local-disk " + disk_size + " HDD"
}


Representation of a platform instance, how much resources it has, and
how much it costs to run.

resource    measurement units
--------    -----------------
memory      MB of RAM
disk        GB of disk space, hard drive or flash drive
cpu         1 per core
price       comparative price

  */
package dx.api

import dx.api.DiskType.DiskType
import wdlTools.util.{Enum, JsUtils}
import spray.json._

// Instance Type on the platform. For example:
// name:   mem1_ssd1_x4
// memory: 4096 MB
// disk:   80 GB
// price:  0.5 dollar per hour
// os:     [(Ubuntu, 12.04), (Ubuntu, 14.04)}
case class DxInstanceType(name: String,
                          memoryMB: Int,
                          diskGB: Int,
                          cpu: Int,
                          gpu: Boolean,
                          os: Vector[(String, String)],
                          diskType: Option[DiskType] = None,
                          price: Option[Float] = None)
    extends Ordered[DxInstanceType] {

  def compare(that: DxInstanceType): Int = {
    // if prices are available, choose the cheapest instance. Otherwise,
    // choose one with minimal resources.
    val costDiff = if (price.isDefined) {
      compareByPrice(that)
    } else {
      compareByResources(that)
    }
    costDiff match {
      case 0 => -compareByType(that)
      case _ => costDiff
    }
  }

  // Does this instance satisfy the requirements?
  def satisfies(memReq: Option[Long],
                diskReq: Option[Long],
                cpuReq: Option[Long],
                gpuReq: Option[Boolean],
                diskType: Option[DiskType]): Boolean = {
    memReq match {
      case Some(x) => if (memoryMB < x) return false
      case None    => ()
    }
    diskReq match {
      case Some(x) => if (diskGB < x) return false
      case None    => ()
    }
    cpuReq match {
      case Some(x) => if (cpu < x) return false
      case None    => ()
    }
    gpuReq match {
      case Some(flag) => if (flag != gpu) return false
      case None       => ()
    }
    (this.diskType, diskType) match {
      case (Some(dt1), Some(dt2)) if dt1 != dt2 => return false
      case _                                    => ()
    }
    true
  }

  def compareByPrice(that: DxInstanceType): Int = {
    // compare by price
    (this.price, that.price) match {
      case (Some(p1), Some(p2)) =>
        p1 - p2 match {
          case 0          => 0
          case i if i > 0 => Math.ceil(i).toInt
          case i          => Math.floor(i).toInt
        }
      case _ => 0
    }
  }

  // Compare based on resource sizes. This is a partial ordering.
  //
  // For example, if A has more memory, disk space, and cores than
  // B, then B < A. We round down memory and disk sizes, to make the
  // comparison insensitive to minor differences.
  //
  // We add some fuzziness to the comparison, because the instance
  // types don't have the exact memory and disk space that you would
  // expect. For example, mem1_ssd1_x2 has less disk space than mem2_ssd1_x2.
  def compareByResources(that: DxInstanceType): Int = {
    val memDelta = (this.memoryMB / 1024) - (that.memoryMB / 1024)
    val diskDelta = (this.diskGB / 16) - (that.diskGB / 16)
    val cpuDelta = this.cpu - that.cpu

    val retval =
      if (memDelta == 0 && diskDelta == 0 && cpuDelta == 0)
        0
      else if (memDelta <= 0 && diskDelta <= 0 && cpuDelta <= 0)
        -1
      else if (memDelta >= 0 && diskDelta >= 0 && cpuDelta >= 0)
        1
      else
        // instances cannot be directly compared.
        0
    retval
  }

  // v2 instances are always better than v1 instances
  def compareByType(that: DxInstanceType): Int = {
    def typeVersion(name: String) =
      if (name contains "_v2") "v2"
      else "v1"

    (typeVersion(this.name), typeVersion(that.name)) match {
      case ("v1", "v2") => -1
      case ("v2", "v1") => 1
      case (_, _)       => 0
    }
  }
}

// support automatic conversion to/from JsValue
object DxInstanceType extends DefaultJsonProtocol {
  implicit val diskType: RootJsonFormat[DiskType] = jsonFormat1(DiskType.withNameIgnoreCase)
  implicit val dxInstanceTypeFormat: RootJsonFormat[DxInstanceType] = jsonFormat8(
      DxInstanceType.apply
  )
}

object DiskType extends Enum {
  type DiskType = Value
  val HDD, SSD = Value
}

// Request for an instance type
case class InstanceTypeRequest(dxInstanceType: Option[String] = None,
                               memoryMB: Option[Long] = None,
                               diskGB: Option[Long] = None,
                               diskType: Option[DiskType] = None,
                               cpu: Option[Long] = None,
                               gpu: Option[Boolean] = None) {
  override def toString: String = {
    s"memory=${memoryMB} disk=${diskGB} cores=${cpu} instancetype=${dxInstanceType}"
  }
}

case class InstanceTypeDB(pricingAvailable: Boolean, instances: Vector[DxInstanceType]) {

  /**
    * Calculate the dx instance type that fits best, based on runtime specifications.
    * Solving this in an optimal way is a hard problem. The approximation we use here is:
    *  1) discard all instances that do not have enough resources
    *  2) choose the cheapest instance
    *
    * @param memoryMB minimal amount of RAM, specified in MB
    * @param diskGB minimal amount of disk space, specified in GB
    * @param cpu minimal number of CPU cores
    * @param gpu whether GPU is required
    * @param diskType HDD or SSD
    * @return instance type name
    */
  def chooseAttrs(memoryMB: Option[Long],
                  diskGB: Option[Long],
                  cpu: Option[Long],
                  gpu: Option[Boolean],
                  diskType: Option[DiskType]): String = {
    // discard all instances that are too weak
    val filteredAndSorted: Vector[DxInstanceType] =
      instances.filter(x => x.satisfies(memoryMB, diskGB, cpu, gpu, diskType)).sortWith(_ < _)
    if (filteredAndSorted.isEmpty) {
      throw new Exception(
          s"""|No instances found that match the requirements
              |memory=$memoryMB, diskGB=$diskGB, cpu=$cpu""".stripMargin
            .replaceAll("\n", " ")
      )
    }
    filteredAndSorted.head.name
  }

  def chooseShortcut(iType: String): String = {
    // Short circut the calculation, and just choose this instance.
    // Make sure it is available.
    instances.find(x => x.name == iType) match {
      case Some(_) =>
        // instance exists, and can be used
        iType
      case None =>
        // Probably a bad instance name
        throw new Exception(s"""|Instance type ${iType} is unavailable
                                |or badly named""".stripMargin.replaceAll("\n", " "))
    }
  }

  // The cheapest available instance, this is normally also the smallest.
  private def calcMinimalInstanceType(instanceTypes: Set[DxInstanceType]): DxInstanceType = {
    if (instanceTypes.isEmpty) {
      throw new Exception("empty list")
    }
    instanceTypes.toVector.sortWith(_ < _).head
  }

  // A fast but cheap instance type. Used to run WDL expression
  // processing, launch jobs, etc.
  //
  def defaultInstanceType: String = {
    // exclude nano instances, they aren't strong enough.
    val goodEnough = instances.filter { iType =>
      !iType.name.contains("test") &&
      iType.memoryMB >= 3 * 1024 &&
      iType.cpu >= 2
    }
    val iType = calcMinimalInstanceType(goodEnough.toSet)
    iType.name
  }

  // check if instance type A is smaller or equal in requirements to
  // instance type B
  def lteqByResources(iTypeA: String, iTypeB: String): Boolean = {
    val ax = instances.find(_.name == iTypeA)
    val bx = instances.find(_.name == iTypeB)

    (ax, bx) match {
      case (Some(a), Some(b)) =>
        // All the resources for instance A should be less than or equal
        // to the resoruces for B.
        a.memoryMB <= b.memoryMB &&
          a.diskGB <= b.diskGB &&
          a.cpu <= b.cpu
      case (_, _) =>
        // At least one of the instances is not in the database.
        // We can't compare them.
        false
    }
  }

  def apply(iType: InstanceTypeRequest): String = {
    iType match {
      case InstanceTypeRequest(Some(dxInstanceType), _, _, _, _, _) =>
        // Shortcut the entire calculation, and provide the dx instance type directly
        chooseShortcut(dxInstanceType)
      case InstanceTypeRequest(None, memoryMB, diskGB, diskType, cpu, gpu) =>
        chooseAttrs(memoryMB, diskGB, cpu, gpu, diskType)
    }
  }

  /**
    * Formats the database as a prettified String. Instance types are sorted.
    * @return
    */
  def prettyFormat(): String = {
    var remain: Set[DxInstanceType] = instances.toSet
    var sortedInstanceTypes: Vector[DxInstanceType] = Vector()
    while (remain.nonEmpty) {
      val smallest = calcMinimalInstanceType(remain)
      sortedInstanceTypes = sortedInstanceTypes :+ smallest
      remain = remain - smallest
    }
    sortedInstanceTypes.toJson.prettyPrint
  }
}

object InstanceTypeDB extends DefaultJsonProtocol {
  // support automatic conversion to/from JsValue
  implicit val instanceTypeDBFormat: RootJsonFormat[InstanceTypeDB] = jsonFormat2(
      InstanceTypeDB.apply
  )
}

case class InstanceTypeDbQuery(dxApi: DxApi) {
  // Query the platform for the available instance types in
  // this project.
  private def queryAvailableInstanceTypes(dxProject: DxProject): Map[String, DxInstanceType] = {
    // get Vector of supported OSes
    def getSupportedOSes(js: JsValue): Vector[(String, String)] = {
      val osSupported: Vector[JsValue] = js.asJsObject.fields.get("os") match {
        case Some(JsArray(x)) => x
        case _                => throw new Exception(s"Missing field os in JSON ${js.prettyPrint}")
      }
      osSupported.map { elem =>
        val distribution = JsUtils.getString(elem, Some("distribution"))
        val release = JsUtils.getString(elem, Some("release"))
        distribution -> release
      }
    }

    val availableInstanceTypes = dxProject.listAvailableInstanceTypes()
    // convert to a list of DxInstanceTypes, with prices set to zero
    availableInstanceTypes.fields.map {
      case (iName, jsValue) =>
        val numCores = JsUtils.getInt(jsValue, Some("numCores"))
        val memoryMB = JsUtils.getInt(jsValue, Some("totalMemoryMB"))
        val diskSpaceGB = JsUtils.getInt(jsValue, Some("ephemeralStorageGB"))
        val diskType = iName.toLowerCase match {
          case s if s.contains("_ssd") => Some(DiskType.SSD)
          case s if s.contains("_hdd") => Some(DiskType.HDD)
          case _                       => None
        }
        val os = getSupportedOSes(jsValue)
        val gpu = iName.toLowerCase.contains("_gpu")
        val dxInstanceType =
          DxInstanceType(iName, memoryMB, diskSpaceGB, numCores, gpu, os, diskType)
        iName -> dxInstanceType
    }
  }

  // Get the mapping from instance type to price, limited to the
  // project we are in. Describing a user requires permission to
  // view the user account. The compiler may not have these
  // permissions, causing this method to throw an exception.
  private def getPricingModel(billTo: String, region: String): Map[String, Float] = {
    val request = Map("fields" -> JsObject("pricingModelsByRegion" -> JsTrue))
    val response = billTo match {
      case _ if billTo.startsWith("org") =>
        dxApi.orgDescribe(billTo, request)
      case _ if billTo.startsWith("user") =>
        dxApi.userDescribe(billTo, request)
      case _ => throw new Exception(s"Invalid billTo ${billTo}")
    }
    val pricingModelsByRegion = JsUtils.getFields(response, Some("pricingModelsByRegion"))
    val computeRatesPerHour =
      JsUtils.getFields(pricingModelsByRegion(region), Some("computeRatesPerHour"))

    // convert from JsValue to a Map
    computeRatesPerHour.map {
      case (key, jsValue) =>
        val hourlyRate: Float = jsValue match {
          case JsNumber(x) => x.toFloat
          case JsString(x) => x.toFloat
          case _           => throw new Exception(s"compute rate is not a number ${jsValue.prettyPrint}")
        }
        key -> hourlyRate
    }
  }

  private def crossTables(availableIT: Map[String, DxInstanceType],
                          pm: Map[String, Float]): Vector[DxInstanceType] = {
    pm.flatMap {
      case (iName, hourlyRate) =>
        availableIT.get(iName) match {
          case None => None
          case Some(iType) =>
            Some(
                DxInstanceType(iName,
                               iType.memoryMB,
                               iType.diskGB,
                               iType.cpu,
                               iType.gpu,
                               iType.os,
                               iType.diskType,
                               Some(hourlyRate))
            )
        }
    }.toVector
  }

  // Check if an instance type passes some basic criteria:
  // - Instance must support Ubuntu 16.04.
  // - Instance is not an FPGA instance.
  // - Instance does not have local HDD storage, this
  //   means it is really old hardware.
  private def instanceCriteria(iType: DxInstanceType): Boolean = {
    val osSupported = iType.os.foldLeft(false) {
      case (accu, (_, release)) =>
        if (release == "16.04")
          true
        else
          accu
    }
    if (!osSupported)
      return false
    if (iType.name contains "fpga")
      return false
    if (iType.name contains "hdd")
      return false
    true
  }

  private def queryNoPrices(dxProject: DxProject): InstanceTypeDB = {
    // Figure out the available instances by describing the project
    val allAvailableIT = queryAvailableInstanceTypes(dxProject)

    // filter out instances that we cannot use
    val iTypes: Vector[DxInstanceType] = allAvailableIT
      .filter { case (_, traits) => instanceCriteria(traits) }
      .map { case (_, traits) => traits }
      .toVector
    InstanceTypeDB(pricingAvailable = false, iTypes)
  }

  private def queryWithPrices(dxProject: DxProject): InstanceTypeDB = {
    // Figure out the available instances by describing the project
    val allAvailableIT = queryAvailableInstanceTypes(dxProject)

    // get billTo and region from the project
    val desc = dxProject.describe(Set(Field.Region, Field.BillTo))
    val billTo = desc.billTo match {
      case Some(s) => s
      case None    => throw new Exception(s"Failed to get billTo from project ${dxProject.id}")
    }
    val region = desc.region match {
      case Some(s) => s
      case None    => throw new Exception(s"Failed to get region from project ${dxProject.id}")
    }

    // get the pricing model
    val pm = getPricingModel(billTo, region)

    // Create fully formed instance types by crossing the tables
    val availableInstanceTypes: Vector[DxInstanceType] = crossTables(allAvailableIT, pm)

    // filter out instances that we cannot use
    val iTypes: Vector[DxInstanceType] = availableInstanceTypes.filter(instanceCriteria)
    InstanceTypeDB(pricingAvailable = true, iTypes)
  }

  def query(dxProject: DxProject): InstanceTypeDB = {
    try {
      queryWithPrices(dxProject)
    } catch {
      // Insufficient permissions to describe the user, we cannot get the price list.
      case _: Throwable =>
        dxApi.logger.warning(
            """|Warning: insufficient permissions to retrieve the
               |instance price list. This will result in suboptimal machine choices,
               |incurring higher costs when running workflows.
               |""".stripMargin.replaceAll("\n", " ")
        )
        queryNoPrices(dxProject)
    }
  }

  // Remove exact price information. For example,
  // if the price list is:
  //   mem1_ssd1_x2:  0.04$
  //   mem1_ssd1_x4:  0.08$
  //   mem3_ssd1_x8:  1.05$
  // convert it into:
  //   mem1_ssd1_x2:  1$
  //   mem1_ssd1_x4:  2$
  //   mem3_ssd1_x8:  3$
  //
  // This is useful when exporting the price list to an applet, which can be reverse
  // engineered. We do not want to risk disclosing the real price list. Leaking the
  // relative ordering of instances, but not actual prices, is considered ok.
  def opaquePrices(db: InstanceTypeDB): InstanceTypeDB = {
    // check if there is no pricing information
    if (db.instances.isEmpty || !db.pricingAvailable) {
      return db
    }
    // sort the prices from low to high, and then replace
    // with rank.
    val sortedInstances = db.instances.sortWith(_.price.get < _.price.get)
    val (_, opaqueInstances) = sortedInstances.foldLeft((1.0, Vector.empty[DxInstanceType])) {
      case ((crntPrice, accu), instance) =>
        val opaquePrice = Some(crntPrice.toFloat)
        val instanceOpq = instance.copy(price = opaquePrice)
        (crntPrice + 1, accu :+ instanceOpq)
    }
    InstanceTypeDB(pricingAvailable = true, opaqueInstances)
  }
}
