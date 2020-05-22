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
package dxWDL.util

import com.dnanexus.DXAPI
import com.fasterxml.jackson.databind.JsonNode
import spray.json._
import wdlTools.eval.WdlValues

import dxWDL.base.{BaseUtils, Verbose}
import dxWDL.dx._

// Request for an instance type
case class InstanceTypeReq(dxInstanceType: Option[String],
                           memoryMB: Option[Int],
                           diskGB: Option[Int],
                           cpu: Option[Int],
                           gpu: Option[Boolean])

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
                          price: Float,
                          os: Vector[(String, String)],
                          gpu: Boolean) {
  // Does this instance satisfy the requirements?
  def satisfies(memReq: Option[Int],
                diskReq: Option[Int],
                cpuReq: Option[Int],
                gpuReq: Option[Boolean]): Boolean = {
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
    return true
  }

  def compareByPrice(that: DxInstanceType): Int = {
    // compare by price
    if (this.price < that.price)
      return -1
    if (this.price > that.price)
      return 1
    return 0
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
    //System.out.println(s"compareByResource ${this.name} ${that.name} retval=${retval}")
    retval
  }

  // v2 instances are always better than v1 instances
  def compareByType(that: DxInstanceType): Int = {
    //System.out.println(s"compareByType ${this.name} ${that.name}")
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
  implicit val dxInstanceTypeFormat = jsonFormat7(DxInstanceType.apply)
}

case class InstanceTypeDB(pricingAvailable: Boolean, instances: Vector[DxInstanceType]) {

  // if prices are available, choose the cheapest instance. Otherwise,
  // choose one with minimal resources.
  private def lteq(x: DxInstanceType, y: DxInstanceType): Boolean = {
    val costDiff =
      if (pricingAvailable) {
        x.compareByPrice(y)
      } else {
        x.compareByResources(y)
      }
    if (costDiff != 0)
      return costDiff <= 0

    // cost is the same, compare by instance type. Better is HIGHER
    x.compareByType(y) >= 0
  }

  // Calculate the dx instance type that fits best, based on
  // runtime specifications.
  //
  // memory:    minimal amount of RAM, specified in MB
  // diskSpace: minimal amount of disk space, specified in GB
  // numCores:  minimal number of cores
  //
  // Solving this in an optimal way is a hard problem. The approximation
  // we use here is:
  // 1) discard all instances that do not have enough resources
  // 2) choose the cheapest instance
  def chooseAttrs(memoryMB: Option[Int],
                  diskGB: Option[Int],
                  cpu: Option[Int],
                  gpu: Option[Boolean]): String = {
    // discard all instances that are too weak
    val sufficient: Vector[DxInstanceType] =
      instances.filter(x => x.satisfies(memoryMB, diskGB, cpu, gpu))
    if (sufficient.length == 0)
      throw new Exception(
          s"""|No instances found that match the requirements
              |memory=$memoryMB, diskGB=$diskGB, cpu=$cpu""".stripMargin
            .replaceAll("\n", " ")
      )
    val initialGuess = sufficient.head
    val bestInstance = sufficient.tail.foldLeft(initialGuess) {
      case (bestSoFar, x) =>
        if (lteq(x, bestSoFar)) x
        else bestSoFar
    }
    bestInstance.name
  }

  def chooseShortcut(iType: String): String = {
    // Short circut the calculation, and just choose this instance.
    // Make sure it is available.
    instances.find(x => x.name == iType) match {
      case Some(x) =>
        // instance exists, and can be used
        iType
      case None =>
        // Probably a bad instance name
        throw new Exception(s"""|Instance type ${iType} is unavailable
                                |or badly named""".stripMargin.replaceAll("\n", " "))
    }
  }

  // The cheapest available instance, this is normally also the smallest.
  private def calcMinimalInstanceType(iTypes: Set[DxInstanceType]): DxInstanceType = {
    if (iTypes.isEmpty)
      throw new Exception("empty list")
    iTypes.tail.foldLeft(iTypes.head) {
      case (best, elem) =>
        if (lteq(elem, best)) elem
        else best
    }
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
        (a.memoryMB <= b.memoryMB &&
          a.diskGB <= b.diskGB &&
          a.cpu <= b.cpu)
      case (_, _) =>
        // At least one of the instances is not in the database.
        // We can't compare them.
        false
    }
  }

  def apply(iType: InstanceTypeReq): String = {
    iType.dxInstanceType match {
      case None =>
        chooseAttrs(iType.memoryMB, iType.diskGB, iType.cpu, iType.gpu)
      case Some(dxIType) =>
        // Shortcut the entire calculation, and provide the dx instance type directly
        chooseShortcut(dxIType)
    }
  }

  // sort the instances, and print them out
  def prettyPrint(): String = {
    var remain: Set[DxInstanceType] = instances.toSet
    var sortediTypes: Vector[DxInstanceType] = Vector()
    while (!remain.isEmpty) {
      val smallest = calcMinimalInstanceType(remain)
      sortediTypes = sortediTypes :+ smallest
      remain = remain - smallest
    }
    sortediTypes.toJson.prettyPrint
  }
}

object InstanceTypeDB extends DefaultJsonProtocol {
  // support automatic conversion to/from JsValue
  implicit val instanceTypeDBFormat = jsonFormat2(InstanceTypeDB.apply)

  // Currently, we support only constants.
  def parse(dxInstanceType: Option[WdlValues.V],
            wdlMemoryMB: Option[WdlValues.V],
            wdlDiskGB: Option[WdlValues.V],
            wdlCpu: Option[WdlValues.V],
            wdlGpu: Option[WdlValues.V]): InstanceTypeReq = {
    // Shortcut the entire calculation, and provide the dx instance type directly
    dxInstanceType match {
      case None => None
      case Some(WdlValues.V_String(iType)) =>
        return InstanceTypeReq(Some(iType), None, None, None, None)
      case Some(x) =>
        throw new Exception(
            s"""|dxInstaceType has to evaluate to a
                |WdlValues.V_String type ${x.toWdlValues.V_String}""".stripMargin
              .replaceAll("\n", " ")
        )
    }

    // Examples for memory specification: "4000 MB", "1 GB"
    val memoryMB: Option[Int] = wdlMemoryMB match {
      case None                 => None
      case Some(WdlValues.V_String(buf)) =>
        // extract number
        val numRex = """(\d+\.?\d*)""".r
        val numbers = numRex.findAllIn(buf).toList
        if (numbers.length != 1)
          throw new Exception(s"Can not parse memory specification ${buf}")
        val number: String = numbers(0)
        val x: Double =
          try {
            number.toDouble
          } catch {
            case e: Throwable =>
              throw new Exception(s"Unrecognized number ${number}")
          }

        // extract memory units
        val memUnitRex = """([a-zA-Z]+)""".r
        val memUnits = memUnitRex.findAllIn(buf).toList
        if (memUnits.length > 1)
          throw new Exception(s"Can not parse memory specification ${buf}")
        val memBytes: Double =
          if (memUnits.isEmpty) {
            // specification is in bytes, convert to megabytes
            x.toInt
          } else {
            // Units were specified
            val memUnit: String = memUnits(0).toLowerCase
            val nBytes: Double = memUnit match {
              case "b"   => x
              case "kb"  => x * 1000d
              case "mb"  => x * 1000d * 1000d
              case "gb"  => x * 1000d * 1000d * 1000d
              case "tb"  => x * 1000d * 1000d * 1000d * 1000d
              case "kib" => x * 1024d
              case "mib" => x * 1024d * 1024d
              case "gib" => x * 1024d * 1024d * 1024d
              case "tib" => x * 1024d * 1024d * 1024d * 1024d
              case _     => throw new Exception(s"Unknown memory unit ${memUnit}")
            }
            nBytes.toDouble
          }
        val memMib: Double = memBytes / (1024 * 1024).toDouble
        Some(memMib.toInt)
      case Some(x) =>
        throw new Exception(s"Memory has to evaluate to a WdlValues.V_String type ${x.toWdlValues.V_String}")
    }

    // Examples: "local-disk 1024 HDD"
    val diskGB: Option[Int] = wdlDiskGB match {
      case None => None
      case Some(WdlValues.V_String(buf)) =>
        val components = buf.split("\\s+")
        val ignoreWords = Set("local-disk", "hdd", "sdd", "ssd")
        val l = components.filter(x => !(ignoreWords contains x.toLowerCase))
        if (l.length != 1)
          throw new Exception(s"Can't parse disk space specification ${buf}")
        val i =
          try {
            l(0).toInt
          } catch {
            case e: Throwable =>
              throw new Exception(s"Parse error for diskSpace attribute ${buf}")
          }
        Some(i)
      case Some(x) =>
        throw new Exception(s"Disk space has to evaluate to a WdlValues.V_String type ${x.toWdlValues.V_String}")
    }

    // Examples: "1", "12"
    val cpu: Option[Int] = wdlCpu match {
      case None => None
      case Some(WdlValues.V_String(buf)) =>
        val i: Int =
          try {
            buf.toInt
          } catch {
            case e: Throwable =>
              throw new Exception(s"Parse error for cpu specification ${buf}")
          }
        Some(i)
      case Some(WdlValues.V_Int(i)) => Some(i)
      case Some(WdlValues.V_Float(x))   => Some(x.toInt)
      case Some(x)             => throw new Exception(s"Cpu has to evaluate to a numeric value ${x}")
    }

    val gpu: Option[Boolean] = wdlGpu match {
      case None                   => None
      case Some(WdlValues.V_Boolean(flag)) => Some(flag)
      case Some(x)                => throw new Exception(s"Gpu has to be a boolean ${x}")
    }

    return InstanceTypeReq(None, memoryMB, diskGB, cpu, gpu)
  }

  // Extract an integer fields from a JsObject
  private def getJsIntField(js: JsValue, fieldName: String): Int = {
    js.asJsObject.fields.get(fieldName) match {
      case Some(JsNumber(x)) => x.toInt
      case Some(JsString(x)) => x.toInt
      case _                 => throw new Exception(s"Missing field ${fieldName} in JSON ${js.prettyPrint}}")
    }
  }

  private def getJsStringField(js: JsValue, fieldName: String): String = {
    js.asJsObject.fields.get(fieldName) match {
      case Some(JsNumber(x)) => x.toString
      case Some(JsString(x)) => x
      case _                 => throw new Exception(s"Missing field ${fieldName} in JSON ${js.prettyPrint}}")
    }
  }

  private def getJsField(js: JsValue, fieldName: String): JsValue = {
    js.asJsObject.fields.get(fieldName) match {
      case Some(x: JsValue) => x
      case None             => throw new Exception(s"Missing field ${fieldName} in ${js.prettyPrint}")
    }
  }

  // Query the platform for the available instance types in
  // this project.
  private def queryAvailableInstanceTypes(dxProject: DxProject): Map[String, DxInstanceType] = {
    // get List of supported OSes
    def getSupportedOSes(js: JsValue): Vector[(String, String)] = {
      val osSupported: Vector[JsValue] = js.asJsObject.fields.get("os") match {
        case Some(JsArray(x)) => x
        case _                => throw new Exception(s"Missing field os in JSON ${js.prettyPrint}")
      }
      osSupported.map { elem =>
        val distribution = getJsStringField(elem, "distribution")
        val release = getJsStringField(elem, "release")
        distribution -> release
      }.toVector
    }

    val req = JsObject("fields" ->
                         JsObject("availableInstanceTypes" -> JsTrue))
    val rep = DXAPI.projectDescribe(dxProject.id, req, classOf[JsonNode])
    val repJs: JsValue = DxUtils.jsValueOfJsonNode(rep)
    val availableInstanceTypes: JsValue =
      repJs.asJsObject.fields.get(availableField) match {
        case Some(x) => x
        case None    => throw new Exception(s"Field ${availableField} is missing ${repJs.prettyPrint}")
      }

    // convert to a list of DxInstanceTypes, with prices set to zero
    availableInstanceTypes.asJsObject.fields.map {
      case (iName, jsValue) =>
        val numCores = getJsIntField(jsValue, "numCores")
        val memoryMB = getJsIntField(jsValue, "totalMemoryMB")
        val diskSpaceGB = getJsIntField(jsValue, "ephemeralStorageGB")
        val os = getSupportedOSes(jsValue)
        val gpu = iName contains "_gpu"
        val dxInstanceType = DxInstanceType(iName, memoryMB, diskSpaceGB, numCores, 0, os, gpu)
        iName -> dxInstanceType
    }.toMap
  }

  // Get the mapping from instance type to price, limited to the
  // project we are in. Describing a user requires permission to
  // view the user account. The compiler may not have these
  // permissions, causing this method to throw an exception.
  private def getPricingModel(billTo: String, region: String): Map[String, Float] = {
    val req = JsObject("fields" ->
                         JsObject("pricingModelsByRegion" -> JsTrue))
    val rep =
      try {
        DXAPI.userDescribe(billTo, req, classOf[JsonNode])
      } catch {
        case e: Throwable =>
          throw new Exception("Insufficient permissions")
      }

    val js: JsValue = DxUtils.jsValueOfJsonNode(rep)
    val pricingModelsByRegion = getJsField(js, "pricingModelsByRegion")
    val pricingModel = getJsField(pricingModelsByRegion, region)
    val computeRatesPerHour = getJsField(pricingModel, "computeRatesPerHour")

    // convert from JsValue to a Map
    computeRatesPerHour.asJsObject.fields.map {
      case (key, jsValue) =>
        val hourlyRate: Float = jsValue match {
          case JsNumber(x) => x.toFloat
          case JsString(x) => x.toFloat
          case _           => throw new Exception(s"compute rate is not a number ${jsValue.prettyPrint}")
        }
        key -> hourlyRate
    }.toMap
  }

  private def crossTables(availableIT: Map[String, DxInstanceType],
                          pm: Map[String, Float]): Vector[DxInstanceType] = {
    pm.map {
        case (iName, hourlyRate) =>
          availableIT.get(iName) match {
            case None => None
            case Some(iType) =>
              Some(
                  DxInstanceType(iName,
                                 iType.memoryMB,
                                 iType.diskGB,
                                 iType.cpu,
                                 hourlyRate,
                                 iType.os,
                                 iType.gpu)
              )
          }
      }
      .flatten
      .toVector
  }

  // Check if an instance type passes some basic criteria:
  // - Instance must support Ubuntu 16.04.
  // - Instance is not an FPGA instance.
  // - Instance does not have local HDD storage, this
  //   means it is really old hardware.
  private def instanceCriteria(iType: DxInstanceType): Boolean = {
    val osSupported = iType.os.foldLeft(false) {
      case (accu, (distribution, release)) =>
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
    return true
  }

  private def queryNoPrices(dxProject: DxProject): InstanceTypeDB = {
    // Figure out the available instances by describing the project
    val allAvailableIT = queryAvailableInstanceTypes(dxProject)

    // filter out instances that we cannot use
    val iTypes: Vector[DxInstanceType] = allAvailableIT
      .filter { case (iName, traits) => instanceCriteria(traits) }
      .map { case (_, traits) => traits }
      .toVector
    InstanceTypeDB(false, iTypes)
  }

  private def queryWithPrices(dxProject: DxProject): InstanceTypeDB = {
    // Figure out the available instances by describing the project
    val allAvailableIT = queryAvailableInstanceTypes(dxProject)

    // get billTo and region from the project
    val (billTo, region) = DxUtils.projectDescribeExtraInfo(dxProject)

    // get the pricing model
    val pm = getPricingModel(billTo, region)

    // Create fully formed instance types by crossing the tables
    val availableInstanceTypes: Vector[DxInstanceType] = crossTables(allAvailableIT, pm)

    // filter out instances that we cannot use
    val iTypes: Vector[DxInstanceType] = availableInstanceTypes.filter(instanceCriteria)
    InstanceTypeDB(true, iTypes)
  }

  def query(dxProject: DxProject, verbose: Verbose): InstanceTypeDB = {
    try {
      queryWithPrices(dxProject)
    } catch {
      // Insufficient permissions to describe the user, we cannot get the price list.
      case e: Throwable =>
        BaseUtils.warning(
            verbose,
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
    if (db.instances.isEmpty)
      return db
    // check if there is no pricing information
    if (!db.pricingAvailable)
      return db

    // sort the prices from low to high, and then replace
    // with rank.
    var crnt_price = 0
    val sortedInstances = db.instances.sortWith(_.price < _.price)
    val opaque = sortedInstances.map { it =>
      crnt_price += 1
      it.copy(price = crnt_price)
    }
    InstanceTypeDB(true, opaque)
  }
}
