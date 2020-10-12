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

import wdlTools.util.{Enum, JsUtils, Logger}
import wdlTools.util.Enum.enumFormat
import spray.json.{RootJsonFormat, _}

object DiskType extends Enum {
  type DiskType = Value
  val HDD, SSD = Value
}

case class ExecutionEnvironment(distribution: String,
                                release: String,
                                versions: Vector[String] = Vector(
                                    ExecutionEnvironment.DefaultVersion
                                ))

object ExecutionEnvironment {
  val DefaultVersion = "0"
}

/**
  * Parameters for an instance type query. Any parameters that are None
  * will not be considered in the query.
  */
case class InstanceTypeRequest(dxInstanceType: Option[String] = None,
                               memoryMB: Option[Long] = None,
                               diskGB: Option[Long] = None,
                               diskType: Option[DiskType.DiskType] = None,
                               cpu: Option[Long] = None,
                               gpu: Option[Boolean] = None,
                               os: Option[ExecutionEnvironment] = None) {
  override def toString: String = {
    s"""memory=${memoryMB} disk=${diskGB} diskType=${diskType} cores=${cpu} gpu=${gpu} 
       |os=${os} instancetype=${dxInstanceType}""".stripMargin.replaceAll("\n", " ")
  }
}

object InstanceTypeRequest {
  lazy val empty: InstanceTypeRequest = InstanceTypeRequest()
}

// Instance Type on the platform. For example:
// name:   mem1_ssd1_x4
// memory: 4096 MB
// disk:   80 GB
// price:  0.5 dollar per hour
// os:     [(Ubuntu, 12.04), (Ubuntu, 14.04)}
case class DxInstanceType(name: String,
                          memoryMB: Long,
                          diskGB: Long,
                          cpu: Int,
                          gpu: Boolean,
                          os: Vector[ExecutionEnvironment],
                          diskType: Option[DiskType.DiskType] = None,
                          price: Option[Float] = None)
    extends Ordered[DxInstanceType] {

  // Does this instance satisfy the requirements?
  def satisfies(query: InstanceTypeRequest): Boolean = {
    if (query.dxInstanceType.contains(name)) {
      true
    } else {
      query.memoryMB.forall(_ <= memoryMB) &&
      query.diskGB.forall(_ <= diskGB) &&
      query.cpu.forall(_ <= cpu) &&
      query.gpu.forall(_ == gpu) &&
      query.os.forall(queryOs => os.contains(queryOs))
    }
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

  /**
    * Compare based on resource sizes. This is a partial ordering.
    * Optionally, adds some fuzziness to the comparison, because the instance
    * types don't have the exact memory and disk space that you would
    * expect (e.g. mem1_ssd1_x2 has less disk space than mem2_ssd1_x2) -
    * round down memory and disk sizes, to make the comparison insensitive to
    * minor differences.
    * @param that DxInstanceType
    * @param fuzzy whether to do fuzz or exact comparison
    * @return
    * @example If A has more memory, disk space, and cores than B, then B < A.
    */
  def compareByResources(that: DxInstanceType, fuzzy: Boolean = true): Option[Int] = {
    val (memDelta, diskDelta) = if (fuzzy) {
      ((this.memoryMB.toDouble / DxInstanceType.MemoryNormFactor) - (that.memoryMB.toDouble / DxInstanceType.MemoryNormFactor),
       (this.diskGB.toDouble / DxInstanceType.DiskNormFactor) - (that.diskGB.toDouble / DxInstanceType.DiskNormFactor))
    } else {
      ((this.memoryMB - that.memoryMB).toDouble, (this.diskGB - that.diskGB).toDouble)
    }
    val cpuDelta: Double = this.cpu - that.cpu
    val gpuDelta: Double = (if (this.gpu) 1 else 0) - (if (that.gpu) 1 else 0)
    Set(memDelta, diskDelta, cpuDelta, gpuDelta).toVector.sortWith(_ < _) match {
      case Vector(0.0)                  => Some(0)
      case deltas if deltas.head >= 0.0 => Some(1)
      case deltas if deltas.last <= 0.0 => Some(-1)
      case _                            => None
    }
  }

  // v2 instances are always better than v1 instances
  def compareByType(that: DxInstanceType): Int = {
    def typeVersion(name: String): Int = if (name contains DxInstanceType.Version2Suffix) 2 else 1
    typeVersion(this.name).compareTo(typeVersion(that.name))
  }

  def compare(that: DxInstanceType): Int = {
    // if prices are available, choose the cheapest instance. Otherwise,
    // choose one with minimal resources.
    val costDiff = if (price.isDefined) {
      compareByPrice(that)
    } else {
      compareByResources(that).getOrElse(0)
    }
    costDiff match {
      case 0 => -compareByType(that)
      case _ => costDiff
    }
  }
}

object DxInstanceType extends DefaultJsonProtocol {
  // support automatic conversion to/from JsValue
  implicit val diskTypeFormat: RootJsonFormat[DiskType.DiskType] = enumFormat(DiskType)
  implicit val execEnvFormat: RootJsonFormat[ExecutionEnvironment] = jsonFormat3(
      ExecutionEnvironment.apply
  )
  implicit val dxInstanceTypeFormat: RootJsonFormat[DxInstanceType] = jsonFormat8(
      DxInstanceType.apply
  )
  val Version2Suffix = "_v2"
  val MemoryNormFactor: Double = 1024.0
  val DiskNormFactor: Double = 16.0
}

case class InstanceTypeDB(instanceTypes: Map[String, DxInstanceType], pricingAvailable: Boolean) {
  // The cheapest available instance, this is normally also the smallest.
  private def selectMinimalInstanceType(
      instanceTypes: Iterable[DxInstanceType]
  ): Option[DxInstanceType] = {
    instanceTypes.toVector.sortWith(_ < _).headOption
  }

  /**
    * The default instance type for this database - the cheapest instance
    * type meeting minimal requirements. Used to run WDL expression
    * processing, launch jobs, etc.
    */
  lazy val defaultInstanceType: DxInstanceType = {
    val (v2InstanceTypes, v1InstanceTypes) = instanceTypes.values
      .filter { instanceType =>
        !instanceType.name.contains("test") &&
        instanceType.memoryMB >= InstanceTypeDB.MinMemory &&
        instanceType.cpu >= InstanceTypeDB.MinCpu
      }
      .partition(_.name.contains(DxInstanceType.Version2Suffix))
    // prefer v2 instance types
    selectMinimalInstanceType(v2InstanceTypes)
      .orElse(selectMinimalInstanceType(v1InstanceTypes))
      .getOrElse(
          throw new Exception(
              s"""no instance types meet the minimal requirements memory >= ${InstanceTypeDB.MinMemory} 
                 |AND cpu >= ${InstanceTypeDB.MinCpu}""".stripMargin.replaceAll("\n", " ")
          )
      )
  }

  def selectAll(query: InstanceTypeRequest): Iterable[DxInstanceType] = {
    val q = if (query.diskType.contains(DiskType.HDD)) {
      // we are ignoring disk type - only SSD instances are considered usable
      Logger.get.warning(
          s"dxCompiler only utilizes SSD instance types - ignoring request for HDD instance ${query}"
      )
      query.copy(diskType = None)
    } else {
      query
    }
    instanceTypes.values.filter(x => x.satisfies(q))
  }

  /**
    * From among all the instance types that satisfy the query, selects the
    * optimal one, where optimal is defined as cheapest if the price list is
    * available, otherwise ...
    */
  def selectOptimal(query: InstanceTypeRequest): Option[DxInstanceType] = {
    selectMinimalInstanceType(selectAll(query))
  }

  def selectByName(name: String): Option[DxInstanceType] = {
    if (instanceTypes.contains(name)) {
      return instanceTypes.get(name)
    }
    val nameWithoutHdd = name.replace("hdd", "ssd")
    if (instanceTypes.contains(nameWithoutHdd)) {
      Logger.get.warning(
          s"dxCompiler only utilizes SSD instance types - ignoring request for HDD instance ${nameWithoutHdd}"
      )
      return instanceTypes.get(nameWithoutHdd)
    }
    None
  }

  /**
    * Query the database. If `query.dxInstanceType` is set, the query will
    * return the specified instance type if it is in the database, otherwise it
    * will return None unless `force=true`. Falls back to querying the database
    * by requirements and returns the cheapest instance type that meets all
    * requirements, if any.
    * @param query the query
    * @param force whether to fall back to querying by requirements if the instance
    *              type name is set but is not found in the database.
    * @return
    */
  def get(query: InstanceTypeRequest, force: Boolean = false): Option[DxInstanceType] = {
    if (query.dxInstanceType.nonEmpty) {
      selectByName(query.dxInstanceType.get).orElse {
        if (force) {
          selectOptimal(query)
        } else {
          None
        }
      }
    } else {
      selectOptimal(query)
    }
  }

  def apply(query: InstanceTypeRequest): DxInstanceType = {
    get(query).getOrElse(
        throw new Exception(s"No instance types found that satisfy query ${query}")
    )
  }

  // check if instance type A is smaller or equal in requirements to
  // instance type B
  def compareByResources(name1: String, name2: String, fuzzy: Boolean = false): Option[Int] = {
    Vector(name1, name2).map(instanceTypes.get) match {
      case Vector(Some(i1), Some(i2)) =>
        i1.compareByResources(i2, fuzzy = fuzzy)
      case _ =>
        throw new Exception(
            s"cannot compare instance types ${name1}, ${name2} - at least one is not in the database"
        )
    }
  }

  /**
    * Formats the database as a prettified String. Instance types are sorted.
    * @return
    */
  def prettyFormat(): String = {
    instanceTypes.values.toVector.sortWith(_ < _).toJson.prettyPrint
  }
}

object InstanceTypeDB extends DefaultJsonProtocol {
  val MinMemory: Long = 3 * 1024
  val MinCpu: Int = 2
  val CpuKey = "numCores"
  val MemoryKey = "totalMemoryMB"
  val DiskKey = "ephemeralStorageGB"
  val OsKey = "os"
  val DistributionKey = "distribution"
  val ReleaseKey = "release"
  val VersionKey = "version"
  val GpuSuffix = "_gpu"
  val SsdSuffix = "_ssd"
  val HddSuffix = "_hdd"

  // support automatic conversion to/from JsValue
  implicit val instanceTypeDBFormat: RootJsonFormat[InstanceTypeDB] =
    new RootJsonFormat[InstanceTypeDB] {
      override def write(obj: InstanceTypeDB): JsValue = {
        JsObject("instanceTypes" -> obj.instanceTypes.toJson,
                 "pricingAvailable" -> JsBoolean(obj.pricingAvailable))
      }

      override def read(json: JsValue): InstanceTypeDB = {
        val Seq(instanceTypes, JsBoolean(pricingAvailable)) =
          json.asJsObject.getFields("instanceTypes", "pricingAvailable")
        InstanceTypeDB(instanceTypes.convertTo[Map[String, DxInstanceType]],
                       pricingAvailable = pricingAvailable)
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
    if (db.instanceTypes.nonEmpty && db.pricingAvailable) {
      // sort the prices from low to high, and then replace with rank.
      val opaqueInstances =
        db.instanceTypes.toVector
          .sortWith(_._2.price.get < _._2.price.get)
          .zipWithIndex
          .map {
            case ((name, instanceType), index) =>
              name -> instanceType.copy(price = Some((index + 1).toFloat))
          }
          .toMap
      InstanceTypeDB(opaqueInstances, pricingAvailable = true)
    } else {
      db
    }
  }

  private def parseInstanceTypes(jsv: JsValue): Map[String, DxInstanceType] = {
    // convert to a list of DxInstanceTypes, with prices set to zero
    jsv.asJsObject.fields.map {
      case (name, jsValue) =>
        val numCores = JsUtils.getInt(jsValue, Some(CpuKey))
        val memoryMB = JsUtils.getInt(jsValue, Some(MemoryKey))
        val diskSpaceGB = JsUtils.getInt(jsValue, Some(DiskKey))
        val os = JsUtils.getValues(jsValue, Some(OsKey)).map {
          case obj: JsObject =>
            val distribution = JsUtils.getString(obj, Some(DistributionKey))
            val release = JsUtils.getString(obj, Some(ReleaseKey))
            val versions = obj.fields.get(VersionKey) match {
              case Some(JsArray(array)) => array.map(JsUtils.getString(_))
              case None                 => Vector(ExecutionEnvironment.DefaultVersion)
              case other =>
                throw new Exception(s"expected 'version' to be a string, not ${other}")
            }
            ExecutionEnvironment(distribution, release, versions)
          case other =>
            throw new Exception(s"invalid os specification ${other}")
        }
        // disk type and gpu details aren't reported by the API, so we
        // have to determine them from the instance type name
        val diskType = name.toLowerCase match {
          case s if s.contains(SsdSuffix) => Some(DiskType.SSD)
          case s if s.contains(HddSuffix) => Some(DiskType.HDD)
          case _                          => None
        }
        val gpu = name.toLowerCase.contains(GpuSuffix)
        name -> DxInstanceType(name, memoryMB, diskSpaceGB, numCores, gpu, os, diskType)
    }
  }

  // Get the mapping from instance type to price, limited to the
  // project we are in. Describing a user requires permission to
  // view the user account. The compiler may not have these
  // permissions, causing this method to throw an exception.
  // TODO: move this to DxOrg and DxUser objects
  private def getPricingModel(dxApi: DxApi, billTo: String, region: String): Map[String, Float] = {
    val request = Map("fields" -> JsObject("pricingModelsByRegion" -> JsTrue))
    val response = billTo match {
      case _ if billTo.startsWith("org") =>
        dxApi.orgDescribe(billTo, request)
      case _ if billTo.startsWith("user") =>
        dxApi.userDescribe(billTo, request)
      case _ =>
        throw new Exception(s"Invalid billTo ${billTo}")
    }
    val pricingModelsByRegion = JsUtils.getFields(response, Some("pricingModelsByRegion"))
    val computeRatesPerHour =
      JsUtils.getFields(pricingModelsByRegion(region), Some("computeRatesPerHour"))
    // convert from JsValue to a Map
    computeRatesPerHour.map {
      case (name, jsValue) =>
        val hourlyRate: Float = jsValue match {
          case JsNumber(x) => x.toFloat
          case JsString(x) => x.toFloat
          case _           => throw new Exception(s"compute rate is not a number ${jsValue.prettyPrint}")
        }
        name -> hourlyRate
    }
  }

  /**
    * Query the platform for the available instance types in this project.
    * @param dxApi DxApi
    * @param instanceTypeFilter function used to filter instance types
    */
  def create(dxProject: DxProject,
             instanceTypeFilter: DxInstanceType => Boolean,
             dxApi: Option[DxApi] = None,
             logger: Logger = Logger.get): InstanceTypeDB = {
    val api = dxApi.getOrElse(dxProject.dxApi)
    val projectDesc =
      dxProject.describe(Set(Field.Region, Field.BillTo, Field.AvailableInstanceTypes))
    val allInstanceTypes = parseInstanceTypes(
        projectDesc.availableInstanceTypes.getOrElse(
            throw new Exception(
                s"could not retrieve available instance types for project ${dxProject}"
            )
        )
    )
    val (availableInstanceTypes, pricingAvailable) =
      try {
        val pricingModel = getPricingModel(api, projectDesc.billTo.get, projectDesc.region.get)
        val instanceTypesWithPrices = allInstanceTypes.keySet
          .intersect(pricingModel.keySet)
          .map { name =>
            name -> allInstanceTypes(name).copy(price = pricingModel.get(name))
          }
          .toMap
        (instanceTypesWithPrices, true)
      } catch {
        case ex: Throwable =>
          logger.warning(
              """|Warning: insufficient permissions to retrieve the
                 |instance price list. This will result in suboptimal machine choices,
                 |incurring higher costs when running workflows.
                 |""".stripMargin.replaceAll("\n", " "),
              exception = Some(ex)
          )
          (allInstanceTypes, false)
      }
    val usableInstanceTypes = availableInstanceTypes.filter {
      case (_, instanceType) => instanceTypeFilter(instanceType)
    }
    InstanceTypeDB(usableInstanceTypes, pricingAvailable = pricingAvailable)
  }
}
