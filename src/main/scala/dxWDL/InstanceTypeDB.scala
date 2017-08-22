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

package dxWDL

import com.dnanexus.{DXAPI, DXJSON, DXProject}
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import java.nio.file.Path
import spray.json._
import spray.json.DefaultJsonProtocol
import wdl4s.wdl.types._
import wdl4s.wdl.values._

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
                          os: Vector[(String, String)]) {
    // Does this instance satisfy the requirements?
    def satisfies(memReq: Option[Int],
                  diskReq: Option[Int],
                  cpuReq: Option[Int]) : Boolean = {
        memReq match {
            case Some(x) => if (memoryMB < x) return false
            case None => ()
        }
        diskReq match {
            case Some(x) => if (diskGB < x) return false
            case None => ()
        }
        cpuReq match {
            case Some(x) => if (cpu < x) return false
            case None => ()
        }
        return true
    }

    // Comparison function. Returns true iff [this] comes before
    // [that] in the ordering.
    //
    // If the hourly price list per instance in available, we sort by
    // price. If we do not have permissions for pricing information,
    // we compare by resources sizes. For example, if A has more
    // memory, disk space, and cores than B, then B < A. We round down
    // memory and disk sizes, to make the comparison insensitive to
    // minor differences.
    def lteq(that: DxInstanceType) : Boolean = {
        // compare by price
        if (this.price < that.price)
            return true
        if (this.price > that.price)
            return false

        // Prices are the same, compare based on resource sizes.
        return ((this.memoryMB / 1024) <= (that.memoryMB / 1024) &&
                    (this.diskGB / 16) <= (that.diskGB / 16) &&
                    this.cpu <= that.cpu)
    }
}

// support automatic conversion to/from JsValue
object DxInstanceType extends DefaultJsonProtocol {
    implicit val dxInstanceTypeFormat = jsonFormat6(DxInstanceType.apply)
}

case class InstanceTypeDB(instances: Vector[DxInstanceType]) {
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
    private def choose3Attr(memoryMB: Option[Int],
                            diskGB: Option[Int],
                            cpu: Option[Int]) : String = {
        // discard all instances that are too weak
        val sufficient: Vector[DxInstanceType] =
            instances.filter(x => x.satisfies(memoryMB, diskGB, cpu))
        if (sufficient.length == 0)
            throw new Exception(s"No instances found that match the requirements (memory=$memoryMB, diskGB=$diskGB, cpu=$cpu")

        // if prices are available, choose the cheapest instance. Otherwise,
        // choose one with minimal resources.
        val initialGuess = sufficient.head
        val bestInstance = sufficient.tail.foldLeft(initialGuess){ case (bestSoFar,x) =>
            if (x.lteq(bestSoFar)) x
            else bestSoFar
        }
        bestInstance.name
    }

    def choose(iTypeShortcut: Option[String],
               memoryMB: Option[Int],
               diskGB: Option[Int],
               cpu: Option[Int]) : String = {
        iTypeShortcut match {
            case Some(iType) =>
                // Short circut the calculation, and just choose this instance.
                // Make sure it is available.
                instances.find(x => x.name == iType) match {
                    case Some(x) =>
                        // instance exists, and can be used
                        iType
                    case None =>
                        // Probably a bad instance name
                        throw new Exception(s"""|Instance type ${iTypeShortcut} is unavailable
                                                |or badly named"""
                                                .stripMargin.replaceAll("\n", " "))
                }
            case None => choose3Attr(memoryMB, diskGB, cpu)
        }
    }

    private def calcMinimalInstanceType(iTypes: Set[DxInstanceType]) : DxInstanceType = {
        if (iTypes.isEmpty)
            throw new Exception("empty list")
        iTypes.tail.foldLeft(iTypes.head) {
            case (cheapest, elem) =>
                if (elem.lteq(cheapest)) elem
                else cheapest
        }
    }

    // The cheapest available instance, this is normally also the smallest.
    def getMinimalInstanceType() : String = {
        calcMinimalInstanceType(instances.toSet).name
    }

    // Currently, we support only constants.
    def apply(dxInstaceType: Option[WdlValue],
              wdlMemoryMB: Option[WdlValue],
              wdlDiskGB: Option[WdlValue],
              wdlCpu: Option[WdlValue]) : String = {
        // Shortcut the entire calculation, and provide the dx instance type directly
        val iTypeShortcut : Option[String] = dxInstaceType match {
            case None => None
            case Some(WdlString(iType)) => Some(iType)
            case Some(x) =>
                throw new Exception(s"""|dxInstaceType has to evaluate to a
                                        |WdlString type ${x.toWdlString}"""
                                        .stripMargin.replaceAll("\n", " "))
        }

        // Examples for memory specification: "4000 MB", "1 GB"
        val memoryMB: Option[Int] = wdlMemoryMB match {
            case None => None
            case Some(WdlString(buf)) =>
                // extract number
                val numRex = """(\d+.?\d*)""".r
                val numbers = numRex.findAllIn(buf).toList
                if (numbers.length != 1)
                    throw new Exception(s"Can not parse memory specification ${buf}")
                val number:String = numbers(0)
                val x:Float = try { number.toFloat } catch {
                    case e: Throwable =>
                        throw new Exception(s"Unrecognized number ${number}")
                }

                // extract memory units
                val memUnitRex = """([a-zA-Z]+)""".r
                val memUnits = memUnitRex.findAllIn(buf).toList
                if (memUnits.length != 1)
                    throw new Exception(s"Can not parse memory specification ${buf}")
                val memUnit:String = memUnits(0).toLowerCase
                val mem: Float = memUnit match {
                    case "mib" | "mb" | "m" => x
                    case "gib" | "gb" | "g" => x * 1024
                    case "tib" | "tb" | "t" => x * 1024 * 1024
                    case _ => throw new Exception(s"Unknown memory unit ${memUnit}")
                }
                Some(mem.ceil.round)
            case Some(x) =>
                throw new Exception(s"Memory has to evaluate to a WdlString type ${x.toWdlString}")
        }

        // Examples: "local-disk 1024 HDD"
        val diskGB: Option[Int] = wdlDiskGB match {
            case None => None
            case Some(WdlString(buf)) =>
                val components = buf.split("\\s+")
                val ignoreWords = Set("local-disk", "hdd", "sdd")
                val l = components.filter(x => !(ignoreWords contains x.toLowerCase))
                if (l.length != 1)
                    throw new Exception(s"Can't parse disk space specification ${buf}")
                val i = try { l(0).toInt } catch {
                    case e: Throwable =>
                        throw new Exception(s"Parse error for diskSpace attribute ${buf}")
                }
                Some(i)
            case Some(x) =>
                throw new Exception(s"Disk space has to evaluate to a WdlString type ${x.toWdlString}")
        }

        // Examples: "1", "12"
        val cpu: Option[Int] = wdlCpu match {
            case None => None
            case Some(WdlString(buf)) =>
                val i:Int = try { buf.toInt } catch {
                    case e: Throwable =>
                        throw new Exception(s"Parse error for cpu specification ${buf}")
                }
                Some(i)
            case Some(WdlInteger(i)) => Some(i)
            case Some(WdlFloat(x)) => Some(x.toInt)
            case Some(x) => throw new Exception(s"Cpu has to evaluate to a numeric value ${x}")
        }

        choose(iTypeShortcut, memoryMB, diskGB, cpu)
    }

    // sort the instances, and print them out
    def prettyPrint() : String = {
        var remain : Set[DxInstanceType] = instances.toSet
        var sortediTypes : Vector[DxInstanceType] = Vector()
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
    implicit val instanceTypeDBFormat = jsonFormat1(InstanceTypeDB.apply)

    // Extract an integer fields from a JsObject
    private def getJsIntField(js: JsValue, fieldName:String) : Int = {
        js.asJsObject.fields.get(fieldName) match {
            case Some(JsNumber(x)) => x.toInt
            case Some(JsString(x)) => x.toInt
            case _ => throw new Exception(s"Missing field ${fieldName} in JSON ${js.prettyPrint}}")
        }
    }

    private def getJsStringField(js: JsValue, fieldName:String) : String = {
        js.asJsObject.fields.get(fieldName) match {
            case Some(JsNumber(x)) => x.toString
            case Some(JsString(x)) => x
            case _ => throw new Exception(s"Missing field ${fieldName} in JSON ${js.prettyPrint}}")
        }
    }

    private def getJsField(js: JsValue, fieldName: String) : JsValue = {
        js.asJsObject.fields.get(fieldName) match {
            case Some(x:JsValue) => x
            case None => throw new Exception(s"Missing field ${fieldName} in ${js.prettyPrint}")
        }
    }

    // Query the platform for the available instance types in
    // this project.
    private def queryAvailableInstanceTypes(dxProject: DXProject) : Map[String, DxInstanceType] = {
        // get List of supported OSes
        def getSupportedOSes(js: JsValue) : Vector[(String, String)]= {
            val osSupported:Vector[JsValue] = js.asJsObject.fields.get("os") match {
                case Some(JsArray(x)) => x
                case _ => throw new Exception(s"Missing field os in JSON ${js.prettyPrint}")
            }
            osSupported.map{ elem =>
                val distribution = getJsStringField(elem, "distribution")
                val release = getJsStringField(elem, "release")
                distribution -> release
            }.toVector
        }

        val availableField = "availableInstanceTypes"
        val req: ObjectNode = DXJSON.getObjectBuilder()
            .put("fields",
                 DXJSON.getObjectBuilder().put(availableField, true)
                     .build())
            .build()
        val rep = DXAPI.projectDescribe(dxProject.getId(), req, classOf[JsonNode])
        val repJs:JsValue = Utils.jsValueOfJsonNode(rep)
        val availableInstanceTypes:JsValue =
            repJs.asJsObject.fields.get(availableField) match {
                case Some(x) => x
                case None => throw new Exception(
                    s"Field ${availableField} is missing ${repJs.prettyPrint}")
            }

        // convert to a list of DxInstanceTypes, with prices set to zero
        availableInstanceTypes.asJsObject.fields.map{ case (iName, jsValue) =>
            val numCores = getJsIntField(jsValue, "numCores")
            val memoryMB = getJsIntField(jsValue, "totalMemoryMB")
            val diskSpaceGB = getJsIntField(jsValue, "ephemeralStorageGB")
            val os = getSupportedOSes(jsValue)
            val dxInstanceType = DxInstanceType(iName, memoryMB, diskSpaceGB, numCores, 0, os)
            iName -> dxInstanceType
        }.toMap
    }

    // Get the mapping from instance type to price, limited to the
    // project we are in. Describing a user requires permission to
    // view the user account. The compiler may not have these
    // permissions, causing this method to throw an exception.
    private def getPricingModel(billTo:String,
                                region:String) : Map[String, Float] = {
        val req: ObjectNode = DXJSON.getObjectBuilder()
            .put("fields",
                 DXJSON.getObjectBuilder().put("pricingModelsByRegion", true)
                     .build())
            .build()

        val rep = try {
            DXAPI.userDescribe(billTo, req, classOf[JsonNode])
        } catch {
            case e: Throwable =>
                throw new Exception("Insufficient permissions")
        }

        val js: JsValue = Utils.jsValueOfJsonNode(rep)
        val pricingModelsByRegion = getJsField(js, "pricingModelsByRegion")
        val pricingModel = getJsField(pricingModelsByRegion, region)
        val computeRatesPerHour = getJsField(pricingModel, "computeRatesPerHour")

        // convert from JsValue to a Map
        computeRatesPerHour.asJsObject.fields.map{ case (key, jsValue) =>
            val hourlyRate:Float = jsValue match {
                case JsNumber(x) => x.toFloat
                case JsString(x) => x.toFloat
                case _ => throw new Exception(s"compute rate is not a number ${jsValue.prettyPrint}")
            }
            key -> hourlyRate
        }.toMap
    }

    private def crossTables(availableIT: Map[String, DxInstanceType],
                            pm: Map[String, Float]): Vector[DxInstanceType] = {
        pm.map{ case (iName, hourlyRate) =>
            availableIT.get(iName) match {
                case None => None
                case Some(iType) =>
                    Some(DxInstanceType(iName, iType.memoryMB, iType.diskGB,
                                        iType.cpu, hourlyRate, iType.os))
            }
        }.flatten.toVector
    }

    // Check if an instance type passes some basic criteria:
    // - Instance must support Ubuntu 14.04.
    // - Instance is not a GPU instance.
    private def instanceCriteria(iType: DxInstanceType) : Boolean = {
        val osSupported = iType.os.foldLeft(false) {
            case (accu, (distribution, release)) =>
                if (release == "14.04")
                    true
                else
                    accu
        }
        if (!osSupported)
            return false
        if (iType.name contains "gpu")
            return false
        return true
    }

    private def queryNoPrices(dxProject: DXProject) : InstanceTypeDB = {
        // Figure out the available instances by describing the project
        val allAvailableIT = queryAvailableInstanceTypes(dxProject)

        // filter out instances that we cannot use
        val iTypes: Vector[DxInstanceType] = allAvailableIT
            .filter{ case (iName,traits) => instanceCriteria(traits) }
            .map{ case (_,traits) => traits}
            .toVector
        InstanceTypeDB(iTypes)
    }

    private def queryWithPrices(dxProject: DXProject) : InstanceTypeDB = {
        // Figure out the available instances by describing the project
        val allAvailableIT = queryAvailableInstanceTypes(dxProject)

        // get billTo and region from the project
        val (billTo, region) = Utils.projectDescribeExtraInfo(dxProject)

        // get the pricing model
        val pm = getPricingModel(billTo, region)

        // Create fully formed instance types by crossing the tables
        val availableInstanceTypes: Vector[DxInstanceType] = crossTables(allAvailableIT, pm)

        // filter out instances that we cannot use
        var iTypes = availableInstanceTypes.filter(instanceCriteria)

        // Do not use overly expensive instances, this a temporary
        // safe guard. We don't want the compiler to accidentally
        // choose expensive instances, annoying the user.
        iTypes = iTypes.filter(x => x.price <= Utils.MAX_HOURLY_RATE)
        InstanceTypeDB(iTypes)
    }

    def query(dxProject: DXProject) : InstanceTypeDB = {
        try {
            queryWithPrices(dxProject)
        } catch {
            // Insufficient permissions to describe the user, we cannot get the price list.
            case e: Throwable =>
                System.err.println(
                    """|Warning: insufficient permissions to retrieve the
                       |instance price list. This will result in suboptimal machine choices,
                       |incurring higher costs when running workflows."""
                        .stripMargin.replaceAll("\n", " "))
                queryNoPrices(dxProject)
        }
    }

    // The original list is at:
    // https://github.com/dnanexus/nucleus/blob/master/node_modules/instance_types/aws_instance_types.json
    //
    // The g2,i2,x1 instances have been removed, because they are not
    // enabled for customers by default.  In addition, the PV (Paravirtual)
    // instances have been removed, because they work only on Ubuntu
    // 12.04.
    //
    // Removed the ssd2 instances, because they actually use EBS storage. A better
    // solution would be asking the platform for the available instances.
    private val instanceList : String = """{
        "mem2_ssd1_x2":       {"internalName": "m3.large",                          "traits": {"numCores":   2, "totalMemoryMB":    7225, "ephemeralStorageGB":   27}},
        "mem2_ssd1_x4":       {"internalName": "m3.xlarge",                         "traits": {"numCores":   4, "totalMemoryMB":   14785, "ephemeralStorageGB":   72}},
        "mem2_ssd1_x8":       {"internalName": "m3.2xlarge",                        "traits": {"numCores":   8, "totalMemoryMB":   29905, "ephemeralStorageGB":  147}},
        "mem1_ssd1_x2":       {"internalName": "c3.large",                          "traits": {"numCores":   2, "totalMemoryMB":    3766, "ephemeralStorageGB":   28}},
        "mem1_ssd1_x4":       {"internalName": "c3.xlarge",                         "traits": {"numCores":   4, "totalMemoryMB":    7225, "ephemeralStorageGB":   77}},
        "mem1_ssd1_x8":       {"internalName": "c3.2xlarge",                        "traits": {"numCores":   8, "totalMemoryMB":   14785, "ephemeralStorageGB":  157}},
        "mem1_ssd1_x16":      {"internalName": "c3.4xlarge",                        "traits": {"numCores":  16, "totalMemoryMB":   29900, "ephemeralStorageGB":  302}},
        "mem1_ssd1_x32":      {"internalName": "c3.8xlarge",                        "traits": {"numCores":  32, "totalMemoryMB":   60139, "ephemeralStorageGB":  637}},
        "mem3_ssd1_x32_gen1": {"internalName": "cr1.8xlarge",                       "traits": {"numCores":  32, "totalMemoryMB":  245751, "ephemeralStorageGB":  237}},
        "mem3_ssd1_x2":       {"internalName": "r3.large",                          "traits": {"numCores":   2, "totalMemoryMB":   15044, "ephemeralStorageGB":   27}},
        "mem3_ssd1_x4":       {"internalName": "r3.xlarge",                         "traits": {"numCores":   4, "totalMemoryMB":   30425, "ephemeralStorageGB":   72}},
        "mem3_ssd1_x8":       {"internalName": "r3.2xlarge",                        "traits": {"numCores":   8, "totalMemoryMB":   61187, "ephemeralStorageGB":  147}},
        "mem3_ssd1_x16":      {"internalName": "r3.4xlarge",                        "traits": {"numCores":  16, "totalMemoryMB":  122705, "ephemeralStorageGB":  297}},
        "mem3_ssd1_x32":      {"internalName": "r3.8xlarge",                        "traits": {"numCores":  32, "totalMemoryMB":  245751, "ephemeralStorageGB":  597}}
}"""

    private val awsOnDemandHourlyPrice =
                """|{
           | "cc2.8xlarge": 2.000,
           | "cg1.4xlarge": 2.100,
           | "m4.large": 0.108,
           | "m4.xlarge": 0.215,
           | "m4.2xlarge": 0.431,
           | "m4.4xlarge": 0.862,
           | "m4.10xlarge": 2.155,
           | "m4.16xlarge": 3.447,
           | "c4.large": 0.100,
           | "c4.xlarge": 0.199,
           | "c4.2xlarge": 0.398,
           | "c4.4xlarge": 0.796,
           | "c4.8xlarge": 1.591,
           | "p2.xlarge": 0.900,
           | "p2.8xlarge": 7.200,
           | "p2.16xlarge": 14.400,
           | "g2.2xlarge": 0.650,
           | "g2.8xlarge": 2.600,
           | "x1.16xlarge": 6.669,
           | "x1.32xlarge": 13.338,
           | "r4.large": 0.133,
           | "r4.xlarge": 0.266,
           | "r4.2xlarge": 0.532,
           | "r4.4xlarge": 1.064,
           | "r4.8xlarge": 2.128,
           | "r4.16xlarge": 4.256,
           | "r3.large": 0.166,
           | "r3.xlarge": 0.333,
           | "r3.2xlarge": 0.665,
           | "r3.4xlarge": 1.330,
           | "r3.8xlarge": 2.660,
           | "i2.xlarge": 0.853,
           | "i2.2xlarge": 1.705,
           | "i2.4xlarge": 3.410,
           | "i2.8xlarge": 6.820,
           | "d2.xlarge": 0.690,
           | "d2.2xlarge": 1.380,
           | "d2.4xlarge": 2.760,
           | "d2.8xlarge": 5.520,
           | "hi1.4xlarge": 3.100,
           | "hs1.8xlarge": 4.600,
           | "m3.medium": 0.067,
           | "m3.large": 0.133,
           | "m3.xlarge": 0.266,
           | "m3.2xlarge": 0.532,
           | "c3.large": 0.090,
           | "c3.xlarge": 0.210,
           | "c3.2xlarge": 0.420,
           | "c3.4xlarge": 0.840,
           | "c3.8xlarge": 1.680,
           | "m1.small": 0.044,
           | "m1.medium": 0.087,
           | "m1.large": 0.175,
           | "m1.xlarge": 0.350,
           | "c1.medium": 0.130,
           | "c1.xlarge": 0.520,
           | "m2.xlarge": 0.245,
           | "m2.2xlarge": 0.490,
           | "m2.4xlarge": 0.980,
           | "t1.micro": 0.020,
           | "cr1.8xlarge": 3.500
           |}
                   |""".stripMargin.trim


    // Create an availble instance list based on a hard coded list
    def genTestDB(pricingInfo: Boolean) : InstanceTypeDB = {
        def intOfJs(jsVal : JsValue) : Int = {
            jsVal match {
                case JsNumber(x) => x.toInt
                case _ => throw new Exception("sanity")
            }
        }
        val awsOnDemandHourlyPriceTable: Map[String, Float] = {
            val fields : Map[String, JsValue] = awsOnDemandHourlyPrice.parseJson.asJsObject.fields
            fields.map{ case(name, v) =>
                val price: Float = v match {
                    case JsNumber(x) => x.toFloat
                    case _ => throw new Exception("sanity")
                }
                name -> price
            }.toMap
        }

        val allInstances : Map[String, JsValue] = instanceList.parseJson.asJsObject.fields
        val db = allInstances.map{ case(name, v) =>
            val fields : Map[String, JsValue] = v.asJsObject.fields
            val internalName = fields("internalName") match {
                case JsString(s) => s
                case _ => throw new Exception("sanity")
            }
            val price: Float =
                if (pricingInfo) awsOnDemandHourlyPriceTable(internalName)
                else 0
            val traits = fields("traits").asJsObject.fields
            val memoryMB = intOfJs(traits("totalMemoryMB"))
            val diskGB = intOfJs(traits("ephemeralStorageGB"))
            val cpu = intOfJs(traits("numCores"))
            DxInstanceType(name, memoryMB, diskGB, cpu, price, Vector.empty)
        }.toVector
        InstanceTypeDB(db)
    }
}
