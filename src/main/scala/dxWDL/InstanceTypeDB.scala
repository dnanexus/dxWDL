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
import wdl4s.types._
import wdl4s.values._

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
    def choose(memoryMB: Option[Int], diskGB: Option[Int], cpu: Option[Int]) : String = {
        // discard all instances that are too weak
        val sufficient: Vector[DxInstanceType] =
            instances.filter(x => x.satisfies(memoryMB, diskGB, cpu))
        if (sufficient.length == 0)
            throw new Exception(s"No instances found that match the requirements (memory=$memoryMB, diskGB=$diskGB, cpu=$cpu")

        // if prices ara available, choose the cheapest instance. Otherwise,
        // choose one with minimal resources.
        val initialGuess = sufficient.head
        val bestInstance = sufficient.tail.foldLeft(initialGuess){ case (bestSoFar,x) =>
            if (x.lteq(bestSoFar)) x
            else bestSoFar
        }
        bestInstance.name
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
    def apply(wdlMemoryMB: Option[WdlValue],
              wdlDiskGB: Option[WdlValue],
              wdlCpu: Option[WdlValue]) : String = {
        // Examples for memory specification: "4000 MB", "1 GB"
        val memoryMB: Option[Int] = wdlMemoryMB match {
            case None => None
            case Some(WdlString(buf)) =>
                val components = buf.split("\\s+")
                if (components.length != 2)
                    throw new Exception(s"Can not parse memory specification ${buf}")
                val i = try { components(0).toInt } catch {
                    case e: Throwable =>
                        throw new Exception(s"Parse error for memory specification ${buf}")
                }
                val mem: Int = components(1) match {
                    case "MB" | "M" => i
                    case "GB" | "G" => i * 1024
                    case "TB" | "T" => i * 1024 * 1024
                }
                Some(mem)
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

        choose(memoryMB, diskGB, cpu)
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

    // describe a project, and extract fields that not currently available
    // through dxjava.
    private def getProjectExtraInfo(dxProject: DXProject) : (String,String) = {
        val rep = DXAPI.projectDescribe(dxProject.getId(), classOf[JsonNode])
        val jso:JsObject = Utils.jsValueOfJsonNode(rep).asJsObject

        val billTo = jso.fields.get("billTo") match {
            case Some(JsString(x)) => x
            case _ => throw new Exception(s"Failed to get billTo from project ${dxProject.getId()}")
        }
        val region = jso.fields.get("region") match {
            case Some(JsString(x)) => x
            case _ => throw new Exception(s"Failed to get region from project ${dxProject.getId()}")
        }
        (billTo,region)
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

    private def buildNoPriceDB(allAvailableIT: Map[String, DxInstanceType]) : InstanceTypeDB = {
        // filter out instances that we cannot use
        val iTypes: Vector[DxInstanceType] = allAvailableIT
            .filter{ case (iName,traits) => instanceCriteria(traits) }
            .map{ case (_,traits) => traits}
            .toVector
        InstanceTypeDB(iTypes)
    }

    def query(dxProject: DXProject) : InstanceTypeDB = {
        // Figure out the available instances by describing the project
        val allAvailableIT = queryAvailableInstanceTypes(dxProject)

        // get billTo and region from the project
        val (billTo, region) = getProjectExtraInfo(dxProject)

        try {
            // get the pricing model
            val pm = getPricingModel(billTo, region)

            // Create fully formed instance types by crossing the tables
            val availableInstanceTypes: Vector[DxInstanceType] = crossTables(allAvailableIT, pm)

            // filter out instances that we cannot use
            var iTypes = availableInstanceTypes.filter(instanceCriteria)

            // Do not use overly expensive instances
            iTypes = iTypes.filter(x => x.price <= Utils.MAX_HOURLY_RATE)
            InstanceTypeDB(iTypes)
        } catch {
            // Insufficient permissions to describe the user, we cannot get the price list.
            case e: Throwable =>
                System.err.println("""|Warning: insufficient permissions to retrive the
                                      |instance price list. This will result in suboptimal machine choices,
                                      |incurring higher costs when running workflows.""")
                buildNoPriceDB(allAvailableIT)
        }
    }

    def queryNoPriceInfo(dxProject: DXProject) : InstanceTypeDB = {
        // Figure out the available instances by describing the project
        val allAvailableIT = queryAvailableInstanceTypes(dxProject)
        buildNoPriceDB(allAvailableIT)
    }
}
