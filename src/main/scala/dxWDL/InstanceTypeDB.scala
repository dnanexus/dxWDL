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
        // step one: discard all instances that are too weak
        val sufficient: Vector[DxInstanceType] =
            instances.filter(x => x.satisfies(memoryMB, diskGB, cpu))
        if (sufficient.length == 0)
            throw new Exception(s"No instances found that match the requirements (memory=$memoryMB, diskGB=$diskGB, cpu=$cpu")

        // step two: choose the cheapest instance
        val initialGuess = sufficient.head
        val bestInstance = sufficient.tail.foldLeft(initialGuess){ case (bestSoFar,x) =>
            if (x.price < bestSoFar.price) x else bestSoFar
        }
        bestInstance.name
    }

    // The cheapest available instance, this is normally also the smallest.
    def getMinimalInstanceType() : String = {
        if (instances.isEmpty)
            throw new Exception("instance type database is empty")
        val cheapest = instances.tail.foldLeft(instances.head){
            case (cheapest, elem) =>
                if (elem.price < cheapest.price)
                    elem
                else
                    cheapest
        }
        cheapest.name
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

}


object InstanceTypeDB extends DefaultJsonProtocol {
    // support automatic conversion to/from JsValue
    implicit val instanceTypeDBFormat = jsonFormat1(InstanceTypeDB.apply)

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
    def genHardcoded : InstanceTypeDB = {
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
            val price: Float = awsOnDemandHourlyPriceTable(internalName)

            val traits = fields("traits").asJsObject.fields
            val memoryMB = intOfJs(traits("totalMemoryMB"))
            val diskGB = intOfJs(traits("ephemeralStorageGB"))
            val cpu = intOfJs(traits("numCores"))
            DxInstanceType(name, memoryMB, diskGB, cpu, price, Vector.empty)
        }.toVector
        InstanceTypeDB(db)
    }

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

    // Get the mapping from instance type to price, limited to the project
    // we are in.
    private def getPricingModel(billTo:String,
                                region:String) : Map[String, Float] = {
        val req: ObjectNode = DXJSON.getObjectBuilder()
            .put("fields",
                 DXJSON.getObjectBuilder().put("pricingModelsByRegion", true)
                     .build())
            .build()
        val rep = DXAPI.userDescribe(billTo, req, classOf[JsonNode])
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
    // - Instance is not overly expensive. Currently, the
    //   threshold is set to the arbitrary number 10$ an hour.
    //
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
        if (iType.price > Utils.MAX_HOURLY_RATE)
            return false
        return true
    }

    private def query(dxProject: DXProject) : InstanceTypeDB = {
        // Figure out the available instances by describing the project
        val allAvailableIT = queryAvailableInstanceTypes(dxProject)

        // get billTo and region from the project
        val (billTo, region) = getProjectExtraInfo(dxProject)

        // get the pricing model
        val pm = getPricingModel(billTo, region)

        // Create fully formed instance types by crossing the tables
        val availableInstanceTypes: Vector[DxInstanceType] = crossTables(allAvailableIT, pm)

        // filter out instances that we do not want to use
        InstanceTypeDB(
            availableInstanceTypes.filter(instanceCriteria)
        )
    }

    def queryWithBackup(dxProject: DXProject) : InstanceTypeDB = {
        try {
            query(dxProject)
        } catch {
            case e: Throwable =>
                System.err.println(
                    """|Error querying the platform for
                       |available instances and their prices.
                       |Failing back to hardcoded list."""
                        .stripMargin.replaceAll("\n", " "))
                System.err.println(Utils.exceptionToString(e))
                genHardcoded
        }
    }
}
