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
memory      GB of RAM
disk        GB of disk space, hard drive or flash drive
cpu         1 per core
price       comparative price

  */

package dxWDL

import com.dnanexus.{DXAPI, DXJSON, DXProject}
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import spray.json._
import spray.json.DefaultJsonProtocol
import wdl4s.types._
import wdl4s.values._

case class DxInstance(name: String, memory: Int, disk: Int, cpu: Int, price: Float) {

    // Does this instance satisfy the requirements?
    def satisfies(memReq: Option[Int],
                  diskReq: Option[Int],
                  cpuReq: Option[Int]) : Boolean = {
        memReq match {
            case Some(x) => if (memory < x) return false
            case None => ()
        }
        diskReq match {
            case Some(x) => if (disk < x) return false
            case None => ()
        }
        cpuReq match {
            case Some(x) => if (cpu < x) return false
            case None => ()
        }
        return true
    }
}

case class InstanceTypeDB(instanceDB: List[DxInstance]) {
    // Calculate the dx instance type that fits best, based on
    // runtime specifications.
    //
    // memory:    minimal amount of RAM, specified in GB
    // diskSpace: minimal amount of disk space, specified in GB
    // numCores:  minimal number of cores
    //
    // Solving this in an optimal way is a hard problem. The approximation
    // we use here is:
    // 1) discard all instances that do not have enough resources
    // 2) choose the cheapest instance
    def choose(memory: Option[Int], disk: Option[Int], cpu: Option[Int]) : String = {
        // step one: discard all instances that are too weak
        val sufficient: List[DxInstance] = instanceDB.filter(x => x.satisfies(memory, disk, cpu))
        if (sufficient.length == 0)
            throw new Exception(s"No instances found that match the requirements (memory=$memory, disk=$disk, cpu=$cpu")

        // step two: choose the cheapest instance
        val initialGuess = sufficient.head
        val bestInstance = sufficient.tail.foldLeft(initialGuess){ case (bestSoFar,x) =>
            if (x.price < bestSoFar.price) x else bestSoFar
        }
        bestInstance.name
    }

    def getMinimalInstanceType() : String = {
        "mem1_ssd1_x2"
    }

    // Currently, we support only constants.
    def apply(wdlMemory: Option[WdlValue],
               wdlDisk: Option[WdlValue],
               wdlCpu: Option[WdlValue]) : String = {
        // Examples for memory specification: "4000 MB", "1 GB"
        val memory: Option[Int] = wdlMemory match {
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
                    case "MB" | "M" => Math.ceil(i / 1024).toInt
                    case "GB" | "G" => i
                    case "TB" | "T" => i * 1024
                }
                Some(mem)
            case Some(x) =>
                throw new Exception(s"Memory has to evaluate to a WdlString type ${x.toWdlString}")
        }

        // Examples: "local-disk 1024 HDD"
        val disk: Option[Int] = wdlDisk match {
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

        choose(memory, disk, cpu)
    }
}

object InstanceTypeDB {
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
    def genHardcoded() : InstanceTypeDB = {
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
            val memory = intOfJs(traits("totalMemoryMB")) / 1024
            val disk = intOfJs(traits("ephemeralStorageGB"))
            val cpu = intOfJs(traits("numCores"))
            DxInstance(name, memory, disk, cpu, price)
        }.toList
        InstanceTypeDB(db)
    }

    // Query the platform for the available instance types in
    // this project.
    def queryAvailableInstanceTypes(dxProject: DXProject) : JsValue = {
        val req: ObjectNode = DXJSON.getObjectBuilder()
            .put("fields",
                 DXJSON.getObjectBuilder().put("availableInstanceTypes", true)
                     .build())
            .build()
        val rep = DXAPI.projectDescribe(dxProject.getId(), req, classOf[JsonNode])
        Utils.jsValueOfJsonNode(rep)
    }

    // Figure out the pricing model, by doing a project.describe
    def query(dxProject: DXProject) : InstanceTypeDB = {
        val available: JsValue = queryAvailableInstanceTypes(dxProject)
        System.err.println(s"${available.prettyPrint}")

        // get billTo from project
        // describe the billTo, and get the billing model

        throw new RuntimeException("query not fully implemented yet")
    }
}
