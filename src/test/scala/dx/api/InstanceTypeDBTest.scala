package dx.api

import dx.Tags.{ApiTest, EdgeTest}
import dx.api.InstanceTypeDB.instanceTypeDBFormat
import dx.core.Constants
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import spray.json._
import wdlTools.util.Logger

class InstanceTypeDBTest extends AnyFlatSpec with Matchers {

  // The original list is at:
  // https://github.com/dnanexus/nucleus/blob/master/commons/instance_types/aws_instance_types.json
  //
  // The g2,i2,x1 instances have been removed, because they are not
  // enabled for customers by default.  In addition, the PV (Paravirtual)
  // instances have been removed, because they work only on Ubuntu
  // 12.04.
  //
  // Removed the ssd2 instances, because they actually use EBS storage. A better
  // solution would be asking the platform for the available instances.
  private val instanceList: String =
    """{
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

  private val ExecutionEnvironments = Vector(
      ExecutionEnvironment(Constants.OsDistribution,
                           Constants.OsRelease,
                           Vector(Constants.OsVersion))
  )

  // Create an availble instance list based on a hard coded list
  private def genTestDB(pricingInfo: Boolean): InstanceTypeDB = {
    def intOfJs(jsVal: JsValue): Int = {
      jsVal match {
        case JsNumber(x) => x.toInt
        case _           => throw new Exception("unexpected")
      }
    }
    val awsOnDemandHourlyPriceTable: Map[String, Float] = {
      val fields: Map[String, JsValue] = awsOnDemandHourlyPrice.parseJson.asJsObject.fields
      fields.map {
        case (name, v) =>
          val price: Float = v match {
            case JsNumber(x) => x.toFloat
            case _           => throw new Exception("unexpected")
          }
          name -> price
      }
    }

    val allInstances: Map[String, JsValue] = instanceList.parseJson.asJsObject.fields
    val db = allInstances.map {
      case (name, v) =>
        val fields: Map[String, JsValue] = v.asJsObject.fields
        val internalName = fields("internalName") match {
          case JsString(s) => s
          case _           => throw new Exception("unexpected")
        }
        val price: Option[Float] =
          if (pricingInfo) Some(awsOnDemandHourlyPriceTable(internalName))
          else None
        val traits = fields("traits").asJsObject.fields
        val memoryMB = intOfJs(traits("totalMemoryMB"))
        val diskGB = intOfJs(traits("ephemeralStorageGB"))
        val cpu = intOfJs(traits("numCores"))
        name -> DxInstanceType(name, memoryMB, diskGB, cpu, gpu = false, Vector.empty, None, price)
    }
    InstanceTypeDB(db, pricingInfo)
  }

  private val dxApi: DxApi = DxApi(Logger.Quiet)
  private val dbFull = genTestDB(true)
  private val dbNoPrices = genTestDB(false)

  it should "compare two instance types" in {
    // instances where all lhs resources are less than all rhs resources
    val c1 = dbFull.compareByResources("mem1_ssd1_x2", "mem1_ssd1_x8").get
    c1 should be < 0
    // instances where some resources are less and some are greater
    dbFull.compareByResources("mem1_ssd1_x4", "mem3_ssd1_x2") shouldBe None
    // non existant instance
    assertThrows[Exception] {
      dbFull.compareByResources("mem1_ssd2_x2", "ggxx") shouldBe 0
    }
  }

  it should "Work even without access to pricing information" in {
    // parameters are:          RAM,     disk,     cores
    dbNoPrices.selectOptimal(InstanceTypeRequest.empty) should matchPattern {
      case Some(instanceType: DxInstanceType) if instanceType.name == "mem1_ssd1_x2" =>
    }
    dbNoPrices
      .selectOptimal(InstanceTypeRequest(memoryMB = Some(1000), cpu = Some(3))) should matchPattern {
      case Some(instanceType: DxInstanceType) if instanceType.name == "mem1_ssd1_x4" =>
    }
  }

  it should "perform JSON serialization" in {
    val js = dbFull.toJson
    val db2 = js.asJsObject.convertTo[InstanceTypeDB]
    dbFull should equal(db2)
  }

  it should "pretty print" in {
    // Test pretty printing
    Logger.get.ignore(dbFull.prettyFormat())
  }

  it should "work on large instances (JIRA-1258)" in {
    val db = InstanceTypeDB(
        Map(
            "mem3_ssd1_x32" -> DxInstanceType(
                "mem3_ssd1_x32",
                245751,
                32,
                597,
                gpu = false,
                ExecutionEnvironments,
                Some(DiskType.SSD),
                Some(13.0.toFloat)
            ),
            "mem4_ssd1_x128" -> DxInstanceType(
                "mem4_ssd1_x128",
                1967522,
                128,
                3573,
                gpu = false,
                ExecutionEnvironments,
                Some(DiskType.SSD),
                Some(14.0.toFloat)
            )
        ),
        pricingAvailable = true
    )

    db.selectOptimal(
        InstanceTypeRequest(memoryMB = Some(239 * 1024), diskGB = Some(18), cpu = Some(32))
    ) should matchPattern {
      case Some(instanceType: DxInstanceType) if instanceType.name == "mem3_ssd1_x32" =>
    }
    db.selectOptimal(
        InstanceTypeRequest(memoryMB = Some(240 * 1024), diskGB = Some(18), cpu = Some(32))
    ) should matchPattern {
      case Some(instanceType: DxInstanceType) if instanceType.name == "mem4_ssd1_x128" =>
    }
  }

  it should "prefer v2 instances over v1's" in {
    val db = InstanceTypeDB(
        Map(
            "mem1_ssd1_v2_x4" -> DxInstanceType(
                "mem1_ssd1_v2_x4",
                8000,
                80,
                4,
                gpu = false,
                ExecutionEnvironments,
                Some(DiskType.SSD),
                Some(0.2.toFloat)
            ),
            "mem1_ssd1_x4" -> DxInstanceType(
                "mem1_ssd1_x4",
                8000,
                80,
                4,
                gpu = false,
                ExecutionEnvironments,
                Some(DiskType.SSD),
                Some(0.2.toFloat)
            )
        ),
        pricingAvailable = true
    )

    db.selectOptimal(InstanceTypeRequest(cpu = Some(4))) should matchPattern {
      case Some(instanceType: DxInstanceType) if instanceType.name == "mem1_ssd1_v2_x4" =>
    }
  }

  it should "respect requests for GPU instances" taggedAs EdgeTest in {
    val db = InstanceTypeDB(
        Map(
            "mem1_ssd1_v2_x4" -> DxInstanceType(
                "mem1_ssd1_v2_x4",
                8000,
                80,
                4,
                gpu = false,
                ExecutionEnvironments,
                Some(DiskType.SSD),
                Some(0.2.toFloat)
            ),
            "mem1_ssd1_x4" -> DxInstanceType(
                "mem1_ssd1_x4",
                8000,
                80,
                4,
                gpu = false,
                ExecutionEnvironments,
                Some(DiskType.SSD),
                Some(0.2.toFloat)
            ),
            "mem3_ssd1_gpu_x8" -> DxInstanceType(
                "mem3_ssd1_gpu_x8",
                30000,
                100,
                8,
                gpu = true,
                ExecutionEnvironments,
                Some(DiskType.SSD),
                Some(1.0.toFloat)
            )
        ),
        pricingAvailable = true
    )

    db.selectOptimal(InstanceTypeRequest(cpu = Some(4), gpu = Some(true))) should matchPattern {
      case Some(instanceType: DxInstanceType) if instanceType.name == "mem3_ssd1_gpu_x8" =>
    }

    // No non-GPU instance has 8 cpus
    db.selectOptimal(InstanceTypeRequest(cpu = Some(8), gpu = Some(false))) shouldBe None
  }

  // FIXME: This test will not pass on CI/CD as we are using scoped-token.
  ignore should "Query returns correct pricing models for org and user" taggedAs ApiTest in {
    // Instance type filter:
    // - Instance must support Ubuntu.
    // - Instance is not an FPGA instance.
    // - Instance does not have local HDD storage (those are older instance types).
    def instanceTypeFilter(instanceType: DxInstanceType): Boolean = {
      instanceType.os.exists(_.release == Constants.OsRelease) &&
      !instanceType.diskType.contains(DiskType.HDD) &&
      !instanceType.name.contains("fpga")
    }

    val userBilltoProject = dxApi.project("project-FqP0vf00bxKykykX5pVXB1YQ") // project name: dxWDL_public_test
    val userResult = InstanceTypeDB.create(userBilltoProject, instanceTypeFilter)
    userResult.pricingAvailable shouldBe true

    val orgBilltoProject = dxApi.project("project-FQ7BqkQ0FyXgJxGP2Bpfv3vK") // project name: dxWDL_CI
    val orgResult = InstanceTypeDB.create(orgBilltoProject, instanceTypeFilter)
    orgResult.pricingAvailable shouldBe true
  }
}
