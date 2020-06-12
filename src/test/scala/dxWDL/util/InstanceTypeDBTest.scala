package dxWDL.util

import org.scalatest.{FlatSpec, Matchers}
import spray.json._
import wom.values._
import dxWDL.base._
import dxWDL.base.Utils
import dxWDL.dx.DxProject

class InstanceTypeDBTest extends FlatSpec with Matchers {

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

  // Create an availble instance list based on a hard coded list
  private def genTestDB(pricingInfo: Boolean): InstanceTypeDB = {
    def intOfJs(jsVal: JsValue): Int = {
      jsVal match {
        case JsNumber(x) => x.toInt
        case _           => throw new Exception("sanity")
      }
    }
    val awsOnDemandHourlyPriceTable: Map[String, Float] = {
      val fields: Map[String, JsValue] = awsOnDemandHourlyPrice.parseJson.asJsObject.fields
      fields.map {
        case (name, v) =>
          val price: Float = v match {
            case JsNumber(x) => x.toFloat
            case _           => throw new Exception("sanity")
          }
          name -> price
      }.toMap
    }

    val allInstances: Map[String, JsValue] = instanceList.parseJson.asJsObject.fields
    val db = allInstances.map {
      case (name, v) =>
        val fields: Map[String, JsValue] = v.asJsObject.fields
        val internalName = fields("internalName") match {
          case JsString(s) => s
          case _           => throw new Exception("sanity")
        }
        val price: Float =
          if (pricingInfo) awsOnDemandHourlyPriceTable(internalName)
          else 0
        val traits = fields("traits").asJsObject.fields
        val memoryMB = intOfJs(traits("totalMemoryMB"))
        val diskGB = intOfJs(traits("ephemeralStorageGB"))
        val cpu = intOfJs(traits("numCores"))
        DxInstanceType(name, memoryMB, diskGB, cpu, price, Vector.empty, false)
    }.toVector
    InstanceTypeDB(pricingInfo, db)
  }

  private def useDB(db: InstanceTypeDB): Unit = {
    db.chooseAttrs(None, None, None, None) should equal("mem1_ssd1_x2")
    db.chooseAttrs(Some(3 * 1024), Some(100), Some(5), None) should equal("mem1_ssd1_x8")
    db.chooseAttrs(Some(2 * 1024), Some(20), None, None) should equal("mem1_ssd1_x2")
    db.chooseAttrs(Some(30 * 1024), Some(128), Some(8), None) should equal("mem3_ssd1_x8")

    assertThrows[Exception] {
      // no instance with 1024 CPUs
      db.chooseAttrs(None, None, Some(1024), None)
    }

    db.apply(
        InstanceTypeDB.parse(None,
                             Some(WomString("3 GB")),
                             Some(WomString("local-disk 10 HDD")),
                             Some(WomString("1")),
                             None)
    ) should equal("mem1_ssd1_x2")
    db.apply(
        InstanceTypeDB.parse(None,
                             Some(WomString("37 GB")),
                             Some(WomString("local-disk 10 HDD")),
                             Some(WomString("6")),
                             None)
    ) should equal("mem3_ssd1_x8")
    db.apply(
        InstanceTypeDB
          .parse(None, Some(WomString("2 GB")), Some(WomString("local-disk 100 HDD")), None, None)
    ) should equal("mem1_ssd1_x8")
    db.apply(
        InstanceTypeDB
          .parse(None, Some(WomString("2.1GB")), Some(WomString("local-disk 100 HDD")), None, None)
    ) should equal("mem1_ssd1_x8")

    db.apply(InstanceTypeDB.parse(Some(WomString("mem3_ssd1_x8")), None, None, None, None)) should equal(
        "mem3_ssd1_x8"
    )

    db.apply(
        InstanceTypeDB.parse(None,
                             Some(WomString("235 GB")),
                             Some(WomString("local-disk 550 HDD")),
                             Some(WomString("32")),
                             None)
    ) should equal("mem3_ssd1_x32")
    db.apply(InstanceTypeDB.parse(Some(WomString("mem3_ssd1_x32")), None, None, None, None)) should equal(
        "mem3_ssd1_x32"
    )

    db.apply(InstanceTypeDB.parse(None, None, None, Some(WomString("8")), None)) should equal(
        "mem1_ssd1_x8"
    )
  }

  val dbFull = genTestDB(true)
  val dbOpaque = InstanceTypeDB.opaquePrices(dbFull)
  val dbNoPrices = genTestDB(false)

  it should "Work even without access to pricing information" in {
    // parameters are:          RAM,     disk,     cores
    dbNoPrices.chooseAttrs(None, None, None, None) should equal("mem1_ssd1_x2")

    dbNoPrices.chooseAttrs(Some(1000), None, Some(3), None) should equal("mem1_ssd1_x4")
  }

  it should "Choose reasonable platform instance types" in {
    useDB(dbFull)
  }

  it should "perform JSON serialization" in {
    val js = dbFull.toJson
    val db2 = js.asJsObject.convertTo[InstanceTypeDB]
    dbFull should equal(db2)
  }

  it should "pretty print" in {
    // Test pretty printing
    Utils.ignore(dbFull.prettyPrint)
  }

  it should "work even with opaque prices" in {
    useDB(dbOpaque)
  }

  it should "compare two instance types" in {
    dbFull.lteqByResources("mem1_ssd1_x2", "mem1_ssd1_x8") should be(true)
    dbFull.lteqByResources("mem1_ssd1_x4", "mem3_ssd1_x2") should be(false)

    // non existant instance
    dbFull.lteqByResources("mem1_ssd2_x2", "ggxx") should be(false)
  }

  it should "catch parsing errors" in {
    assertThrows[Exception] {
      // illegal request format
      InstanceTypeDB.parse(Some(WomInteger(4)), None, None, None, None)
    }

    // memory specification
    InstanceTypeDB.parse(None, Some(WomString("230MB")), None, None, None) shouldBe
      InstanceTypeReq(None, Some((230 * 1000 * 1000) / (1024 * 1024).toInt), None, None, None)

    InstanceTypeDB.parse(None, Some(WomString("230MiB")), None, None, None) shouldBe
      InstanceTypeReq(None, Some(230), None, None, None)

    InstanceTypeDB.parse(None, Some(WomString("230GB")), None, None, None) shouldBe
      InstanceTypeReq(None,
                      Some(((230d * 1000d * 1000d * 1000d) / (1024d * 1024d)).toInt),
                      None,
                      None,
                      None)

    InstanceTypeDB.parse(None, Some(WomString("230GiB")), None, None, None) shouldBe
      InstanceTypeReq(None, Some(230 * 1024), None, None, None)

    InstanceTypeDB.parse(None, Some(WomString("1000 TB")), None, None, None) shouldBe
      InstanceTypeReq(None,
                      Some(((1000d * 1000d * 1000d * 1000d * 1000d) / (1024d * 1024d)).toInt),
                      None,
                      None,
                      None)

    InstanceTypeDB.parse(None, Some(WomString("1000 TiB")), None, None, None) shouldBe
      InstanceTypeReq(None, Some(1000 * 1024 * 1024), None, None, None)

    assertThrows[Exception] {
      InstanceTypeDB.parse(None, Some(WomString("230 44 34 GB")), None, None, None)
    }
    assertThrows[Exception] {
      InstanceTypeDB.parse(None, Some(WomString("230.x GB")), None, None, None)
    }
    assertThrows[Exception] {
      InstanceTypeDB.parse(None, Some(WomString("230.x GB")), None, None, None)
    }
    assertThrows[Exception] {
      InstanceTypeDB.parse(None, Some(WomFloat(230.3)), None, None, None)
    }
    assertThrows[Exception] {
      InstanceTypeDB.parse(None, Some(WomString("230 XXB")), None, None, None)
    }

    // disk spec
    assertThrows[Exception] {
      InstanceTypeDB.parse(None, None, Some(WomString("just give me a disk")), None, None)
    }
    assertThrows[Exception] {
      InstanceTypeDB.parse(None, None, Some(WomString("local-disk xxxx")), None, None)
    }
    assertThrows[Exception] {
      InstanceTypeDB.parse(None, None, Some(WomInteger(1024)), None, None)
    }

    // cpu
    assertThrows[Exception] {
      InstanceTypeDB.parse(None, None, None, Some(WomString("xxyy")), None)
    }
    InstanceTypeDB.parse(None, None, None, Some(WomInteger(1)), None) shouldBe InstanceTypeReq(
        None,
        None,
        None,
        Some(1),
        None
    )
    InstanceTypeDB.parse(None, None, None, Some(WomFloat(1.2)), None) shouldBe InstanceTypeReq(
        None,
        None,
        None,
        Some(1),
        None
    )

    assertThrows[Exception] {
      InstanceTypeDB.parse(None, None, None, Some(WomBoolean(false)), None)
    }

    // gpu
    InstanceTypeDB
      .parse(None, Some(WomString("1000 TiB")), None, None, Some(WomBoolean(true))) shouldBe
      InstanceTypeReq(None, Some(1000 * 1024 * 1024), None, None, Some(true))

    InstanceTypeDB.parse(None, None, None, None, Some(WomBoolean(false))) shouldBe
      InstanceTypeReq(None, None, None, None, Some(false))
  }

  it should "work on large instances (JIRA-1258)" in {
    val db = InstanceTypeDB(
        true,
        Vector(
            DxInstanceType(
                "mem3_ssd1_x32",
                245751,
                32,
                597,
                13.0.toFloat,
                Vector(("Ubuntu", "16.04")),
                false
            ),
            DxInstanceType(
                "mem4_ssd1_x128",
                1967522,
                128,
                3573,
                14.0.toFloat,
                Vector(("Ubuntu", "16.04")),
                false
            )
        )
    )

    db.chooseAttrs(Some(239 * 1024), Some(18), Some(32), None) should equal("mem3_ssd1_x32")
    db.chooseAttrs(Some(240 * 1024), Some(18), Some(32), None) should equal("mem4_ssd1_x128")
  }

  it should "prefer v2 instances over v1's" in {
    val db = InstanceTypeDB(
        true,
        Vector(
            DxInstanceType("mem1_ssd1_v2_x4",
                           8000,
                           80,
                           4,
                           0.2.toFloat,
                           Vector(("Ubuntu", "16.04")),
                           false),
            DxInstanceType("mem1_ssd1_x4",
                           8000,
                           80,
                           4,
                           0.2.toFloat,
                           Vector(("Ubuntu", "16.04")),
                           false)
        )
    )

    db.chooseAttrs(None, None, Some(4), None) should equal("mem1_ssd1_v2_x4")
  }

  it should "respect requests for GPU instances" taggedAs (EdgeTest) in {
    val db = InstanceTypeDB(
        true,
        Vector(
            DxInstanceType("mem1_ssd1_v2_x4",
                           8000,
                           80,
                           4,
                           0.2.toFloat,
                           Vector(("Ubuntu", "16.04")),
                           false),
            DxInstanceType("mem1_ssd1_x4",
                           8000,
                           80,
                           4,
                           0.2.toFloat,
                           Vector(("Ubuntu", "16.04")),
                           false),
            DxInstanceType("mem3_ssd1_gpu_x8",
                           30000,
                           100,
                           8,
                           1.0.toFloat,
                           Vector(("Ubuntu", "16.04")),
                           true)
        )
    )

    db.chooseAttrs(None, None, Some(4), Some(true)) should equal("mem3_ssd1_gpu_x8")

    assertThrows[Exception] {
      // No non-GPU instance has 8 cpus
      db.chooseAttrs(None, None, Some(8), Some(false))
    }
  }
  // FIXME: This test will not pass on CI/CD as we are using scoped-token.
  ignore should "Query returns correct pricing models for org and user" in {
    val userBilltoProject = DxProject("project-FqP0vf00bxKykykX5pVXB1YQ") // project name: dxWDL_public_test
    val orgBilltoProject = DxProject("project-FQ7BqkQ0FyXgJxGP2Bpfv3vK") // project name: dxWDL_CI

    val userResult = InstanceTypeDB.query(userBilltoProject, Verbose(on = false, quiet = true, null))
    val orgResult = InstanceTypeDB.query(orgBilltoProject, Verbose(on = false, quiet = true, null))

    userResult.pricingAvailable shouldBe true
    orgResult.pricingAvailable shouldBe true
  }
}
