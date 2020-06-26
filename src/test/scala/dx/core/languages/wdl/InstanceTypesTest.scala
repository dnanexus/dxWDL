package dx.core.languages.wdl

import dx.api.{DxApi, DxInstanceType, InstanceTypeDB, InstanceTypeDbQuery, InstanceTypeReq}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import spray.json._
import wdlTools.eval.WdlValues.{V_Boolean, V_Float, V_Int, V_String}
import wdlTools.util.Logger

class InstanceTypesTest extends AnyFlatSpec with Matchers {
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
      }
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
        DxInstanceType(name, memoryMB, diskGB, cpu, price, Vector.empty, gpu = false)
    }.toVector
    InstanceTypeDB(pricingInfo, db)
  }

  private val dbFull = genTestDB(true)
  private val dxApi: DxApi = DxApi(Logger.Quiet)
  private val dbOpaque = InstanceTypeDbQuery(dxApi).opaquePrices(dbFull)

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
        InstanceTypes.parse(None,
                            Some(V_String("3 GB")),
                            Some(V_String("local-disk 10 HDD")),
                            Some(V_String("1")),
                            None)
    ) should equal("mem1_ssd1_x2")
    db.apply(
        InstanceTypes.parse(None,
                            Some(V_String("37 GB")),
                            Some(V_String("local-disk 10 HDD")),
                            Some(V_String("6")),
                            None)
    ) should equal("mem3_ssd1_x8")
    db.apply(
        InstanceTypes
          .parse(None, Some(V_String("2 GB")), Some(V_String("local-disk 100 HDD")), None, None)
    ) should equal("mem1_ssd1_x8")
    db.apply(
        InstanceTypes
          .parse(None, Some(V_String("2.1GB")), Some(V_String("local-disk 100 HDD")), None, None)
    ) should equal("mem1_ssd1_x8")

    db.apply(InstanceTypes.parse(Some(V_String("mem3_ssd1_x8")), None, None, None, None)) should equal(
        "mem3_ssd1_x8"
    )

    db.apply(
        InstanceTypes.parse(None,
                            Some(V_String("235 GB")),
                            Some(V_String("local-disk 550 HDD")),
                            Some(V_String("32")),
                            None)
    ) should equal("mem3_ssd1_x32")
    db.apply(InstanceTypes.parse(Some(V_String("mem3_ssd1_x32")), None, None, None, None)) should equal(
        "mem3_ssd1_x32"
    )

    db.apply(InstanceTypes.parse(None, None, None, Some(V_String("8")), None)) should equal(
        "mem1_ssd1_x8"
    )
  }

  it should "Choose reasonable platform instance types" in {
    useDB(dbFull)
  }

  it should "work even with opaque prices" in {
    useDB(dbOpaque)
  }

  it should "catch parsing errors" in {
    assertThrows[Exception] {
      // illegal request format
      InstanceTypes.parse(Some(V_Int(4)), None, None, None, None)
    }

    // memory specification
    InstanceTypes.parse(None, Some(V_String("230MB")), None, None, None) shouldBe
      InstanceTypeReq(None, Some((230 * 1000 * 1000) / (1024 * 1024).toInt), None, None, None)

    InstanceTypes.parse(None, Some(V_String("230MiB")), None, None, None) shouldBe
      InstanceTypeReq(None, Some(230), None, None, None)

    InstanceTypes.parse(None, Some(V_String("230GB")), None, None, None) shouldBe
      InstanceTypeReq(None,
                      Some(((230d * 1000d * 1000d * 1000d) / (1024d * 1024d)).toInt),
                      None,
                      None,
                      None)

    InstanceTypes.parse(None, Some(V_String("230GiB")), None, None, None) shouldBe
      InstanceTypeReq(None, Some(230 * 1024), None, None, None)

    InstanceTypes.parse(None, Some(V_String("1000 TB")), None, None, None) shouldBe
      InstanceTypeReq(None,
                      Some(((1000d * 1000d * 1000d * 1000d * 1000d) / (1024d * 1024d)).toInt),
                      None,
                      None,
                      None)

    InstanceTypes.parse(None, Some(V_String("1000 TiB")), None, None, None) shouldBe
      InstanceTypeReq(None, Some(1000 * 1024 * 1024), None, None, None)

    assertThrows[Exception] {
      InstanceTypes.parse(None, Some(V_String("230 44 34 GB")), None, None, None)
    }
    assertThrows[Exception] {
      InstanceTypes.parse(None, Some(V_String("230.x GB")), None, None, None)
    }
    assertThrows[Exception] {
      InstanceTypes.parse(None, Some(V_String("230.x GB")), None, None, None)
    }
    assertThrows[Exception] {
      InstanceTypes.parse(None, Some(V_Float(230.3)), None, None, None)
    }
    assertThrows[Exception] {
      InstanceTypes.parse(None, Some(V_String("230 XXB")), None, None, None)
    }

    // disk spec
    assertThrows[Exception] {
      InstanceTypes.parse(None, None, Some(V_String("just give me a disk")), None, None)
    }
    assertThrows[Exception] {
      InstanceTypes.parse(None, None, Some(V_String("local-disk xxxx")), None, None)
    }
    assertThrows[Exception] {
      InstanceTypes.parse(None, None, Some(V_Int(1024)), None, None)
    }

    // cpu
    assertThrows[Exception] {
      InstanceTypes.parse(None, None, None, Some(V_String("xxyy")), None)
    }
    InstanceTypes.parse(None, None, None, Some(V_Int(1)), None) shouldBe InstanceTypeReq(
        None,
        None,
        None,
        Some(1),
        None
    )
    InstanceTypes.parse(None, None, None, Some(V_Float(1.2)), None) shouldBe InstanceTypeReq(
        None,
        None,
        None,
        Some(1),
        None
    )

    assertThrows[Exception] {
      InstanceTypes.parse(None, None, None, Some(V_Boolean(false)), None)
    }

    // gpu
    InstanceTypes
      .parse(None, Some(V_String("1000 TiB")), None, None, Some(V_Boolean(true))) shouldBe
      InstanceTypeReq(None, Some(1000 * 1024 * 1024), None, None, Some(true))

    InstanceTypes.parse(None, None, None, None, Some(V_Boolean(false))) shouldBe
      InstanceTypeReq(None, None, None, None, Some(false))
  }
}
