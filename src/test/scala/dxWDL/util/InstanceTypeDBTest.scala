package dxWDL.util

import org.scalatest.{FlatSpec, Matchers}
import spray.json._
import wom.values._

import dxWDL.base._
import dxWDL.base.Utils

class InstanceTypeDBTest extends FlatSpec with Matchers {

    val dbFull = InstanceTypeDB.genTestDB(true)
    val dbOpaque = InstanceTypeDB.opaquePrices(dbFull)
    val dbNoPrices = InstanceTypeDB.genTestDB(false)

    it should "Work even without access to pricing information" in {
        // parameters are:          RAM,     disk,     cores
        dbNoPrices.choose3Attr(None, None, None) should equal("mem1_ssd1_x2")
    }

    private def useDB(db: InstanceTypeDB) : Unit = {
        db.choose3Attr(None, None, None) should equal("mem1_ssd1_x2")
        db.choose3Attr(Some(3*1024), Some(100), Some(5)) should equal("mem1_ssd1_x8")
        db.choose3Attr(Some(2*1024), Some(20), None) should equal("mem1_ssd1_x2")
        db.choose3Attr(Some(30*1024), Some(128), Some(8)) should equal("mem3_ssd1_x8")

        assertThrows[Exception] {
            // no instance with 1024 CPUs
            db.choose3Attr(None, None, Some(1024))
        }


        db.defaultInstanceType should equal(InstanceTypeDB.DEFAULT_INSTANCE_TYPE)

        db.apply(InstanceTypeDB.parse(None,
                                      Some(WomString("3 GB")),
                                      Some(WomString("local-disk 10 HDD")),
                                      Some(WomString("1")))) should equal("mem1_ssd1_x2")
        db.apply(InstanceTypeDB.parse(None,
                                      Some(WomString("37 GB")),
                                      Some(WomString("local-disk 10 HDD")),
                                      Some(WomString("6")))) should equal("mem3_ssd1_x8")
        db.apply(InstanceTypeDB.parse(None,
                                      Some(WomString("2 GB")),
                                      Some(WomString("local-disk 100 HDD")),
                                      None)) should equal("mem1_ssd1_x8")
        db.apply(InstanceTypeDB.parse(None,
                                      Some(WomString("2.1GB")),
                                      Some(WomString("local-disk 100 HDD")),
                                      None)) should equal("mem1_ssd1_x8")

        db.apply(InstanceTypeDB.parse(Some(WomString("mem3_ssd1_x8")),
                                      None,
                                      None,
                                      None)) should equal("mem3_ssd1_x8")

        db.apply(InstanceTypeDB.parse(None,
                                      Some(WomString("235 GB")),
                                      Some(WomString("local-disk 550 HDD")),
                                      Some(WomString("32")))) should equal("mem3_ssd1_x32")
        db.apply(InstanceTypeDB.parse(Some(WomString("mem3_ssd1_x32")),
                                      None,
                                      None,
                                      None)) should equal("mem3_ssd1_x32")

        db.apply(InstanceTypeDB.parse(None,
                                      None,
                                      None,
                                      Some(WomString("8")))) should equal("mem1_ssd1_x8")
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
        dbFull.lteqByResources("mem1_ssd1_x2", "mem1_ssd1_x8") should be (true)
        dbFull.lteqByResources("mem1_ssd1_x4", "mem3_ssd1_x2") should be (false)

        // non existant instance
        dbFull.lteqByResources("mem1_ssd2_x2", "ggxx") should be (false)
    }

    it should "catch parsing errors" in {
        assertThrows[Exception] {
            // illegal request format
            InstanceTypeDB.parse(Some(WomInteger(4)), None, None, None)
        }

        // memory specification
        InstanceTypeDB.parse(None, Some(WomString("230GB")), None, None) shouldBe InstanceTypeReq(None, Some(230 * 1024), None, None)

        InstanceTypeDB.parse(None, Some(WomString("1000 TB")), None, None) shouldBe InstanceTypeReq(None, Some(1000 * 1024 * 1024), None, None)

        assertThrows[Exception] {
            InstanceTypeDB.parse(None, Some(WomString("230 44 34 GB")), None, None)
        }
        assertThrows[Exception] {
            InstanceTypeDB.parse(None, Some(WomString("230.x GB")), None, None)
        }
        assertThrows[Exception] {
            InstanceTypeDB.parse(None, Some(WomString("230.x GB")), None, None)
        }
        assertThrows[Exception] {
            InstanceTypeDB.parse(None, Some(WomFloat(230.3)), None, None)
        }
        assertThrows[Exception] {
            InstanceTypeDB.parse(None, Some(WomString("230 XXB")), None, None)
        }

        // disk spec
        assertThrows[Exception] {
            InstanceTypeDB.parse(None, None, Some(WomString("just give me a disk")), None)
        }
        assertThrows[Exception] {
            InstanceTypeDB.parse(None, None, Some(WomString("local-disk xxxx")), None)
        }
        assertThrows[Exception] {
            InstanceTypeDB.parse(None, None, Some(WomInteger(1024)), None)
        }

        // cpu
        assertThrows[Exception] {
            InstanceTypeDB.parse(None, None, None, Some(WomString("xxyy")))
        }
        InstanceTypeDB.parse(None, None, None, Some(WomInteger(1))) shouldBe InstanceTypeReq(None, None, None, Some(1))
        InstanceTypeDB.parse(None, None, None, Some(WomFloat(1.2))) shouldBe InstanceTypeReq(None, None, None, Some(1))

        assertThrows[Exception] {
            InstanceTypeDB.parse(None, None, None, Some(WomBoolean(false)))
        }
    }

    it should "work on large instances (JIRA-1258)" in {
        val db = InstanceTypeDB(
            Vector(
                DxInstanceType(
                    "mem3_ssd1_x32",
                    245751,
                    32,
                    597,
                    13.0.toFloat,
                    Vector(("Ubuntu", "16.04"))
                ),
                DxInstanceType(
                    "mem4_ssd1_x128",
                    1967522,
                    128,
                    3573,
                    14.0.toFloat,
                    Vector(("Ubuntu", "16.04"))
                )
            )
        )

        db.choose3Attr(Some(239 * 1024), Some(18), Some(32)) should equal("mem3_ssd1_x32")
        db.choose3Attr(Some(240 * 1024), Some(18), Some(32)) should equal("mem4_ssd1_x128")
    }
}
