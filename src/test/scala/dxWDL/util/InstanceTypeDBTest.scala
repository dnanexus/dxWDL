package dxWDL.util

import org.scalatest.{FlatSpec, Matchers}
import wom.values._

class InstanceTypeDBTest extends FlatSpec with Matchers {

    it should "Work even without access to pricing information" in {
        val db = InstanceTypeDB.genTestDB(false)

        // parameters are:          RAM,     disk,     cores
        db.choose3Attr(None, None, None) should equal("mem1_ssd1_x2")
    }

    private def useDB(db: InstanceTypeDB) : Unit = {
        db.choose3Attr(None, None, None) should equal("mem1_ssd1_x2")
        db.choose3Attr(Some(3*1024), Some(100), Some(5)) should equal("mem1_ssd1_x8")
        db.choose3Attr(Some(2*1024), Some(20), None) should equal("mem1_ssd1_x2")
        db.choose3Attr(Some(30*1024), Some(128), Some(8)) should equal("mem3_ssd1_x8")

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
    }

    val db = InstanceTypeDB.genTestDB(true)

    it should "Choose reasonable platform instance types" in {
        // parameters are:          RAM,     disk,     cores
        useDB(db)
    }

    it should "work even with opaque prices" in {
        val dbOpaque = InstanceTypeDB.opaquePrices(db)
        useDB(dbOpaque)
    }

    it should "compare two instance types" in {
        db.lteqByResources("mem1_ssd1_x2", "mem1_ssd1_x8") should be (true)
        db.lteqByResources("mem1_ssd1_x4", "mem3_ssd1_x2") should be (false)

        // non existant instance
        db.lteqByResources("mem1_ssd2_x2", "ggxx") should be (false)
    }
}
