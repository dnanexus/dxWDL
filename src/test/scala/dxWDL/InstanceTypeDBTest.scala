package dxWDL

import org.scalatest.{FlatSpec, Matchers}
import wdl4s.wdl.values._

class InstaceTypeDBTest extends FlatSpec with Matchers {

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
                                      Some(WdlString("3 GB")),
                                      Some(WdlString("local-disk 10 HDD")),
                                      Some(WdlString("1")))) should equal("mem1_ssd1_x2")
        db.apply(InstanceTypeDB.parse(None,
                                      Some(WdlString("37 GB")),
                                      Some(WdlString("local-disk 10 HDD")),
                                      Some(WdlString("6")))) should equal("mem3_ssd1_x8")
        db.apply(InstanceTypeDB.parse(None,
                                      Some(WdlString("2 GB")),
                                      Some(WdlString("local-disk 100 HDD")),
                                      None)) should equal("mem1_ssd1_x8")
        db.apply(InstanceTypeDB.parse(None,
                                      Some(WdlString("2.1GB")),
                                      Some(WdlString("local-disk 100 HDD")),
                                      None)) should equal("mem1_ssd1_x8")

        db.apply(InstanceTypeDB.parse(Some(WdlString("mem3_ssd1_x8")),
                                      None,
                                      None,
                                      None)) should equal("mem3_ssd1_x8")

        db.apply(InstanceTypeDB.parse(None,
                                      Some(WdlString("235 GB")),
                                      Some(WdlString("local-disk 550 HDD")),
                                      Some(WdlString("32")))) should equal("mem3_ssd1_x32")
        db.apply(InstanceTypeDB.parse(Some(WdlString("mem3_ssd1_x32")),
                                      None,
                                      None,
                                      None)) should equal("mem3_ssd1_x32")
    }

    it should "Choose reasonable platform instance types" in {
        // parameters are:          RAM,     disk,     cores
        val db = InstanceTypeDB.genTestDB(true)
        useDB(db)
    }

    it should "Work even with opaque prices" in {
        val db = InstanceTypeDB.genTestDB(true)
        val dbOpaque = InstanceTypeDB.opaquePrices(db)
        useDB(dbOpaque)
    }
}
