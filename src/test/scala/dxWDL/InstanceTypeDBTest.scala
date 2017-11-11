package dxWDL

import org.scalatest.{BeforeAndAfterEach, FlatSpec, OneInstancePerTest}
import wdl4s.wdl.values._

class InstaceTypeDBTest extends FlatSpec with BeforeAndAfterEach with OneInstancePerTest {

    it should "Work even without access to pricing information" in {
        val db = InstanceTypeDB.genTestDB(false)

        // parameters are:          RAM,     disk,     cores
        assert(db.choose(None, None, None, None) == "mem1_ssd1_x2")
    }

    it should "Choose reasonable platform instance types" in {
        // parameters are:          RAM,     disk,     cores
        val db = InstanceTypeDB.genTestDB(true)
        assert(db.choose(None, None, None, None) == "mem1_ssd1_x2")
        assert(db.choose(None, Some(3*1024), Some(100), Some(5)) == "mem1_ssd1_x8")
        assert(db.choose(None, Some(2*1024), Some(20), None) == "mem1_ssd1_x2")
        assert(db.choose(None, Some(30*1024), Some(128), Some(8)) == "mem3_ssd1_x8")

        assert(db.apply(None,
                        Some(WdlString("3 GB")),
                        Some(WdlString("local-disk 10 HDD")),
                        Some(WdlString("1"))) == "mem1_ssd1_x2")
        assert(db.apply(None,
                        Some(WdlString("37 GB")),
                        Some(WdlString("local-disk 10 HDD")),
                        Some(WdlString("6"))) == "mem3_ssd1_x8")
        assert(db.apply(None,
                        Some(WdlString("2 GB")),
                        Some(WdlString("local-disk 100 HDD")),
                        None) == "mem1_ssd1_x8")
        assert(db.apply(None,
                        Some(WdlString("2.1GB")),
                        Some(WdlString("local-disk 100 HDD")),
                        None) == "mem1_ssd1_x8")

        assert(db.apply(Some(WdlString("mem3_ssd1_x8")), None, None, None) == "mem3_ssd1_x8")

        assert(db.apply(None,
                        Some(WdlString("235 GB")),
                        Some(WdlString("local-disk 550 HDD")),
                        Some(WdlString("32"))) == "mem3_ssd1_x32")
        assert(db.apply(Some(WdlString("mem3_ssd1_x32")), None, None, None) == "mem3_ssd1_x32")
    }
}
