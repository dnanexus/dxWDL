package dxWDL

import org.scalatest.{BeforeAndAfterEach, FlatSpec, OneInstancePerTest}
import wdl4s.types._
import wdl4s.values._

class InstaceTypeTest extends FlatSpec with BeforeAndAfterEach with OneInstancePerTest {

    "InstanceTypeDB" should "Choose reasonable platform instance types" in {
        val db = InstanceTypeDB.genHardcoded()

        assert(db.choose(None, None, None) == "mem1_ssd1_x2")

        // parameters are:          RAM,     disk,     cores
        assert(db.choose(Some(3*1024), Some(100), Some(4)) == "mem1_ssd1_x8")
        assert(db.choose(Some(2*1024), Some(20), None) == "mem1_ssd1_x2")
        assert(db.choose(Some(30*1024), Some(128), Some(8)) == "mem3_ssd1_x8")

        assert(db.apply(Some(WdlString("3 GB")),
                                   Some(WdlString("local-disk 10 HDD")),
                                   Some(WdlString("1"))) == "mem1_ssd1_x2")
        assert(db.apply(Some(WdlString("37 GB")),
                                   Some(WdlString("local-disk 10 HDD")),
                                   Some(WdlString("6"))) == "mem3_ssd1_x8")
        assert(db.apply(Some(WdlString("2 GB")),
                                   Some(WdlString("local-disk 100 HDD")),
                                   None) == "mem1_ssd1_x8")
    }
}
