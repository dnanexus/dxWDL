package dxWDL

import org.scalatest.{BeforeAndAfterEach, FlatSpec, OneInstancePerTest}
import wdl4s.types._
import wdl4s.values._

class InstaceTypesTest extends FlatSpec with BeforeAndAfterEach with OneInstancePerTest {

    "InstanceTypes" should "Choose reasonable platform instance types" in {
        assert(InstanceTypes.choose(None, None, None) == "mem1_ssd1_x2")

        // parameters are:          RAM,     disk,     cores
        assert(InstanceTypes.choose(Some(3), Some(100), Some(4)) == "mem1_ssd1_x8")
        assert(InstanceTypes.choose(Some(2), Some(20), None) == "mem1_ssd1_x2")
        assert(InstanceTypes.choose(Some(30), Some(128), Some(8)) == "mem3_ssd1_x8")

        assert(InstanceTypes.apply(Some(WdlString("3 GB")),
                                   Some(WdlString("local-disk 10 HDD")),
                                   Some(WdlString("1"))) == "mem1_ssd1_x2")
        assert(InstanceTypes.apply(Some(WdlString("37 GB")),
                                   Some(WdlString("local-disk 10 HDD")),
                                   Some(WdlString("6"))) == "mem3_ssd1_x8")
        assert(InstanceTypes.apply(Some(WdlString("2 GB")),
                                   Some(WdlString("local-disk 100 HDD")),
                                   None) == "mem1_ssd1_x8")
    }
}
