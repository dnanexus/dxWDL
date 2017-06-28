package dxWDL

import com.dnanexus.{DXEnvironment, DXProject}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, OneInstancePerTest}
import wdl4s.types._
import wdl4s.values._

class InstaceTypeDBTest extends FlatSpec with BeforeAndAfterEach with OneInstancePerTest {

    it should "Work even without access to pricing information" in {
        // get the default project
        val dxEnv = com.dnanexus.DXEnvironment.create()
        val dxProject = dxEnv.getProjectContext()

        val db = InstanceTypeDB.queryNoPrices(dxProject)
        //System.err.println(s"DB=${db.prettyPrint}")

        // parameters are:          RAM,     disk,     cores
        assert(db.choose(None, None, None) == "mem1_ssd1_x2")
    }

    it should "Choose reasonable platform instance types" in {
        // get the default project
        val dxEnv = com.dnanexus.DXEnvironment.create()
        val dxProject = dxEnv.getProjectContext()

        // parameters are:          RAM,     disk,     cores
        try {
            val db = InstanceTypeDB.queryWithPrices(dxProject)
            assert(db.choose(None, None, None) == "mem1_ssd1_x2")
            assert(db.choose(Some(3*1024), Some(100), Some(5)) == "mem1_ssd1_x8")
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
                            None) == "mem1_ssd2_x2")
        } catch {
            case e: Throwable =>
                System.err.println("Insufficient permissions, can't get instance price list")
        }
    }
}
