package dxWDL

import java.nio.file.{Path, Paths}
import org.scalatest.{FlatSpec, Matchers}
//import org.scalatest.Inside._
//import wdl._

class SubblocksTest extends FlatSpec with Matchers {
    lazy val currentWorkDir:Path = Paths.get(System.getProperty("user.dir"))
    private def pathFromBasename(basename: String) : Path = {
        currentWorkDir.resolve(s"src/test/resources/${basename}")
    }


    it should "Compile subblocks into subworkflows" in {
        val path = pathFromBasename("subblocks_as_subworkflows/long_block.wdl")
        Main.compile(
            List(path.toString, "--compileMode", "ir", "--locked", "--verbose")
        ) should equal(Main.SuccessfulTermination(""))
    }
}
