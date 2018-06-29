package dxWDL.compiler

import dxWDL.{Main}
import java.nio.file.{Path, Paths}
import org.scalatest.{FlatSpec, Matchers}


// These tests involve compilation -without- access to the platform.
//
class ImportsTest extends FlatSpec with Matchers {
    lazy val currentWorkDir:Path = Paths.get(System.getProperty("user.dir"))
    private def pathFromBasename(basename: String) : Path = {
        currentWorkDir.resolve(s"src/test/resources/${basename}")
    }

    it should "be able to import http URLs" in {
        val path = pathFromBasename("imports/http_import.wdl")
        Main.compile(
            List(path.toString, "--compileMode", "ir", "-quiet")
        ) shouldBe a [Main.SuccessfulTerminationIR]
    }

}
