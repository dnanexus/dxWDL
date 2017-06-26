package dxWDL

import java.nio.file.{Files, FileSystems, Path, Paths, PathMatcher}
import org.scalatest.{BeforeAndAfterEach, FlatSpec}
import wdl4s.types._
import wdl4s.values._

class DxFunctionsTest extends FlatSpec with BeforeAndAfterEach {

    it should "implement glob correctly" in {
        System.err.println(DxFunctions.glob("/tmp/K", "*.txt"))
        System.err.println(DxFunctions.glob("/tmp", "*.txt"))
    }
}
