package dxWDL

import java.nio.file.{Files, FileSystems, Path, Paths, PathMatcher}
import org.scalatest.{BeforeAndAfterEach, FlatSpec}
import wdl4s.types._
import wdl4s.values._

class DxFunctionsTest extends FlatSpec with BeforeAndAfterEach {

    // Search for the pattern in the home directory. Not clear
    // yet what to do with the [path] argument.
    def glob(path: String, pattern: String): Seq[String] = {
        val baseDir: Path =
            if (path.isEmpty) Paths.get("/tmp")
            else Paths.get("/tmp").resolve(path)
        val matcher:PathMatcher = FileSystems.getDefault()
            .getPathMatcher(s"glob:${baseDir.toString}/${pattern}")
        baseDir.toFile.listFiles
            .filter(_.isFile)
            .map(_.toPath)
            .filter(matcher.matches(_))
            .map(_.toString)
            .toSeq
    }

    it should "implement glob correctly" in {
        System.err.println(glob("K", "*.txt"))
        System.err.println(glob("", "*.txt"))
    }
}
