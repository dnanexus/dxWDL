package dxWDL.compiler

import dxWDL.Main
import java.nio.file.{Path, Paths}
import org.scalatest.{FlatSpec, Matchers}

class DecomposeBlocksTest extends FlatSpec with Matchers {
    lazy val currentWorkDir:Path = Paths.get(System.getProperty("user.dir"))
    private def pathFromBasename(basename: String) : Path = {
        currentWorkDir.resolve(s"src/test/resources/decompose_blocks/${basename}")
    }


    ignore should "Compile subblocks into subworkflows" in {
        val path = pathFromBasename("long_block.wdl")
        Main.compile(
            List(path.toString, "--compileMode", "ir", "--locked", "--quiet")
        ) should equal(Main.SuccessfulTermination(""))
    }

    it should "recognize references to blocks compiled into sub-workflows" in {
        val path = pathFromBasename("long_refs.wdl")
        Main.compile(
            List(path.toString, "--compileMode", "ir", "--locked",
                 "--verbose", "decompose", "--verbose", "NamespaceOps")
        ) should equal(Main.SuccessfulTermination(""))
    }

    ignore should "handle wide and deep nesting" in {
        val path = pathFromBasename("two_phase.wdl")
        Main.compile(
            List(path.toString, "--compileMode", "ir", "--locked", "--quiet")
        ) should equal(Main.SuccessfulTermination(""))
    }


    ignore should "recognize dependencies inside an interpolation" in {
        val path = pathFromBasename("interpolation.wdl")
        Main.compile(
            List(path.toString, "--compileMode", "ir", "--locked", "--quiet")
        ) should equal(Main.SuccessfulTermination(""))
    }
}
