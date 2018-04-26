package dxWDL.compiler

import dxWDL.Main
import java.nio.file.{Path, Paths}
import org.scalatest.{FlatSpec, Matchers}

class DecomposeBlocksTest extends FlatSpec with Matchers {
    lazy val currentWorkDir:Path = Paths.get(System.getProperty("user.dir"))
    private def pathFromBasename(basename: String) : Path = {
        currentWorkDir.resolve(s"src/test/resources/decompose_blocks/${basename}")
    }


    it should "Compile subblocks into subworkflows" in {
        val path = pathFromBasename("long_block.wdl")
        Main.compile(
            List(path.toString, "--compileMode", "ir", "--locked")
        ) should equal(Main.SuccessfulTermination(""))
    }

    it should "recognize references to blocks compiled into sub-workflows" in {
        val path = pathFromBasename("long_refs.wdl")
        Main.compile(
            List(path.toString, "--compileMode", "ir", "--locked", "--quiet")
        ) should equal(Main.SuccessfulTermination(""))
    }

    it should "handle wide and deep nesting" in {
        val path = pathFromBasename("two_phase.wdl")
        Main.compile(
            List(path.toString, "--compileMode", "ir", "--locked", "--quiet")
        ) should equal(Main.SuccessfulTermination(""))
    }


    it should "recognize dependencies inside an interpolation" in {
        val path = pathFromBasename("interpolation.wdl")
        Main.compile(
            List(path.toString, "--compileMode", "ir", "--locked", "--quiet")
        ) should equal(Main.SuccessfulTermination(""))
    }

    it should "avoid giving the same name to scatters with similar items" in {
        val path = pathFromBasename("naming_scatters.wdl")
        Main.compile(
            List(path.toString, "--compileMode", "ir", "--locked", "--quiet")
        ) should equal(Main.SuccessfulTermination(""))
    }

}
