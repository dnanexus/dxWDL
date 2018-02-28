package dxWDL

import java.nio.file.{Path, Paths}
import org.scalatest.{FlatSpec, Matchers}
import wdl._

class CompilerUnitTest extends FlatSpec with Matchers {
    lazy val currentWorkDir:Path = Paths.get(System.getProperty("user.dir"))
    private def pathFromBasename(basename: String) : Path = {
        currentWorkDir.resolve(s"src/test/resources/${basename}")
    }

    private def compareIgnoreWhitespace(a: String, b:String): Boolean = {
        val retval = (a.replaceAll("\\s+", "") == b.replaceAll("\\s+", ""))
        if (!retval) {
            System.err.println("--- String comparison failed ---")
            System.err.println(s"${a}")
            System.err.println("---")
            System.err.println(s"${b}")
            System.err.println("---")
        }
        retval
    }


    // These tests require compilation -without- access to the platform.
    // We need to split the compiler into front/back-ends to be able to
    // do this.
    it should "Allow adding unbound argument" in {
        val path = pathFromBasename("unbound_arg.wdl")
        Main.compile(
            List(path.toString, "--compileMode", "ir", "-quiet")
        ) should equal(Main.SuccessfulTermination(""))
    }

    it should "Report a useful error for a missing reference" in {
        val path = pathFromBasename("ngs.wdl")
        val retval = Main.compile(
            List(path.toString, "--compileMode", "ir", "--locked", "--quiet")
        )
        retval match  {
            case Main.UnsuccessfulTermination(errMsg) =>
                errMsg should include ("Could not resolve")
            case _ =>
                true should equal(false)
        }
    }

    it should "Handle array access" in {
        val path = pathFromBasename("file_array.wdl")
        val retval = Main.compile(
            List(path.toString, "--compileMode", "ir", "--locked")
        )
        retval match  {
            case Main.SuccessfulTermination(_) =>
                true should equal(true)
            case _ =>
                print(retval)
                true should equal(false)
        }
    }

    it should "Pretty print declaration" in {
        val wdl = "Array[Int] integers"
        val ns = WdlNamespace.loadUsingSource(wdl, None, None).get
        val decl = ns.declarations.head
        val strWdlCode = WdlPrettyPrinter(true, None).apply(decl, 0).mkString("\n")
        compareIgnoreWhitespace(strWdlCode, wdl) should be(true)
    }

    it should "Pretty print task" in {
        val wdl = """|task inc {
                     |  File input_file
                     |
                     |  command <<<
                     |     wc -l ${input_file} | awk '{print $1}' > line.count
                     |  >>>
                     |
                     |  output {
                     |    Int line_count = read_int("line.count")
                     |  }
                     |}""".stripMargin.trim

        val ns = WdlNamespace.loadUsingSource(wdl, None, None).get
        val task = ns.findTask("inc").get
        WdlPrettyPrinter(false, None).commandBracketTaskSymbol(task) should be ("<<<",">>>")
    }

    it should "Report a useful error for an invalid call name" in {
        val path = pathFromBasename("illegal_call_name.wdl")
        val retval = Main.compile(
            List(path.toString, "--compileMode", "ir", "--locked", "--quiet")
        )
        retval match  {
            case Main.UnsuccessfulTermination(errMsg) =>
                errMsg should include ("Illegal call name")
            case _ =>
                true should equal(false)
        }
    }

}
