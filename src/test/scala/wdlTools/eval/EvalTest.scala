package wdlTools.eval

import java.nio.file.{Files, Path, Paths}

import org.scalatest.Inside
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import wdlTools.eval.WdlValues._
import wdlTools.syntax.{AbstractSyntax => AST}
import wdlTools.syntax.v1.ParseAll
import wdlTools.util.{EvalConfig, Options, Util => UUtil, TypeCheckingRegime, Verbosity}
import wdlTools.types.{Context => TypeContext, Stdlib => TypeStdlib, TypeChecker}

class EvalTest extends AnyFlatSpec with Matchers with Inside {
  private val srcDir = Paths.get(getClass.getResource("/wdlTools/eval/v1").getPath)
  private val opts =
    Options(typeChecking = TypeCheckingRegime.Lenient,
            antlr4Trace = false,
            localDirectories = Vector(srcDir),
            verbosity = Verbosity.Normal)
  private val parser = ParseAll(opts)
  private val stdlib = TypeStdlib(opts)
  private val checker = TypeChecker(stdlib)

  def safeMkdir(path: Path): Unit = {
    if (!Files.exists(path)) {
      Files.createDirectories(path)
    } else {
      // Path exists, make sure it is a directory, and not a file
      if (!Files.isDirectory(path))
        throw new Exception(s"Path ${path} exists, but is not a directory")
    }
  }

  private lazy val evalCfg: EvalConfig = {
    val baseDir = Paths.get("/tmp/evalTest")
    val homeDir = baseDir.resolve("home")
    val tmpDir = baseDir.resolve("tmp")
    for (d <- Vector(baseDir, homeDir, tmpDir))
      safeMkdir(d)
    val stdout = baseDir.resolve("stdout")
    val stderr = baseDir.resolve("stderr")
    EvalConfig(homeDir, tmpDir, stdout, stderr)
  }

  def parseAndTypeCheck(file: Path): (AST.Document, TypeContext) = {
    val doc = parser.parseDocument(UUtil.pathToUrl(file))
    val typeCtx = checker.apply(doc)
    (doc, typeCtx)
  }

  it should "handle simple expressions" in {
    val file = srcDir.resolve("simple_expr.wdl")
    val (doc, typeCtx) = parseAndTypeCheck(file)
    val evaluator = Eval(opts,
                         evalCfg,
                         typeCtx.structs,
                         wdlTools.syntax.WdlVersion.V1,
                         Some(opts.getUrl(file.toString)))

    doc.workflow should not be empty
    val wf = doc.workflow.get

    val decls: Vector[AST.Declaration] = wf.body.collect {
      case x: AST.Declaration => x
    }

    val ctxEnd = evaluator.applyDeclarations(decls, Context(Map.empty))
    val bindings = ctxEnd.bindings
    bindings("k0") shouldBe WV_Int(-1)
    bindings("k1") shouldBe WV_Int(1)

    bindings("b1") shouldBe WV_Boolean(true || false)
    bindings("b2") shouldBe WV_Boolean(true && false)
    bindings("b3") shouldBe WV_Boolean(10 == 3)
    bindings("b4") shouldBe WV_Boolean(4 < 8)
    bindings("b5") shouldBe WV_Boolean(4 >= 8)
    bindings("b6") shouldBe WV_Boolean(4 != 8)
    bindings("b7") shouldBe WV_Boolean(4 <= 8)
    bindings("b8") shouldBe WV_Boolean(11 > 8)

    // Arithmetic
    bindings("i1") shouldBe WV_Int(3 + 4)
    bindings("i2") shouldBe WV_Int(3 - 4)
    bindings("i3") shouldBe WV_Int(3 % 4)
    bindings("i4") shouldBe WV_Int(3 * 4)
    bindings("i5") shouldBe WV_Int(3 / 4)

    bindings("l0") shouldBe WV_String("a")
    bindings("l1") shouldBe WV_String("b")

    // pairs
    bindings("l") shouldBe WV_String("hello")
    bindings("r") shouldBe WV_Boolean(true)

    // structs
    bindings("pr1") shouldBe WV_Struct("Person",
                                       Map(
                                           "name" -> WV_String("Jay"),
                                           "city" -> WV_String("SF"),
                                           "age" -> WV_Int(31)
                                       ))
    bindings("name") shouldBe WV_String("Jay")
  }

  it should "call stdlib" in {
    val file = srcDir.resolve("stdlib.wdl")
    val (doc, typeCtx) = parseAndTypeCheck(file)
    val evaluator = Eval(opts,
                         evalCfg,
                         typeCtx.structs,
                         wdlTools.syntax.WdlVersion.V1,
                         Some(opts.getUrl(file.toString)))

    doc.workflow should not be empty
    val wf = doc.workflow.get

    val decls: Vector[AST.Declaration] = wf.body.collect {
      case x: AST.Declaration => x
    }

    val ctx = Context(Map.empty).addBinding("empty_string", WV_Null)
    val ctxEnd = evaluator.applyDeclarations(decls, ctx)
    val bd = ctxEnd.bindings

    bd("x") shouldBe WV_Float(1.4)
    bd("n1") shouldBe WV_Int(1)
    bd("n2") shouldBe WV_Int(2)
    bd("n3") shouldBe WV_Int(1)
    bd("cities2") shouldBe WV_Array(
        Vector(WV_String("LA"), WV_String("Seattle"), WV_String("San Francisco"))
    )

    bd("table2") shouldBe WV_Array(
        Vector(
            WV_Array(Vector(WV_String("A"), WV_String("allow"))),
            WV_Array(Vector(WV_String("B"), WV_String("big"))),
            WV_Array(Vector(WV_String("C"), WV_String("clam")))
        )
    )
    bd("m2") shouldBe WV_Map(
        Map(WV_String("name") -> WV_String("hawk"), WV_String("kind") -> WV_String("bird"))
    )

    // sub
    bd("sentence1") shouldBe WV_String(
        "She visited three places on his trip: Aa, Ab, C, D, and E"
    )
    bd("sentence2") shouldBe WV_String(
        "He visited three places on his trip: Berlin, Berlin, C, D, and E"
    )
    bd("sentence3") shouldBe WV_String("H      : A, A, C, D,  E")

    // transpose
    bd("ar3") shouldBe WV_Array(Vector(WV_Int(0), WV_Int(1), WV_Int(2)))
    bd("ar_ar2") shouldBe WV_Array(
        Vector(
            WV_Array(Vector(WV_Int(1), WV_Int(4))),
            WV_Array(Vector(WV_Int(2), WV_Int(5))),
            WV_Array(Vector(WV_Int(3), WV_Int(6)))
        )
    )

    // zip
    bd("zlf") shouldBe WV_Array(
        Vector(
            WV_Pair(WV_String("A"), WV_Boolean(true)),
            WV_Pair(WV_String("B"), WV_Boolean(false)),
            WV_Pair(WV_String("C"), WV_Boolean(true))
        )
    )

    // cross
    inside(bd("cln")) {
      case WV_Array(vec) =>
        // the order of the cross product is unspecified
        Vector(
            WV_Pair(WV_String("A"), WV_Int(1)),
            WV_Pair(WV_String("B"), WV_Int(1)),
            WV_Pair(WV_String("C"), WV_Int(1)),
            WV_Pair(WV_String("A"), WV_Int(13)),
            WV_Pair(WV_String("B"), WV_Int(13)),
            WV_Pair(WV_String("C"), WV_Int(13))
        ).foreach(pair => vec contains pair)
    }

    bd("l1") shouldBe WV_Int(2)
    bd("l2") shouldBe WV_Int(6)

    bd("files2") shouldBe WV_Array(
        Vector(WV_File("A"), WV_File("B"), WV_File("C"), WV_File("G"), WV_File("J"), WV_File("K"))
    )

    bd("pref2") shouldBe WV_Array(
        Vector(WV_String("i_1"), WV_String("i_3"), WV_String("i_5"), WV_String("i_7"))
    )
    bd("pref3") shouldBe WV_Array(
        Vector(WV_String("sub_1.0"), WV_String("sub_3.4"), WV_String("sub_5.1"))
    )

    bd("sel1") shouldBe WV_String("A")
    bd("sel2") shouldBe WV_String("Henry")
    bd("sel3") shouldBe WV_Array(Vector(WV_String("Henry"), WV_String("bear"), WV_String("tree")))

    bd("d1") shouldBe WV_Boolean(true)
    bd("d2") shouldBe WV_Boolean(false)
    bd("d3") shouldBe WV_Boolean(true)

    // basename
    bd("path1") shouldBe WV_String("C.txt")
    bd("path2") shouldBe WV_String("nuts_and_bolts.txt")
    bd("path3") shouldBe WV_String("docs.md")
    bd("path4") shouldBe WV_String("C")
    bd("path5") shouldBe WV_String("C.txt")

    // read/write object
    val obj1 = WV_Object(
        Map("author" -> WV_String("Benjamin"),
            "year" -> WV_String("1973"),
            "title" -> WV_String("Color the sky green"))
    )
    val obj2 = WV_Object(
        Map("author" -> WV_String("Primo Levy"),
            "year" -> WV_String("1975"),
            "title" -> WV_String("The Periodic Table"))
    )
    bd("o2") shouldBe obj1
    bd("arObj") shouldBe WV_Array(Vector(obj1, obj2))
    bd("houseObj") shouldBe WV_Object(
        Map("city" -> WV_String("Seattle"),
            "team" -> WV_String("Trail Blazers"),
            "zipcode" -> WV_Int(98109))
    )

  }

  it should "perform coercions" in {
    val file = srcDir.resolve("coercions.wdl")
    val (doc, typeCtx) = parseAndTypeCheck(file)
    val evaluator = Eval(opts,
                         evalCfg,
                         typeCtx.structs,
                         wdlTools.syntax.WdlVersion.V1,
                         Some(opts.getUrl(file.toString)))

    doc.workflow should not be empty
    val wf = doc.workflow.get

    val decls: Vector[AST.Declaration] = wf.body.collect {
      case x: AST.Declaration => x
    }

    val ctxEnd = evaluator.applyDeclarations(decls, Context(Map.empty))
    val bd = ctxEnd.bindings

    bd("i1") shouldBe WV_Int(13)
    bd("i2") shouldBe WV_Int(13)
    bd("i3") shouldBe WV_Int(8)

    bd("x1") shouldBe WV_Float(3)
    bd("x2") shouldBe WV_Float(13)
    bd("x3") shouldBe WV_Float(44.3)
    bd("x4") shouldBe WV_Float(44.3)
    bd("x5") shouldBe WV_Float(4.5)

    bd("s1") shouldBe WV_String("true")
    bd("s2") shouldBe WV_String("3")
    bd("s3") shouldBe WV_String("4.3")
    bd("s4") shouldBe WV_String("hello")
    bd("s5") shouldBe WV_Optional(WV_String("hello"))
  }

  private def evalCommand(wdlSourceFileName: String): String = {
    val file = srcDir.resolve(wdlSourceFileName)
    val (doc, typeCtx) = parseAndTypeCheck(file)
    val evaluator = Eval(opts,
                         evalCfg,
                         typeCtx.structs,
                         wdlTools.syntax.WdlVersion.V1,
                         Some(opts.getUrl(file.toString)))

    doc.elements should not be empty
    val task = doc.elements.head.asInstanceOf[AST.Task]
    val ctx = evaluator.applyDeclarations(task.declarations, Context(Map.empty))
    evaluator.applyCommand(task.command, ctx)
  }

  it should "evaluate simple command section" in {
    val command = evalCommand("command_simple.wdl")
    command.trim shouldBe "We just discovered a new flower with 100 basepairs. Is that possible?"
  }

  it should "evaluate command section with some variables" in {
    val command = evalCommand("command2.wdl")
    command.trim shouldBe "His trumpet playing is not bad"
  }

  it should "command section with several kinds of primitives" in {
    val command = evalCommand("command3.wdl")
    command.trim shouldBe "His trumpet playing is good. There are 10 instruments in the band. It this true?"
  }

  it should "separator placeholder" in {
    val command = evalCommand("sep_placeholder.wdl")
    command.trim shouldBe "We have lots of numbers here 1, 10, 100"
  }

  it should "boolean placeholder" taggedAs Edge in {
    val command = evalCommand("bool_placeholder.wdl")
    val lines = command.split("\n")
    lines(1).trim shouldBe "--no"
    lines(2).trim shouldBe "--yes"
  }

  it should "default placeholder" taggedAs Edge in {
    val command = evalCommand("default_placeholder.wdl")
    command.trim shouldBe "hello"
  }

  it should "bad coercion" in {
    val file = srcDir.resolve("bad_coercion.wdl")
    val (doc, typeCtx) = parseAndTypeCheck(file)
    val evaluator = Eval(opts,
                         evalCfg,
                         typeCtx.structs,
                         wdlTools.syntax.WdlVersion.V1,
                         Some(opts.getUrl(file.toString)))

    doc.workflow should not be empty
    val wf = doc.workflow.get

    val decls: Vector[AST.Declaration] = wf.body.collect {
      case x: AST.Declaration => x
    }

    assertThrows[EvalException] {
      val _ = evaluator.applyDeclarations(decls, Context(Map.empty))
    }
  }

}
