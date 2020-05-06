package wdlTools.syntax.draft_2

import java.nio.file.Paths

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import wdlTools.syntax.{Comment, Edge, SyntaxException, TextSource}
import wdlTools.syntax.draft_2.ConcreteSyntax._
import wdlTools.util.Verbosity.Quiet
import wdlTools.util.{Options, SourceCode, Util}

class ConcreteSyntaxDraft2Test extends AnyFlatSpec with Matchers {
  private val sourcePath = Paths.get(getClass.getResource("/wdlTools/syntax/draft_2").getPath)
  private val tasksDir = sourcePath.resolve("tasks")
  private val workflowsDir = sourcePath.resolve("workflows")
  private val opts = Options(
      antlr4Trace = false,
      verbosity = Quiet,
      localDirectories = Vector(tasksDir, workflowsDir)
  )
  private val grammarFactory = WdlDraft2GrammarFactory(opts)

  private def getTaskSource(fname: String): SourceCode = {
    SourceCode.loadFrom(Util.pathToUrl(tasksDir.resolve(fname)))
  }

  private def getWorkflowSource(fname: String): SourceCode = {
    SourceCode.loadFrom(Util.pathToUrl(workflowsDir.resolve(fname)))
  }

  private def getDocument(sourceCode: SourceCode, conf: Options = opts): Document = {
    ParseTop(conf,
             grammarFactory.createGrammar(sourceCode.toString),
             sourceCode.toString,
             Some(sourceCode.url)).parseDocument
  }

  it should "handle various types" in {
    val doc = getDocument(getTaskSource("types.wdl"))

    doc.elements.size shouldBe 1
    val elem = doc.elements(0)
    elem shouldBe a[Task]
    val task = elem.asInstanceOf[Task]

    val InputSection(decls, _) = task.input.get
    decls(0) should matchPattern { case Declaration("i", TypeInt(_), None, _)     => }
    decls(1) should matchPattern { case Declaration("s", TypeString(_), None, _)  => }
    decls(2) should matchPattern { case Declaration("x", TypeFloat(_), None, _)   => }
    decls(3) should matchPattern { case Declaration("b", TypeBoolean(_), None, _) => }
    decls(4) should matchPattern { case Declaration("f", TypeFile(_), None, _)    => }
    decls(5) should matchPattern {
      case Declaration("p1", TypePair(_: TypeInt, _: TypeString, _), None, _) =>
    }
    decls(6) should matchPattern {
      case Declaration("p2", TypePair(_: TypeFloat, _: TypeFile, _), None, _) =>
    }
    decls(7) should matchPattern {
      case Declaration("p3", TypePair(_: TypeBoolean, _: TypeBoolean, _), None, _) =>
    }
    decls(8) should matchPattern {
      case Declaration("ia", TypeArray(_: TypeInt, false, _), None, _) =>
    }
    decls(9) should matchPattern {
      case Declaration("sa", TypeArray(_: TypeString, false, _), None, _) =>
    }
    decls(10) should matchPattern {
      case Declaration("xa", TypeArray(_: TypeFloat, false, _), None, _) =>
    }
    decls(11) should matchPattern {
      case Declaration("ba", TypeArray(_: TypeBoolean, false, _), None, _) =>
    }
    decls(12) should matchPattern {
      case Declaration("fa", TypeArray(_: TypeFile, false, _), None, _) =>
    }
    decls(13) should matchPattern {
      case Declaration("m_si", TypeMap(_: TypeInt, _: TypeString, _), None, _) =>
    }
    decls(14) should matchPattern {
      case Declaration("m_ff", TypeMap(_: TypeFile, _: TypeFile, _), None, _) =>
    }
    decls(15) should matchPattern {
      case Declaration("m_bf", TypeMap(_: TypeBoolean, _: TypeFloat, _), None, _) =>
    }
  }

  it should "handle types and expressions" in {
    val doc = getDocument(getTaskSource("expressions.wdl"))

    doc.elements.size shouldBe 1
    val elem = doc.elements(0)
    elem shouldBe a[Task]
    val task = elem.asInstanceOf[Task]

    task.input.get.declarations(0) should matchPattern {
      case Declaration("i", _: TypeInt, Some(ExprInt(3, _)), _) =>
    }
    task.input.get.declarations(1) should matchPattern {
      case Declaration("s", _: TypeString, Some(ExprString("hello world", _)), _) =>
    }
    task.input.get.declarations(2) should matchPattern {
      case Declaration("x", _: TypeFloat, Some(ExprFloat(4.3, _)), _) =>
    }
    task.input.get.declarations(3) should matchPattern {
      case Declaration("f", _: TypeFile, Some(ExprString("/dummy/x.txt", _)), _) =>
    }
    task.input.get.declarations(4) should matchPattern {
      case Declaration("b", _: TypeBoolean, Some(ExprBoolean(false, _)), _) =>
    }
    task.input.get.declarations(5) should matchPattern {
      case Declaration(
          "ia",
          TypeArray(_: TypeInt, false, _),
          Some(ExprArrayLiteral(Vector(ExprInt(1, _), ExprInt(2, _), ExprInt(3, _)), _)),
          _
          ) =>
    }
    task.input.get.declarations(6) should matchPattern {
      case Declaration("ia",
                       TypeArray(_: TypeInt, true, _),
                       Some(ExprArrayLiteral(Vector(ExprInt(10, _)), _)),
                       _) =>
    }
    task.input.get.declarations(7) should matchPattern {
      case Declaration("m", TypeMap(TypeInt(_), TypeString(_), _), Some(ExprMapLiteral(_, _)), _) =>
    }
    task.input.get.declarations(8) should matchPattern {
      case Declaration("o", _: TypeObject, Some(ExprObjectLiteral(_, _)), _) =>
    }
    /*(Map("A" -> ExprInt(1, _),
     "B" -> ExprInt(2, _,
     _))), _)) */

    task.input.get.declarations(9) should matchPattern {
      case Declaration("twenty_threes",
                       TypePair(TypeInt(_), TypeString(_), _),
                       Some(ExprPair(ExprInt(23, _), ExprString("twenty-three", _), _)),
                       _) =>
    }

    // Logical expressions
    task.declarations(0) should matchPattern {
      case Declaration("b2",
                       _: TypeBoolean,
                       Some(ExprLor(ExprBoolean(true, _), ExprBoolean(false, _), _)),
                       _) =>
    }
    task.declarations(1) should matchPattern {
      case Declaration("b3",
                       _: TypeBoolean,
                       Some(ExprLand(ExprBoolean(true, _), ExprBoolean(false, _), _)),
                       _) =>
    }
    task.declarations(2) should matchPattern {
      case Declaration("b4", _: TypeBoolean, Some(ExprEqeq(ExprInt(3, _), ExprInt(5, _), _)), _) =>
    }
    task.declarations(3) should matchPattern {
      case Declaration("b5", _: TypeBoolean, Some(ExprLt(ExprInt(4, _), ExprInt(5, _), _)), _) =>
    }
    task.declarations(4) should matchPattern {
      case Declaration("b6", _: TypeBoolean, Some(ExprGte(ExprInt(4, _), ExprInt(5, _), _)), _) =>
    }
    task.declarations(5) should matchPattern {
      case Declaration("b7", _: TypeBoolean, Some(ExprNeq(ExprInt(6, _), ExprInt(7, _), _)), _) =>
    }
    task.declarations(6) should matchPattern {
      case Declaration("b8", _: TypeBoolean, Some(ExprLte(ExprInt(6, _), ExprInt(7, _), _)), _) =>
    }
    task.declarations(7) should matchPattern {
      case Declaration("b9", _: TypeBoolean, Some(ExprGt(ExprInt(6, _), ExprInt(7, _), _)), _) =>
    }
    task.declarations(8) should matchPattern {
      case Declaration("b10", _: TypeBoolean, Some(ExprNegate(ExprIdentifier("b2", _), _)), _) =>
    }

    // Arithmetic
    task.declarations(9) should matchPattern {
      case Declaration("j", _: TypeInt, Some(ExprAdd(ExprInt(4, _), ExprInt(5, _), _)), _) =>
    }
    task.declarations(10) should matchPattern {
      case Declaration("j1", _: TypeInt, Some(ExprMod(ExprInt(4, _), ExprInt(10, _), _)), _) =>
    }
    task.declarations(11) should matchPattern {
      case Declaration("j2", _: TypeInt, Some(ExprDivide(ExprInt(10, _), ExprInt(7, _), _)), _) =>
    }
    task.declarations(12) should matchPattern {
      case Declaration("j3", _: TypeInt, Some(ExprIdentifier("j", _)), _) =>
    }
    task.declarations(13) should matchPattern {
      case Declaration("j4",
                       _: TypeInt,
                       Some(ExprAdd(ExprIdentifier("j", _), ExprInt(19, _), _)),
                       _) =>
    }

    task.declarations(14) should matchPattern {
      case Declaration("k",
                       _: TypeInt,
                       Some(ExprAt(ExprIdentifier("ia", _), ExprInt(3, _), _)),
                       _) =>
    }
    task.declarations(15) should matchPattern {
      case Declaration(
          "k2",
          _: TypeInt,
          Some(ExprApply("f", Vector(ExprInt(1, _), ExprInt(2, _), ExprInt(3, _)), _)),
          _
          ) =>
    }

    /*    val m = task.declarations(23).expr
    m should matchPattern {
      case Map(ExprInt(1, _) -> ExprString("a", _),
                                      ExprInt(2, _) -> ExprString("b", _)),
    _)),*/

    task.declarations(16) should matchPattern {
      case Declaration("k3",
                       _: TypeInt,
                       Some(ExprIfThenElse(ExprBoolean(true, _), ExprInt(1, _), ExprInt(2, _), _)),
                       _) =>
    }
    task.declarations(17) should matchPattern {
      case Declaration("k4", _: TypeInt, Some(ExprGetName(ExprIdentifier("x", _), "a", _)), _) =>
    }
  }

  it should "handle get name" in {
    val doc = getDocument(getTaskSource("get_name_bug.wdl"))

    doc.elements.size shouldBe 1
    val elem = doc.elements(0)
    elem shouldBe a[Task]
    val task = elem.asInstanceOf[Task]

    task.name shouldBe "district"
    task.declarations(0) should matchPattern {
      case Declaration("k", TypeInt(_), Some(ExprGetName(ExprIdentifier("x", _), "a", _)), _) =>
    }
    task.output shouldBe None
    task.command should matchPattern {
      case CommandSection(Vector(), _) =>
    }
    task.meta shouldBe None
    task.parameterMeta shouldBe None
  }

  it should "detect a wrong comment style" in {
    val confQuiet = opts.copy(verbosity = Quiet)
    assertThrows[Exception] {
      getDocument(getTaskSource("wrong_comment_style.wdl"), confQuiet)
    }
  }

  it should "parse a task with an output section only" in {
    val doc = getDocument(getTaskSource("output_section.wdl"))

    doc.elements.size shouldBe 1
    val elem = doc.elements(0)
    elem shouldBe a[Task]
    val task = elem.asInstanceOf[Task]

    task.name shouldBe "wc"
    task.output.get should matchPattern {
      case OutputSection(Vector(Declaration("num_lines", TypeInt(_), Some(ExprInt(3, _)), _)), _) =>
    }
  }

  it should "parse a task" in {
    val doc = getDocument(getTaskSource("wc.wdl"))

    doc.comments(1) should matchPattern {
      case Comment("# A task that counts how many lines a file has", TextSource(1, 0, 1, 46)) =>
    }
    doc.comments(4) should matchPattern {
      case Comment("# Just a random declaration", TextSource(4, 2, 4, 29)) =>
    }
    doc.comments(7) should matchPattern {
      case Comment("# comment after bracket", TextSource(7, 12, 7, 35)) =>
    }
    doc.comments(8) should matchPattern {
      case Comment("# Int num_lines = read_int(stdout())", TextSource(8, 4, 8, 40)) =>
    }
    doc.comments(9) should matchPattern {
      case Comment("# end of line comment", TextSource(9, 23, 9, 44)) =>
    }
    doc.comments(18) should matchPattern {
      case Comment("# The comment below is empty", TextSource(18, 4, 18, 32)) =>
    }
    doc.comments(19) should matchPattern {
      case Comment("#", TextSource(19, 4, 19, 5)) =>
    }

    doc.elements.size shouldBe 1
    val elem = doc.elements(0)
    elem shouldBe a[Task]
    val task = elem.asInstanceOf[Task]

    task.name shouldBe "wc"
    task.input.get should matchPattern {
      case InputSection(Vector(Declaration("inp_file", _: TypeFile, None, _)), _) =>
    }
    task.declarations should matchPattern {
      case Vector(
          Declaration("i", TypeInt(_), Some(ExprAdd(ExprInt(4, _), ExprInt(5, _), _)), _)
          ) =>
    }
    task.output.get should matchPattern {
      case OutputSection(Vector(Declaration("num_lines", _: TypeInt, Some(ExprInt(3, _)), _)), _) =>
    }
    task.command shouldBe a[CommandSection]
    task.command.parts.size shouldBe 3
    task.command.parts(0) should matchPattern {
      case ExprString("\n    # this is inside the command and so not a WDL comment\n    wc -l ",
                      _) =>
    }
    task.command.parts(1) should matchPattern {
      case ExprIdentifier("inp_file", _) =>
    }

    task.meta.get shouldBe a[MetaSection]
    task.meta.get.kvs.size shouldBe 1
    val mkv = task.meta.get.kvs.head
    mkv should matchPattern {
      case MetaKV("author", "Robin Hood", _) =>
    }

    task.parameterMeta.get shouldBe a[ParameterMetaSection]
    task.parameterMeta.get.kvs.size shouldBe 1
    val mpkv = task.parameterMeta.get.kvs.head
    mpkv should matchPattern {
      case MetaKV("inp_file", "just because", _) =>
    }
  }

  it should "detect when a task section appears twice" in {
    assertThrows[Exception] {
      getDocument(getTaskSource("multiple_input_section.wdl"))
    }
  }

  it should "handle string interpolation" in {
    val doc = getDocument(getTaskSource("interpolation.wdl"))

    doc.elements.size shouldBe 1
    val elem = doc.elements(0)
    elem shouldBe a[Task]
    val task = elem.asInstanceOf[Task]

    task.name shouldBe "foo"
    task.input.get shouldBe a[InputSection]
    task.input.get.declarations.size shouldBe 2
    task.input.get.declarations(0) should matchPattern {
      case Declaration("min_std_max_min", TypeInt(_), None, _) =>
    }
    task.input.get.declarations(1) should matchPattern {
      case Declaration("prefix", TypeString(_), None, _) =>
    }

    task.command shouldBe a[CommandSection]
    task.command.parts(0) should matchPattern {
      case ExprString("\n    echo ", _) =>
    }
    task.command.parts(1) should matchPattern {
      case ExprPlaceholderSep(ExprString(",", _), ExprIdentifier("min_std_max_min", _), _) =>
    }
  }

  it should "parse a simple workflow" in {
    val doc = getDocument(getWorkflowSource("I.wdl"))
    doc.elements.size shouldBe 0

    val wf = doc.workflow.get
    wf shouldBe a[Workflow]

    wf.name shouldBe "biz"
    wf.body.size shouldBe 3

    val calls = wf.body.collect {
      case x: Call => x
    }
    calls.size shouldBe 1
    calls(0) should matchPattern {
      case Call("bar", Some(CallAlias("boz", _)), _, _) =>
    }
    calls(0).inputs.get.value should matchPattern {
      case Vector(CallInput("i", ExprIdentifier("s", _), _)) =>
    }

    val scatters = wf.body.collect {
      case x: Scatter => x
    }
    scatters.size shouldBe 1
    scatters(0).identifier shouldBe "i"
    scatters(0).expr should matchPattern {
      case ExprArrayLiteral(Vector(ExprInt(1, _), ExprInt(2, _), ExprInt(3, _)), _) =>
    }
    scatters(0).body.size shouldBe 1
    scatters(0).body(0) should matchPattern {
      case Call("add", None, _, _) =>
    }
    val call = scatters(0).body(0).asInstanceOf[Call]
    call.inputs.get.value should matchPattern {
      case Vector(CallInput("x", ExprIdentifier("i", _), _)) =>
    }

    val conditionals = wf.body.collect {
      case x: Conditional => x
    }
    conditionals.size shouldBe 1
    conditionals(0).expr should matchPattern {
      case ExprEqeq(ExprBoolean(true, _), ExprBoolean(false, _), _) =>
    }
    conditionals(0).body should matchPattern {
      case Vector(Call("sub", None, _, _)) =>
    }
    conditionals(0).body(0).asInstanceOf[Call].inputs.size shouldBe 0

    wf.meta.get shouldBe a[MetaSection]
    wf.meta.get.kvs.size shouldEqual 1
    wf.meta.get.kvs.head.value shouldEqual "Robert Heinlein"
    wf.meta.get.kvs should matchPattern {
      case Vector(MetaKV("author", "Robert Heinlein", _)) =>
    }
  }

  it should "handle import statements" in {
    val doc = getDocument(getWorkflowSource("imports.wdl"))

    val imports = doc.elements.collect {
      case x: ImportDoc => x
    }
    imports.size shouldBe 2

    doc.workflow should not be empty

    val calls: Vector[Call] = doc.workflow.get.body.collect {
      case call: Call => call
    }
    calls(0).name shouldBe "I.biz"
    calls(1).name shouldBe "I.undefined"
    calls(2).name shouldBe "wc"
  }

  it should "parse a workflow that is illegal in v1.0" in {
    val _ = getDocument(getWorkflowSource("bad_declaration.wdl"))
  }

  it should "handle chained operations" in {
    val doc = getDocument(getTaskSource("bug16-chained-operations.wdl"))

    doc.elements.size shouldBe 1
    val elem = doc.elements(0)
    elem shouldBe a[Task]
    val task = elem.asInstanceOf[Task]

    task.declarations.size shouldBe 1
    val decl = task.declarations.head
    decl.name shouldBe "j"
    decl.expr.get should matchPattern {
      case ExprAdd(ExprAdd(ExprIdentifier("i", _), ExprIdentifier("i", _), _),
                   ExprIdentifier("i", _),
                   _) =>
    }
  }

  it should "handle chained operations in a workflow" in {
    val doc = getDocument(getWorkflowSource("chained_expr.wdl"))
    doc.elements.size shouldBe 0

    val wf = doc.workflow.get
    wf shouldBe a[Workflow]

    wf.body.size shouldBe 1
    val decl = wf.body.head.asInstanceOf[Declaration]
    decl.name shouldBe "b"
    decl.expr.get should matchPattern {
      case ExprAdd(ExprAdd(ExprInt(1, _), ExprInt(2, _), _), ExprInt(3, _), _) =>
    }
  }

  it should "handle compound expression" taggedAs Edge in {
    val doc = getDocument(getWorkflowSource("compound_expr_bug.wdl"))
    doc.elements.size shouldBe 0

    val wf = doc.workflow.get
    wf shouldBe a[Workflow]

    wf.body.size shouldBe 1
    val decl = wf.body.head.asInstanceOf[Declaration]
    decl.name shouldBe "a"
    decl.expr.get should matchPattern {
      case ExprApply("select_first",
                     Vector(
                         ExprArrayLiteral(Vector(ExprInt(3, _),
                                                 ExprApply("round", Vector(ExprInt(100, _)), _)),
                                          _)
                     ),
                     _) =>
        ()
    }
  }

  it should "report bad types" in {
    assertThrows[SyntaxException] {
      getDocument(getWorkflowSource("bad_type.wdl"))
    }
  }
}
