package dxWDL.exec

import java.nio.file.{Path, Paths}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import spray.json._

import wdlTools.eval.{Context => EvalContext, Eval => WdlExprEval, EvalConfig, WdlValues}
import wdlTools.types.{TypedAbstractSyntax => TAT, TypeOptions, WdlTypes}

import dxWDL.base.{Language, ParseWomSourceFile, RunnerWfFragmentMode, Utils, WdlRuntimeAttrs}
import dxWDL.dx.ExecLinkInfo
import dxWDL.util.{
  Block,
  DxIoFunctions,
  DxInstanceType,
  DxPathConfig,
  InstanceTypeDB
}

// This test module requires being logged in to the platform.
// It compiles WDL scripts without the runtime library.
// This tests the compiler Native mode, however, it creates
// dnanexus applets and workflows that are not runnable.
class WfFragRunnerTest extends AnyFlatSpec with Matchers {
  private val runtimeDebugLevel = 0
  private val unicornInstance =
    DxInstanceType("mem_ssd_unicorn", 100, 100, 4, 1.00f, Vector(("Ubuntu", "16.04")), false)
  private val instanceTypeDB = InstanceTypeDB(true, Vector(unicornInstance))

  private def setup(): (DxPathConfig, DxIoFunctions) = {
    // Create a clean directory in "/tmp" for the task to use
    val jobHomeDir: Path = Paths.get("/tmp/dxwdl_applet_test")
    Utils.deleteRecursive(jobHomeDir.toFile)
    Utils.safeMkdir(jobHomeDir)
    val dxPathConfig = DxPathConfig.apply(jobHomeDir, false, runtimeDebugLevel >= 1)
    dxPathConfig.createCleanDirs()
    val dxIoFunctions = DxIoFunctions(Map.empty, dxPathConfig, runtimeDebugLevel)
    (dxPathConfig, dxIoFunctions)
  }

  private def setupFragRunner(dxPathConfig: DxPathConfig,
                              dxIoFunctions: DxIoFunctions,
                              wfSourceCode: String): (TAT.Workflow, WfFragRunner) = {
    val (wf, taskDir, typeAliases, document) =
      ParseWomSourceFile(false).parseWdlWorkflow(wfSourceCode)
    val fragInputOutput =
      new WfFragInputOutput(dxIoFunctions, null, typeAliases, document.version.value, runtimeDebugLevel)
    val fragRunner = new WfFragRunner(wf,
                                      taskDir,
                                      typeAliases,
                                      document,
                                      instanceTypeDB,
                                      Map.empty[String, ExecLinkInfo],
                                      dxPathConfig,
                                      dxIoFunctions,
                                      JsNull,
                                      fragInputOutput,
                                      Some(WdlRuntimeAttrs(Map.empty)),
                                      Some(false),
                                      runtimeDebugLevel)
    (wf, fragRunner)
  }

  // Note: if the file doesn't exist, this throws a null pointer exception
  def pathFromBasename(dir: String, basename: String): Path = {
    val p = getClass.getResource(s"/${dir}/${basename}").getPath
    Paths.get(p)
  }

  def evaluateWomExpression(expr: TAT.Expr,
                            env: Map[String, WdlValues.V],
                            dxIoFunctions: DxIoFunctions,
                            language : Language.Value): WdlValues.V = {
    // build an object capable of evaluating WDL expressions
    val evalOpts = TypeOptions(typeChecking = wdlTools.util.TypeCheckingRegime.Strict,
                                         antlr4Trace = false,
                                         localDirectories = Vector.empty,
                                         verbosity = wdlTools.util.Verbosity.Quiet)
    val evalCfg = EvalConfig(dxIoFunctions.config.homeDir,
                             dxIoFunctions.config.tmpDir,
                             dxIoFunctions.config.stdout,
                             dxIoFunctions.config.stderr)
    val evaluator = new WdlExprEval(evalOpts, evalCfg, Language.toWdlVersion(language), None)

    evaluator.applyExpr(expr, EvalContext(env))
  }

  it should "second block in a linear workflow" in {
    val source: Path = pathFromBasename("frag_runner", "wf_linear.wdl")
    val (dxPathConfig, dxIoFunctions) = setup()

    val (language, womBundle, _, _) =
      ParseWomSourceFile(false).apply(source, List.empty)

    val wf: TAT.Workflow = womBundle.primaryCallable match {
      case Some(wf: TAT.Workflow) => wf
      case _                            => throw new Exception("sanity")
    }
    val subBlocks = Block.splitWorkflow(wf)

    val block = subBlocks(1)

    val env: Map[String, WdlValues.V] = Map("x" -> WdlValues.V_Int(3),
                                            "y" -> WdlValues.V_Int(5),
                                            "add.result" -> WdlValues.V_Int(8))

    val decls: Vector[TAT.Declaration] = block.nodes.collect {
      case eNode: TAT.Declaration => eNode
    }
    val expr: TAT.Expr = decls.head.expr.get
    val value: WdlValues.V = evaluateWomExpression(expr, env, dxIoFunctions, language)
    value should be(WdlValues.V_Int(9))
  }

  it should "evaluate a scatter without a call" in {
    val path = pathFromBasename("frag_runner", "scatter_no_call.wdl")
    val wfSourceCode = Utils.readFileContent(path)

    val (dxPathConfig, dxIoFunctions) = setup()
    val (wf, fragRunner) = setupFragRunner(dxPathConfig, dxIoFunctions, wfSourceCode)
    val subBlocks = Block.splitWorkflow(wf)
    val block = subBlocks(0)

    val env = Map.empty[String, (WdlTypes.T, WdlValues.V)]
    val results: Map[String, (WdlTypes.T, WdlValues.V)] = fragRunner.evalExpressions(block.nodes, env)
    results.keys should be(Set("names", "full_name"))
    results should be(
        Map(
          "names" -> (WdlTypes.T_Array(WdlTypes.T_String, false),
                      WdlValues.V_Array(
                                Vector(WdlValues.V_String("Michael"),
                                       WdlValues.V_String("Lukas"),
                                       WdlValues.V_String("Martin"),
                                       WdlValues.V_String("Shelly"),
                                       WdlValues.V_String("Amy")))),
          "full_name" ->
            (WdlTypes.T_Array(WdlTypes.T_String, false),
             WdlValues.V_Array(
               Vector(WdlValues.V_String("Michael_Manhaim"),
                      WdlValues.V_String("Lukas_Manhaim"),
                      WdlValues.V_String("Martin_Manhaim"),
                      WdlValues.V_String("Shelly_Manhaim"),
                      WdlValues.V_String("Amy_Manhaim")))
            )))
  }

  it should "evaluate a conditional without a call" in {
    val path = pathFromBasename("frag_runner", "conditional_no_call.wdl")
    val wfSourceCode = Utils.readFileContent(path)

    val (dxPathConfig, dxIoFunctions) = setup()
    val (wf, fragRunner) = setupFragRunner(dxPathConfig, dxIoFunctions, wfSourceCode)
    val subBlocks = Block.splitWorkflow(wf)
    val block = subBlocks.head

    val env = Map.empty[String, (WdlTypes.T, WdlValues.V)]
    val results: Map[String, (WdlTypes.T, WdlValues.V)] = fragRunner.evalExpressions(block.nodes, env)
    results should be(
      Map("flag" -> (WdlTypes.T_Boolean,
                     WdlValues.V_Boolean(true)),
          "cats" -> (WdlTypes.T_Optional(WdlTypes.T_String),
                     WdlValues.V_Optional(WdlValues.V_String("Mr. Baggins")))
      ))
  }

  it should "evaluate a nested conditional/scatter without a call" in {
    val path = pathFromBasename("frag_runner", "nested_no_call.wdl")
    val wfSourceCode = Utils.readFileContent(path)

    val (dxPathConfig, dxIoFunctions) = setup()
    val (wf, fragRunner) = setupFragRunner(dxPathConfig, dxIoFunctions, wfSourceCode)
    val subBlocks = Block.splitWorkflow(wf)

    val block = subBlocks.head
    val results = fragRunner.evalExpressions(block.nodes, Map.empty[String, (WdlTypes.T, WdlValues.V)])
    results should be(Map("z" -> (WdlTypes.T_Optional(WdlTypes.T_Array(WdlTypes.T_Int, false)),
                                  WdlValues.V_Null)))
  }

  it should "create proper names for scatter results" in {
    val path = pathFromBasename("frag_runner", "strings.wdl")
    val wfSourceCode = Utils.readFileContent(path)
    val (wf, _, _, _) = ParseWomSourceFile(false).parseWdlWorkflow(wfSourceCode)

    val scatters = wf.body.collect {
      case x : TAT.Scatter => x
    }
    scatters.size shouldBe(1)
    val scatterNode = scatters.head

    scatterNode.identifier should be("x")
  }

  it should "Make sure calls cannot be handled by evalExpressions" in {
    val path = pathFromBasename("draft2", "shapes.wdl")
    val wfSourceCode = Utils.readFileContent(path)

    val (dxPathConfig, dxIoFunctions) = setup()
    val (wf, fragRunner) = setupFragRunner(dxPathConfig, dxIoFunctions, wfSourceCode)
    val subBlocks = Block.splitWorkflow(wf)

    // Make sure an exception is thrown if eval-expressions is called with
    // a wdl-call.
    assertThrows[Exception] {
      fragRunner.evalExpressions(subBlocks(1).nodes, Map.empty[String, (WdlTypes.T, WdlValues.V)])
    }
  }

  it should "handles draft2" in {
    val path = pathFromBasename("draft2", "conditionals2.wdl")
    val wfSourceCode = Utils.readFileContent(path)

    val (dxPathConfig, dxIoFunctions) = setup()
    val (wf, fragRunner) = setupFragRunner(dxPathConfig, dxIoFunctions, wfSourceCode)
    val subBlocks = Block.splitWorkflow(wf)
    /*
        val results = fragRunner.evalExpressions(subBlocks(0).nodes,
                                                 Map.empty[String, WomValue])
 Utils.ignore(results)*/
    Utils.ignore(subBlocks)
  }

  it should "evaluate expressions that define variables" in {
    val path = pathFromBasename("draft2", "conditionals3.wdl")
    val wfSourceCode = Utils.readFileContent(path)

    val (dxPathConfig, dxIoFunctions) = setup()
    val (wf, fragRunner) = setupFragRunner(dxPathConfig, dxIoFunctions, wfSourceCode)
    val subBlocks = Block.splitWorkflow(wf)

    val results = fragRunner.evalExpressions(subBlocks(0).nodes,
                                             Map.empty[String, (WdlTypes.T, WdlValues.V)])
    results.keys should be(Set("powers10", "i1", "i2", "i3"))
    results("i1") should be((WdlTypes.T_Int,
                             WdlValues.V_Optional(WdlValues.V_Int(1))))
    results("i2") should be((WdlTypes.T_Int,
                             WdlValues.V_Null))
    results("i3") should be((WdlTypes.T_Int,
                             WdlValues.V_Optional(WdlValues.V_Int(100))))
    results("powers10") should be(
      (WdlTypes.T_Array(WdlTypes.T_Optional(WdlTypes.T_Int), false),
       WdlValues.V_Array(
         Vector(WdlValues.V_Optional(WdlValues.V_Int(1)),
                WdlValues.V_Null,
                WdlValues.V_Optional(WdlValues.V_Int(100)))
       )))
  }

  // find the call by recursively searching the workflow nodes and nested blocks
  private def findCallByName(callName: String,
                             allNodes : Vector[TAT.WorkflowElement]): TAT.Call = {
    def f(nodes : Vector[TAT.WorkflowElement]) : Option[TAT.Call] = {
      nodes.foldLeft(None : Option[TAT.Call]) {
      case (Some(call), _) =>
        Some(call)
      case (None, call : TAT.Call) if call.fullyQualifiedName == callName =>
        Some(call)
      case (None, call : TAT.Call) =>
        None
      case (None, decl : TAT.Declaration) =>
        None
      case (None, cond : TAT.Conditional) =>
          f(cond.body)
      case (None, scatter : TAT.Scatter) =>
          f(scatter.body)
      }
    }
    f(allNodes) match {
      case None              => throw new Exception(s"call ${callName} not found")
      case Some(x) => x
    }
  }

  it should "evaluate call inputs properly" in {
    val path = pathFromBasename("draft2", "various_calls.wdl")
    val wfSourceCode = Utils.readFileContent(path)
    val (dxPathConfig, dxIoFunctions) = setup()
    val (wf, fragRunner) = setupFragRunner(dxPathConfig, dxIoFunctions, wfSourceCode)

    val call1 = findCallByName("MaybeInt", wf.body)
    val callInputs1: Map[String, (WdlTypes.T, WdlValues.V)] =
      fragRunner.evalCallInputs(call1, Map("i" -> (WdlTypes.T_Int, WdlValues.V_Int(1))))
    callInputs1 should be(Map("a" -> (WdlTypes.T_Int,
                                      WdlValues.V_Optional(WdlValues.V_Int(1)))
                          ))

    val call2 = findCallByName("ManyArgs", wf.body)
    val callInputs2: Map[String, (WdlTypes.T, WdlValues.V)] =
      fragRunner.evalCallInputs(call2,
                                Map("powers10" -> (WdlTypes.T_Array(WdlTypes.T_Int, false),
                                                   WdlValues.V_Array(
                                                     Vector(WdlValues.V_Int(1), WdlValues.V_Int(10))))
                                ))

    callInputs2 should be(
      Map("a" -> (WdlTypes.T_String,
                  WdlValues.V_String("hello")),
          "b" -> (WdlTypes.T_Array(WdlTypes.T_Int),
                  WdlValues.V_Array(Vector(WdlValues.V_Int(1), WdlValues.V_Int(10))))
      ))
  }

  it should "evaluate call constant inputs" in {
    val path = pathFromBasename("nested", "two_levels.wdl")
    val wfSourceCode = Utils.readFileContent(path)
    val (dxPathConfig, dxIoFunctions) = setup()
    val (wf, fragRunner) = setupFragRunner(dxPathConfig, dxIoFunctions, wfSourceCode)

    val inc4Call = findCallByName("inc5", wf.body)

    val args = fragRunner.evalCallInputs(inc4Call, Map.empty)
    args shouldBe (Map("a" -> (WdlTypes.T_Int, WdlValues.V_Int(3))))
  }

  it should "expressions with structs" in {
    val path = pathFromBasename("frag_runner", "House.wdl")
    val wfSourceCode = Utils.readFileContent(path)

    val (dxPathConfig, dxIoFunctions) = setup()
    val (wf, fragRunner) = setupFragRunner(dxPathConfig, dxIoFunctions, wfSourceCode)
    val subBlocks = Block.splitWorkflow(wf)

    val results = fragRunner.evalExpressions(subBlocks(0).nodes,
                                             Map.empty[String, (WdlTypes.T, WdlValues.V)])
    results.keys should be(
        Set("a", "b", "tot_height", "tot_num_floors", "streets", "cities", "tot")
    )
    results("tot") should be(
      (WdlTypes.T_Struct("House",
                         Map("height" -> WdlTypes.T_Int,
                             "num_floors" -> WdlTypes.T_Int,
                             "street" -> WdlTypes.T_String,
                             "city" -> WdlTypes.T_String)),
       WdlValues.V_Struct("House",
            Map("height" -> WdlValues.V_Int(32),
                "num_floors" -> WdlValues.V_Int(4),
                "street" -> WdlValues.V_String("Alda_Mary"),
                "city" -> WdlValues.V_String("Sunnyvale_Santa Clara")))
      ))
  }

  it should "fill in missing optionals" in {
    val path = pathFromBasename("frag_runner", "missing_args.wdl")
    val wfSourceCode = Utils.readFileContent(path)

    val (dxPathConfig, dxIoFunctions) = setup()
    val (wf, fragRunner) = setupFragRunner(dxPathConfig, dxIoFunctions, wfSourceCode)
    val results: Map[String, JsValue] =
      fragRunner.apply(Vector(0),
                       Map("y" -> (WdlTypes.T_Int, WdlValues.V_Int(5))),
                       RunnerWfFragmentMode.Launch)
    results shouldBe (Map("retval" -> JsNumber(5)))
  }

  it should "evaluate expressions in correct order" taggedAs (EdgeTest) in {
    val path = pathFromBasename("frag_runner", "scatter_variable_not_found.wdl")
    val wfSourceCode = Utils.readFileContent(path)

    val (dxPathConfig, dxIoFunctions) = setup()
    val (wf, fragRunner) = setupFragRunner(dxPathConfig, dxIoFunctions, wfSourceCode)
    val results: Map[String, JsValue] =
      fragRunner.apply(Vector(0), Map.empty, RunnerWfFragmentMode.Launch)
    results.keys should contain("bam_lane1")
    results("bam_lane1") shouldBe (JsObject("___" -> JsArray(JsString("1_ACGT_1.bam"), JsNull)))
  }
}
