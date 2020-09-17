package dx.executor.wdl

import java.nio.file.{Files, Path, Paths}

import dx.Assumptions.isLoggedIn
import dx.Tags.{EdgeTest, NativeTest}
import dx.api._
import dx.core.Native
import dx.core.io.{DxFileAccessProtocol, DxWorkerPaths}
import dx.core.ir.ParameterLinkSerializer
import dx.core.languages.wdl.{WdlBlock, Utils => WdlUtils}
import dx.core.util.CompressionUtils
import dx.executor.{JobMeta, WorkflowAction, WorkflowExecutor}
import dx.translator.wdl.WdlBundle
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import spray.json._
import wdlTools.eval.{Eval, EvalPaths, WdlValueBindings, WdlValues}
import wdlTools.syntax.WdlVersion
import wdlTools.types.{WdlTypes, TypedAbstractSyntax => TAT}
import wdlTools.util.{FileSourceResolver, FileUtils, Logger}

private case class WorkflowTestJobMeta(override val homeDir: Path = DxWorkerPaths.HomeDir,
                                       override val dxApi: DxApi = DxApi.get,
                                       override val logger: Logger = Logger.get,
                                       rawEnv: Map[String, (WdlTypes.T, WdlValues.V)],
                                       rawBlockPath: Vector[Int],
                                       rawInstanceTypeDb: InstanceTypeDB,
                                       rawSourceCode: String)
    extends JobMeta(homeDir, dxApi, logger) {
  override val project: DxProject = null

  override val jsInputs: Map[String, JsValue] =
    ParameterLinkSerializer().createFieldsFromMap(WdlUtils.toIR(rawEnv))

  override def writeJsOutputs(outputJs: Map[String, JsValue]): Unit = {}

  override val jobId: String = null

  override val analysis: Option[DxAnalysis] = None

  override val parentJob: Option[DxJob] = None

  override val instanceType: Option[String] = Some(WorkflowTestJobMeta.InstanceType)

  override def getJobDetail(name: String): Option[JsValue] = None

  private val executableDetails: Map[String, JsValue] = Map(
      Native.BlockPath -> JsArray(rawBlockPath.map(JsNumber(_))),
      Native.InstanceTypeDb -> JsString(
          CompressionUtils.gzipAndBase64Encode(
              instanceTypeDb.toJson.prettyPrint
          )
      ),
      Native.SourceCode -> JsString(CompressionUtils.gzipAndBase64Encode(rawSourceCode))
  )

  override def getExecutableDetail(name: String): Option[JsValue] = executableDetails.get(name)

  override def error(e: Throwable): Unit = {}
}

private object WorkflowTestJobMeta {
  val InstanceType: String = "mem_ssd_unicorn"
}

// This test module requires being logged in to the platform.
// It compiles WDL scripts without the runtime library.
// This tests the compiler Native mode, however, it creates
// dnanexus applets and workflows that are not runnable.
class WfFragRunnerTest extends AnyFlatSpec with Matchers {
  assume(isLoggedIn)
  private val logger = Logger.Quiet
  private val dxApi = DxApi(logger)
  private val unicornInstance =
    DxInstanceType(WorkflowTestJobMeta.InstanceType,
                   100,
                   100,
                   4,
                   gpu = false,
                   Vector(("Ubuntu", "16.04")),
                   Some(DiskType.SSD),
                   Some(1.00f))
  private val instanceTypeDB = InstanceTypeDB(pricingAvailable = true, Vector(unicornInstance))

  private def setup(): DxWorkerPaths = {
    // Create a clean temp directory for the task to use
    val jobHomeDir: Path = Files.createTempDirectory("dxwdl_applet_test")
    val workerPaths = DxWorkerPaths(jobHomeDir)
    workerPaths.createCleanDirs()
    workerPaths
  }

  private def createWorkflowExecutor(
      workerPaths: DxWorkerPaths,
      sourcePath: Path,
      blockPath: Vector[Int] = Vector(0),
      env: Map[String, (WdlTypes.T, WdlValues.V)] = Map.empty
  ): WorkflowExecutor = {
    val wfSourceCode = FileUtils.readFileContent(sourcePath)
    val jobMeta =
      WorkflowTestJobMeta(workerPaths.homeDir,
                          dxApi,
                          logger,
                          env,
                          blockPath,
                          instanceTypeDB,
                          wfSourceCode)
    WorkflowExecutor(jobMeta, Some(workerPaths))
  }

  private def createFileResolver(workerPaths: DxWorkerPaths): FileSourceResolver = {
    val dxProtocol = DxFileAccessProtocol(dxApi)
    FileSourceResolver.create(
        localDirectories = Vector(workerPaths.homeDir),
        userProtocols = Vector(dxProtocol),
        logger = logger
    )
  }

  private def createEvaluator(workerPaths: DxWorkerPaths,
                              wdlVersion: WdlVersion,
                              fileResolver: FileSourceResolver): Eval = {
    val evalPaths = EvalPaths(workerPaths.homeDir, workerPaths.tmpDir)
    Eval(evalPaths, Some(wdlVersion), fileResolver, logger)
  }

  // Note: if the file doesn't exist, this throws a null pointer exception
  private def pathFromBasename(dir: String, basename: String): Path = {
    Paths.get(getClass.getResource(s"/${dir}/${basename}").getPath)
  }

  private def parse(path: Path): WdlBundle = {
    val (doc, _) = WdlUtils.parseSourceFile(path)
    WdlBundle.flattenDepthFirst(doc)
  }

  it should "second block in a linear workflow" in {
    val workerPaths = setup()
    val path = pathFromBasename("frag_runner", "wf_linear.wdl")
    val wdlBundle = parse(path)
    val wf: TAT.Workflow = wdlBundle.primaryCallable match {
      case Some(wf: TAT.Workflow) => wf
      case _                      => throw new Exception("unexpected")
    }
    val subBlocks = WdlBlock.createBlocks(wf.body)
    val block = subBlocks(1)
    val env: Map[String, WdlValues.V] = Map(
        "x" -> WdlValues.V_Int(3),
        "y" -> WdlValues.V_Int(5),
        "add" -> WdlValues.V_Call("add", Map("result" -> WdlValues.V_Int(8)))
    )
    val decls: Vector[TAT.Declaration] = block.elements.collect {
      case eNode: TAT.Declaration => eNode
    }
    val expr: TAT.Expr = decls.head.expr.get
    val fileResolver = createFileResolver(workerPaths)
    val eval: Eval = createEvaluator(workerPaths, wdlBundle.version, fileResolver)
    val value: WdlValues.V = eval.applyExpr(expr, WdlValueBindings(env))
    value should be(WdlValues.V_Int(9))
  }

  it should "evaluate a scatter without a call" in {
    val workerPaths = setup()
    val path = pathFromBasename("frag_runner", "scatter_no_call.wdl")
    val wdlBundle = parse(path)
    val wf: TAT.Workflow = wdlBundle.primaryCallable match {
      case Some(wf: TAT.Workflow) => wf
      case _                      => throw new Exception("unexpected")
    }
    val subBlocks = WdlBlock.createBlocks(wf.body)
    val block = subBlocks(0)

    val wfExecutor = createWorkflowExecutor(workerPaths, path)
    val env = Map.empty[String, (WdlTypes.T, WdlValues.V)]
    val results: Map[String, (WdlTypes.T, WdlValues.V)] = wfExecutor.workflowSupport match {
      case supp: WdlWorkflowSupport => supp.evaluateWorkflowElements(block.elements, env)
      case _                        => throw new Exception("expected WdlWorkflowSupport")
    }
    results.keys should be(Set("names", "full_name"))
    results should be(
        Map(
            "names" -> (WdlTypes.T_Array(WdlTypes.T_String, nonEmpty = false),
            WdlValues.V_Array(
                Vector(WdlValues.V_String("Michael"),
                       WdlValues.V_String("Lukas"),
                       WdlValues.V_String("Martin"),
                       WdlValues.V_String("Shelly"),
                       WdlValues.V_String("Amy"))
            )),
            "full_name" ->
              (WdlTypes.T_Array(WdlTypes.T_String, nonEmpty = false),
              WdlValues.V_Array(
                  Vector(
                      WdlValues.V_String("Michael_Manhaim"),
                      WdlValues.V_String("Lukas_Manhaim"),
                      WdlValues.V_String("Martin_Manhaim"),
                      WdlValues.V_String("Shelly_Manhaim"),
                      WdlValues.V_String("Amy_Manhaim")
                  )
              ))
        )
    )
  }

  it should "evaluate a conditional without a call" in {
    val workerPaths = setup()
    val path = pathFromBasename("frag_runner", "conditional_no_call.wdl")
    val wdlBundle = parse(path)
    val wf: TAT.Workflow = wdlBundle.primaryCallable match {
      case Some(wf: TAT.Workflow) => wf
      case _                      => throw new Exception("unexpected")
    }
    val subBlocks = WdlBlock.createBlocks(wf.body)
    val block = subBlocks.head
    val wfExecutor = createWorkflowExecutor(workerPaths, path)
    val env = Map.empty[String, (WdlTypes.T, WdlValues.V)]
    val results: Map[String, (WdlTypes.T, WdlValues.V)] = wfExecutor.workflowSupport match {
      case supp: WdlWorkflowSupport => supp.evaluateWorkflowElements(block.elements, env)
      case _                        => throw new Exception("expected WdlWorkflowSupport")
    }
    results should be(
        Map(
            "flag" -> (WdlTypes.T_Boolean,
            WdlValues.V_Boolean(true)),
            "cats" -> (WdlTypes.T_Optional(WdlTypes.T_String),
            WdlValues.V_Optional(WdlValues.V_String("Mr. Baggins")))
        )
    )
  }

  it should "evaluate a nested conditional/scatter without a call" in {
    val workerPaths = setup()
    val path = pathFromBasename("frag_runner", "nested_no_call.wdl")
    val wdlBundle = parse(path)
    val wf: TAT.Workflow = wdlBundle.primaryCallable match {
      case Some(wf: TAT.Workflow) => wf
      case _                      => throw new Exception("unexpected")
    }
    val subBlocks = WdlBlock.createBlocks(wf.body)
    val block = subBlocks.head
    val wfExecutor = createWorkflowExecutor(workerPaths, path)
    val results: Map[String, (WdlTypes.T, WdlValues.V)] = wfExecutor.workflowSupport match {
      case supp: WdlWorkflowSupport =>
        supp.evaluateWorkflowElements(block.elements, Map.empty[String, (WdlTypes.T, WdlValues.V)])
      case _ => throw new Exception("expected WdlWorkflowSupport")
    }
    results should be(
        Map(
            "z" -> (WdlTypes.T_Optional(WdlTypes.T_Array(WdlTypes.T_Int, nonEmpty = false)),
            WdlValues.V_Null)
        )
    )
  }

  it should "create proper names for scatter results" in {
    val path = pathFromBasename("frag_runner", "strings.wdl")
    val wdlBundle = parse(path)
    val wf: TAT.Workflow = wdlBundle.primaryCallable match {
      case Some(wf: TAT.Workflow) => wf
      case _                      => throw new Exception("unexpected")
    }
    val scatters = wf.body.collect {
      case x: TAT.Scatter => x
    }
    scatters.size shouldBe 1
    val scatterNode = scatters.head
    scatterNode.identifier should be("x")
  }

  it should "Make sure calls cannot be handled by evalExpressions" in {
    val workerPaths = setup()
    val path = pathFromBasename("draft2", "shapes.wdl")
    val wdlBundle = parse(path)
    val wf: TAT.Workflow = wdlBundle.primaryCallable match {
      case Some(wf: TAT.Workflow) => wf
      case _                      => throw new Exception("unexpected")
    }
    val subBlocks = WdlBlock.createBlocks(wf.body)
    val wfExecutor = createWorkflowExecutor(workerPaths, path)
    val wdlWorkflowSupport = wfExecutor.workflowSupport match {
      case supp: WdlWorkflowSupport => supp
      case _                        => throw new Exception("expected WdlWorkflowSupport")
    }
    // Make sure an exception is thrown if eval-expressions is called with
    // a wdl-call.
    assertThrows[Exception] {
      wdlWorkflowSupport.evaluateWorkflowElements(subBlocks(1).elements,
                                                  Map.empty[String, (WdlTypes.T, WdlValues.V)])
    }
  }

  it should "evaluate expressions that define variables" in {
    val workerPaths = setup()
    val path = pathFromBasename("draft2", "conditionals3.wdl")
    val wdlBundle = parse(path)
    val wf: TAT.Workflow = wdlBundle.primaryCallable match {
      case Some(wf: TAT.Workflow) => wf
      case _                      => throw new Exception("unexpected")
    }
    val subBlocks = WdlBlock.createBlocks(wf.body)
    val wfExecutor = createWorkflowExecutor(workerPaths, path)
    val results: Map[String, (WdlTypes.T, WdlValues.V)] = wfExecutor.workflowSupport match {
      case supp: WdlWorkflowSupport =>
        supp.evaluateWorkflowElements(subBlocks(0).elements,
                                      Map.empty[String, (WdlTypes.T, WdlValues.V)])
      case _ => throw new Exception("expected WdlWorkflowSupport")
    }
    results.keys should be(Set("powers10", "i1", "i2", "i3"))
    results("i1") should be(
        (WdlTypes.T_Optional(WdlTypes.T_Int), WdlValues.V_Optional(WdlValues.V_Int(1)))
    )
    results("i2") should be((WdlTypes.T_Optional(WdlTypes.T_Int), WdlValues.V_Null))
    results("i3") should be(
        (WdlTypes.T_Optional(WdlTypes.T_Int), WdlValues.V_Optional(WdlValues.V_Int(100)))
    )
    results("powers10") should be(
        (WdlTypes.T_Array(WdlTypes.T_Optional(WdlTypes.T_Int), nonEmpty = false),
         WdlValues.V_Array(
             Vector(WdlValues.V_Optional(WdlValues.V_Int(1)),
                    WdlValues.V_Null,
                    WdlValues.V_Optional(WdlValues.V_Int(100)))
         ))
    )
  }

  // find the call by recursively searching the workflow nodes and nested blocks
  private def findCallByName(callName: String, allNodes: Vector[TAT.WorkflowElement]): TAT.Call = {
    def f(nodes: Vector[TAT.WorkflowElement]): Option[TAT.Call] = {
      nodes.foldLeft(None: Option[TAT.Call]) {
        case (Some(call), _) =>
          Some(call)
        case (None, call: TAT.Call) if call.actualName == callName =>
          Some(call)
        case (None, _: TAT.Call) =>
          None
        case (None, _: TAT.Declaration) =>
          None
        case (None, cond: TAT.Conditional) =>
          f(cond.body)
        case (None, scatter: TAT.Scatter) =>
          f(scatter.body)
      }
    }
    f(allNodes) match {
      case None    => throw new Exception(s"call ${callName} not found")
      case Some(x) => x
    }
  }

  it should "evaluate call inputs properly" in {
    val workerPaths = setup()
    val path = pathFromBasename("draft2", "various_calls.wdl")
    val wdlBundle = parse(path)
    val wf: TAT.Workflow = wdlBundle.primaryCallable match {
      case Some(wf: TAT.Workflow) => wf
      case _                      => throw new Exception("unexpected")
    }
    val wfExecutor = createWorkflowExecutor(workerPaths, path)
    val call1 = findCallByName("MaybeInt", wf.body)
    val wfSupport = wfExecutor.workflowSupport match {
      case supp: WdlWorkflowSupport => supp
      case _                        => throw new Exception("expected WdlWorkflowSupport")
    }
    val callInputs1: Map[String, (WdlTypes.T, WdlValues.V)] =
      wfSupport.evaluateWorkflowElements(Vector(call1),
                                         Map("i" -> (WdlTypes.T_Int, WdlValues.V_Int(1))))
    // We need to coerce the inputs into what the callee is expecting
    callInputs1 should be(
        Map(
            "a" -> (WdlTypes.T_Optional(WdlTypes.T_Int),
            WdlValues.V_Optional(WdlValues.V_Int(1)))
        )
    )

    val call2 = findCallByName("ManyArgs", wf.body)
    val callInputs2: Map[String, (WdlTypes.T, WdlValues.V)] =
      wfSupport.evaluateWorkflowElements(
          Vector(call2),
          Map(
              "powers10" -> (WdlTypes.T_Array(WdlTypes.T_Int, nonEmpty = false),
              WdlValues.V_Array(Vector(WdlValues.V_Int(1), WdlValues.V_Int(10))))
          )
      )

    callInputs2 should be(
        Map(
            "a" -> (WdlTypes.T_String,
            WdlValues.V_String("hello")),
            "b" -> (WdlTypes.T_Array(WdlTypes.T_Int),
            WdlValues.V_Array(Vector(WdlValues.V_Int(1), WdlValues.V_Int(10))))
        )
    )
  }

  it should "evaluate call constant inputs" in {
    val workerPaths = setup()
    val path = pathFromBasename("nested", "two_levels.wdl")
    val wdlBundle = parse(path)
    val wf: TAT.Workflow = wdlBundle.primaryCallable match {
      case Some(wf: TAT.Workflow) => wf
      case _                      => throw new Exception("unexpected")
    }
    val wfExecutor = createWorkflowExecutor(workerPaths, path)
    val zincCall = findCallByName("zincWithNoParams", wf.body)
    val wfSupport = wfExecutor.workflowSupport match {
      case supp: WdlWorkflowSupport => supp
      case _                        => throw new Exception("expected WdlWorkflowSupport")
    }
    val args = wfSupport.WdlBlockContext.evaluateCallInputs(zincCall, Map.empty)
    args shouldBe Map.empty // ("a" -> (WdlTypes.T_Int, WdlValues.V_Int(3)))
  }

  it should "expressions with structs" in {
    val workerPaths = setup()
    val path = pathFromBasename("frag_runner", "House.wdl")
    val wdlBundle = parse(path)
    val wf: TAT.Workflow = wdlBundle.primaryCallable match {
      case Some(wf: TAT.Workflow) => wf
      case _                      => throw new Exception("unexpected")
    }
    val subBlocks = WdlBlock.createBlocks(wf.body)
    val wfExecutor = createWorkflowExecutor(workerPaths, path)
    val wfSupport = wfExecutor.workflowSupport match {
      case supp: WdlWorkflowSupport => supp
      case _                        => throw new Exception("expected WdlWorkflowSupport")
    }
    val results =
      wfSupport.evaluateWorkflowElements(subBlocks(0).elements,
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
         WdlValues.V_Struct(
             "House",
             Map(
                 "height" -> WdlValues.V_Int(32),
                 "num_floors" -> WdlValues.V_Int(4),
                 "street" -> WdlValues.V_String("Alda_Mary"),
                 "city" -> WdlValues.V_String("Sunnyvale_Santa Clara")
             )
         ))
    )
  }

  it should "fill in missing optionals" taggedAs NativeTest in {
    val workerPaths = setup()
    val path = pathFromBasename("frag_runner", "missing_args.wdl")
    val env = Map(
        "x" -> (WdlTypes.T_Optional(WdlTypes.T_Int), WdlValues.V_Null),
        "y" -> (WdlTypes.T_Int, WdlValues.V_Int(5))
    )
    val wfExecutor = createWorkflowExecutor(workerPaths, path, Vector(0), env)
    val (results, msg) = wfExecutor.apply(WorkflowAction.Run)
    msg shouldBe "success Run"
    results shouldBe Map("retval" -> JsNumber(5))
  }

  it should "evaluate expressions in correct order" taggedAs NativeTest in {
    val workerPaths = setup()
    val path = pathFromBasename("frag_runner", "scatter_variable_not_found.wdl")
    val wfExecutor = createWorkflowExecutor(workerPaths, path)
    val (results, msg) = wfExecutor.apply(WorkflowAction.Run)
    msg shouldBe "success Run"
    results.keys should contain("bam_lane1")
    results("bam_lane1") shouldBe JsObject("___" -> JsArray(JsString("1_ACGT_1.bam"), JsNull))
  }

  it should "handle pair field access (left/right)" taggedAs (NativeTest, EdgeTest) in {
    val path = pathFromBasename("frag_runner", "scatter_with_eval.wdl")
    val workerPaths = setup()
    val wfExecutor = createWorkflowExecutor(workerPaths, path)
    val (results, msg) = wfExecutor.apply(WorkflowAction.Run)
    msg shouldBe "success Run"
    results.keys should contain("info")
    results("info") shouldBe JsArray(JsString("Michael_27"),
                                     JsString("Lukas_9"),
                                     JsString("Martin_13"),
                                     JsString("Shelly_67"),
                                     JsString("Amy_2"))
  }

  it should "Build limited sized names" in {
    WdlWorkflowSupport.getComplexScatterName(Vector(1, 2, 3), 10) should be("[1, 2, 3]")
    WdlWorkflowSupport.getComplexScatterName(Vector(100, 200, 300).map(_.toString), 10) should be(
        "[100, 200]"
    )
    WdlWorkflowSupport.getComplexScatterName(Vector("A", "B", "hel", "nope"), 10) should be(
        "[A, B, hel, ...]"
    )
    WdlWorkflowSupport.getComplexScatterName(Vector("A", "B", "C", "D", "neverland"), 13) should be(
        "[A, B, C, D, ...]"
    )
    WdlWorkflowSupport.getComplexScatterName(Vector.empty, 4) should be("[]")
  }
}
