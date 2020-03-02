package dxWDL.compiler

import java.nio.file.{Path, Paths}
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.Inside._
import wom.callable.CallableTaskDefinition
import wom.callable.MetaValueElement
import wom.types._
import wom.values._
import dxWDL.Main
import dxWDL.base.Utils
import dxWDL.dx._

// These tests involve compilation -without- access to the platform.
//
class GenerateIRTest extends FlatSpec with Matchers {
  private def pathFromBasename(dir: String, basename: String): Path = {
    val p = getClass.getResource(s"/${dir}/${basename}").getPath
    Paths.get(p)
  }

  private val dxProject = {
    val p = DxUtils.dxEnv.getProjectContext()
    if (p == null)
      throw new Exception("Must be logged in to run this test")
    DxProject(p)
  }

  // task compilation
  private val cFlags = List("--compileMode",
                            "ir",
                            "-quiet",
                            "-fatalValidationWarnings",
                            "--locked",
                            "--project",
                            dxProject.getId)

  private val cFlagsUnlocked =
    List("--compileMode", "ir", "-quiet", "-fatalValidationWarnings", "--project", dxProject.getId)

  val dbgFlags = List("--compileMode",
                      "ir",
                      "--verbose",
                      "--verboseKey",
                      "GenerateIR",
                      "--locked",
                      "--project",
                      dxProject.id)

  it should "IR compile a single WDL task" in {
    val path = pathFromBasename("compiler", "add.wdl")
    Main.compile(path.toString :: cFlags) shouldBe a[Main.SuccessfulTerminationIR]
  }

  it should "IR compile a task with docker" in {
    val path = pathFromBasename("compiler", "BroadGenomicsDocker.wdl")
    Main.compile(path.toString :: cFlags) shouldBe a[Main.SuccessfulTerminationIR]
  }

  // workflow compilation
  it should "IR compile a linear WDL workflow without expressions" in {
    val path = pathFromBasename("compiler", "wf_linear_no_expr.wdl")
    Main.compile(path.toString :: cFlags) shouldBe a[Main.SuccessfulTerminationIR]
  }

  it should "IR compile a linear WDL workflow" in {
    val path = pathFromBasename("compiler", "wf_linear.wdl")
    Main.compile(
        path.toString :: cFlags
    ) shouldBe a[Main.SuccessfulTerminationIR]
  }

  it should "IR compile unlocked workflow" in {
    val path = pathFromBasename("compiler", "wf_linear.wdl")
    Main.compile(
        path.toString :: cFlagsUnlocked
    ) shouldBe a[Main.SuccessfulTerminationIR]
  }

  it should "IR compile a non trivial linear workflow with variable coercions" in {
    val path = pathFromBasename("compiler", "cast.wdl")
    Main.compile(path.toString :: cFlags) shouldBe a[Main.SuccessfulTerminationIR]
  }

  it should "IR compile a workflow with two consecutive calls" in {
    val path = pathFromBasename("compiler", "strings.wdl")
    Main.compile(path.toString :: cFlags) shouldBe a[Main.SuccessfulTerminationIR]
  }

  it should "IR compile a workflow with a scatter without a call" in {
    val path = pathFromBasename("compiler", "scatter_no_call.wdl")
    Main.compile(
        path.toString :: cFlags
    ) shouldBe a[Main.SuccessfulTerminationIR]
  }

  it should "IR compile optionals" in {
    val path = pathFromBasename("compiler", "optionals.wdl")
    Main.compile(
        path.toString
//                :: "--verbose"
//                :: "--verboseKey" :: "GenerateIR"
          :: cFlags
    ) shouldBe a[Main.SuccessfulTerminationIR]
  }

  it should "support imports" in {
    val path = pathFromBasename("compiler", "check_imports.wdl")
    Main.compile(
        path.toString :: cFlags
    ) shouldBe a[Main.SuccessfulTerminationIR]
  }

  it should "IR compile a draft2 workflow" in {
    val path = pathFromBasename("draft2", "shapes.wdl")
    val retval = Main.compile(
        path.toString :: cFlags
    )
    retval shouldBe a[Main.SuccessfulTerminationIR]
  }

  it should "expressions in an output block" in {
    val path = pathFromBasename("compiler", "expr_output_block.wdl")
    Main.compile(
        path.toString :: cFlags
    ) shouldBe a[Main.SuccessfulTerminationIR]
  }

  it should "scatters over maps" in {
    val path = pathFromBasename("compiler", "dict2.wdl")
    Main.compile(
        path.toString :: cFlags
    ) shouldBe a[Main.SuccessfulTerminationIR]
  }

  it should "skip missing optional arguments" in {
    val path = pathFromBasename("util", "missing_inputs_to_direct_call.wdl")
    Main.compile(
        path.toString :: cFlags
    ) shouldBe a[Main.SuccessfulTerminationIR]
  }

  it should "handle calling subworkflows" in {
    val path = pathFromBasename("subworkflows", "trains.wdl")
    val retval = Main.compile(
        path.toString :: cFlags
    )
    retval shouldBe a[Main.SuccessfulTerminationIR]
    val irwf = retval match {
      case Main.SuccessfulTerminationIR(irwf) => irwf
      case _                                  => throw new Exception("sanity")
    }
    val primaryWf: IR.Workflow = irwf.primaryCallable match {
      case Some(wf: IR.Workflow) => wf
      case _                     => throw new Exception("sanity")
    }
    primaryWf.stages.size shouldBe (2)
  }

  it should "compile a sub-block with several calls" in {
    val path = pathFromBasename("compiler", "subblock_several_calls.wdl")
    Main.compile(
        path.toString :: cFlags
    ) shouldBe a[Main.SuccessfulTerminationIR]
  }

  it should "missing workflow inputs" in {
    val path = pathFromBasename("input_file", "missing_args.wdl")
    Main.compile(
        path.toString :: List("--compileMode", "ir", "--quiet", "--project", dxProject.id)
    ) shouldBe a[Main.SuccessfulTerminationIR]
  }

  // Nested blocks
  it should "compile two level nested workflow" in {
    val path = pathFromBasename("nested", "two_levels.wdl")
    Main.compile(
        path.toString :: cFlags
    ) shouldBe a[Main.SuccessfulTerminationIR]
  }

  it should "handle passing closure arguments to nested blocks" in {
    val path = pathFromBasename("nested", "param_passing.wdl")
    Main.compile(
        path.toString :: cFlags
    ) shouldBe a[Main.SuccessfulTerminationIR]
  }

  it should "compile a workflow calling a subworkflow as a direct call" in {
    val path = pathFromBasename("draft2", "movies.wdl")
    val bundle: IR.Bundle = Main.compile(path.toString :: cFlags) match {
      case Main.SuccessfulTerminationIR(bundle) => bundle
      case other =>
        Utils.error(other.toString)
        throw new Exception(s"Failed to compile ${path}")
    }
    val wf: IR.Workflow = bundle.primaryCallable match {
      case Some(wf: IR.Workflow) =>
        wf
      case _ => throw new Exception("bad value in bundle")
    }
    val stage = wf.stages.head
    stage.description shouldBe ("review")
  }

  it should "compile a workflow calling a subworkflow as a direct call with development version" in {
    val path = pathFromBasename("development", "movies.wdl")
    val bundle: IR.Bundle = Main.compile(path.toString :: cFlags) match {
      case Main.SuccessfulTerminationIR(bundle) => bundle
      case other =>
        Utils.error(other.toString)
        throw new Exception(s"Failed to compile ${path}")
    }
    val wf: IR.Workflow = bundle.primaryCallable match {
      case Some(wf: IR.Workflow) =>
        wf
      case _ => throw new Exception("bad value in bundle")
    }
    val stage = wf.stages.head
    stage.description shouldBe ("review")
  }

  it should "compile a workflow calling a subworkflow with native DNANexus applet as a direct call with development version" in {
    val path = pathFromBasename("development", "call_dnanexus_applet.wdl")
    val bundle: IR.Bundle = Main.compile(path.toString :: cFlags) match {
      case Main.SuccessfulTerminationIR(bundle) => bundle
      case other =>
        Utils.error(other.toString)
        throw new Exception(s"Failed to compile ${path}")
    }
    val wf: IR.Workflow = bundle.primaryCallable match {
      case Some(wf: IR.Workflow) =>
        wf
      case _ => throw new Exception("bad value in bundle")
    }
    val stage = wf.stages.head
    stage.description shouldBe ("native_sum_wf")
  }

  it should "three nesting levels" in {
    val path = pathFromBasename("nested", "three_levels.wdl")
    val retval = Main.compile(
        path.toString :: cFlags
    )
    retval shouldBe a[Main.SuccessfulTerminationIR]
    val bundle = retval match {
      case Main.SuccessfulTerminationIR(ir) => ir
      case _                                => throw new Exception("sanity")
    }
    val primary: IR.Callable = bundle.primaryCallable.get
    val wf = primary match {
      case wf: IR.Workflow => wf
      case _               => throw new Exception("sanity")
    }
    wf.stages.size shouldBe (1)

    val level2 = bundle.allCallables(wf.name)
    level2 shouldBe a[IR.Workflow]
    val wfLevel2 = level2.asInstanceOf[IR.Workflow]
    wfLevel2.stages.size shouldBe (1)
  }

  it should "four nesting levels" in {
    val path = pathFromBasename("nested", "four_levels.wdl")
    val retval = Main.compile(
        path.toString :: cFlags
    )
    /*        inside(retval) {
            case Main.UnsuccessfulTermination(errMsg) =>
                errMsg should include ("nested scatter")
 }*/
    retval shouldBe a[Main.SuccessfulTerminationIR]
  }

  private def getAppletByName(name: String, bundle: IR.Bundle): IR.Applet =
    bundle.allCallables(name) match {
      case a: IR.Applet => a
      case _            => throw new Exception(s"${name} is not an applet")
    }

  private def getTaskByName(
      name: String,
      bundle: IR.Bundle
  ): CallableTaskDefinition = {
    val applet = getAppletByName(name, bundle)
    val task: CallableTaskDefinition = applet.kind match {
      case IR.AppletKindTask(x) => x
      case _                    => throw new Exception(s"${name} is not a task")
    }
    task
  }

  // Check parameter_meta `pattern` keyword
  it should "recognize pattern in parameters_meta via CVar for input CVars" in {
    val path = pathFromBasename("compiler", "pattern_params.wdl")
    val retval = Main.compile(
        path.toString :: cFlags
    )
    retval shouldBe a[Main.SuccessfulTerminationIR]
    val bundle = retval match {
      case Main.SuccessfulTerminationIR(ir) => ir
      case _                                => throw new Exception("sanity")
    }

    val cgrepApplet = getAppletByName("pattern_params_cgrep", bundle)
    cgrepApplet.inputs shouldBe Vector(
        IR.CVar(
            "in_file",
            WomSingleFileType,
            None,
            Some(
                Vector(
                    IR.IOAttrHelp("The input file to be searched"),
                    IR.IOAttrPatterns(IR.PatternsReprArray(Vector("*.txt", "*.tsv"))),
                    IR.IOAttrGroup("Common"),
                    IR.IOAttrLabel("Input file")
                )
            )
        ),
        IR.CVar(
            "pattern",
            WomStringType,
            None,
            Some(
                Vector(
                    IR.IOAttrHelp("The pattern to use to search in_file"),
                    IR.IOAttrGroup("Common"),
                    IR.IOAttrLabel("Search pattern")
                )
            )
        )
    )
    cgrepApplet.outputs shouldBe Vector(
        IR.CVar("count", WomIntegerType, None, None),
        IR.CVar(
            "out_file",
            WomSingleFileType,
            None,
            None
        )
    )
  }

  // Check parameter_meta `pattern` keyword
  it should "recognize pattern object in parameters_obj_meta via CVar for input CVars" in {
    val path = pathFromBasename("compiler", "pattern_obj_params.wdl")
    val retval = Main.compile(
        path.toString :: cFlags
    )
    retval shouldBe a[Main.SuccessfulTerminationIR]
    val bundle = retval match {
      case Main.SuccessfulTerminationIR(ir) => ir
      case _                                => throw new Exception("sanity")
    }

    val cgrepApplet = getAppletByName("pattern_params_obj_cgrep", bundle)
    cgrepApplet.inputs shouldBe Vector(
        IR.CVar(
            "in_file",
            WomSingleFileType,
            None,
            Some(
                Vector(
                    IR.IOAttrHelp("The input file to be searched"),
                    IR.IOAttrPatterns(
                        IR.PatternsReprObj(
                            Some(Vector("*.txt", "*.tsv")),
                            Some("file"),
                            Some(Vector("foo", "bar"))
                        )
                    ),
                    IR.IOAttrGroup("Common"),
                    IR.IOAttrLabel("Input file")
                )
            )
        ),
        IR.CVar(
            "pattern",
            WomStringType,
            None,
            Some(
                Vector(
                    IR.IOAttrHelp("The pattern to use to search in_file"),
                    IR.IOAttrGroup("Common"),
                    IR.IOAttrLabel("Search pattern")
                )
            )
        )
    )
    cgrepApplet.outputs shouldBe Vector(
        IR.CVar("count", WomIntegerType, None, None),
        IR.CVar(
            "out_file",
            WomSingleFileType,
            None,
            None
        )
    )
  }

  // Check parameter_meta pattern: ["array"]
  it should "recognize pattern in parameters_meta via WOM" in {
    val path = pathFromBasename("compiler", "pattern_params.wdl")
    val retval = Main.compile(
        path.toString :: cFlags
    )
    retval shouldBe a[Main.SuccessfulTerminationIR]
    val bundle = retval match {
      case Main.SuccessfulTerminationIR(ir) => ir
      case _                                => throw new Exception("sanity")
    }

    val cgrepTask = getTaskByName("pattern_params_cgrep", bundle)
    cgrepTask.parameterMeta shouldBe (
        Map(
            "in_file" -> MetaValueElement.MetaValueElementObject(
                Map(
                    "help" -> MetaValueElement
                      .MetaValueElementString("The input file to be searched"),
                    "patterns" -> MetaValueElement.MetaValueElementArray(
                        Vector(
                            MetaValueElement.MetaValueElementString("*.txt"),
                            MetaValueElement.MetaValueElementString("*.tsv")
                        )
                    ),
                    "group" -> MetaValueElement
                      .MetaValueElementString("Common"),
                    "label" -> MetaValueElement
                      .MetaValueElementString("Input file")
                )
            ),
            "pattern" -> MetaValueElement.MetaValueElementObject(
                Map(
                    "help" -> MetaValueElement
                      .MetaValueElementString("The pattern to use to search in_file"),
                    "group" -> MetaValueElement
                      .MetaValueElementString("Common"),
                    "label" -> MetaValueElement
                      .MetaValueElementString("Search pattern")
                )
            ),
            "out_file" -> MetaValueElement.MetaValueElementObject(
                Map(
                    "patterns" -> MetaValueElement.MetaValueElementArray(
                        Vector(
                            MetaValueElement.MetaValueElementString("*.txt"),
                            MetaValueElement.MetaValueElementString("*.tsv")
                        )
                    ),
                    "group" -> MetaValueElement.MetaValueElementString("Common"),
                    "label" -> MetaValueElement.MetaValueElementString("Output file")
                )
            )
        )
    )
    val iDef = cgrepTask.inputs.find(_.name == "in_file").get
    iDef.parameterMeta shouldBe (Some(
        MetaValueElement.MetaValueElementObject(
            Map(
                "group" -> MetaValueElement
                  .MetaValueElementString("Common"),
                "help" -> MetaValueElement
                  .MetaValueElementString("The input file to be searched"),
                "patterns" -> MetaValueElement.MetaValueElementArray(
                    Vector(
                        MetaValueElement.MetaValueElementString("*.txt"),
                        MetaValueElement.MetaValueElementString("*.tsv")
                    )
                ),
                "label" -> MetaValueElement
                  .MetaValueElementString("Input file")
            )
        )
    ))
  }

  // Check parameter_meta pattern: {"object"}
  it should "recognize pattern object in parameters_meta via WOM" in {
    val path = pathFromBasename("compiler", "pattern_obj_params.wdl")
    val retval = Main.compile(
        path.toString :: cFlags
    )
    retval shouldBe a[Main.SuccessfulTerminationIR]
    val bundle = retval match {
      case Main.SuccessfulTerminationIR(ir) => ir
      case _                                => throw new Exception("sanity")
    }

    val cgrepTask = getTaskByName("pattern_params_obj_cgrep", bundle)
    cgrepTask.parameterMeta shouldBe (
        Map(
            "in_file" -> MetaValueElement.MetaValueElementObject(
                Map(
                    "help" -> MetaValueElement
                      .MetaValueElementString("The input file to be searched"),
                    "patterns" -> MetaValueElement.MetaValueElementObject(
                        Map(
                            "class" -> MetaValueElement.MetaValueElementString("file"),
                            "tag" -> MetaValueElement.MetaValueElementArray(
                                Vector(MetaValueElement.MetaValueElementString("foo"),
                                       MetaValueElement.MetaValueElementString("bar"))
                            ),
                            "name" -> MetaValueElement.MetaValueElementArray(
                                Vector(MetaValueElement.MetaValueElementString("*.txt"),
                                       MetaValueElement.MetaValueElementString("*.tsv"))
                            )
                        )
                    ),
                    "group" -> MetaValueElement
                      .MetaValueElementString("Common"),
                    "label" -> MetaValueElement
                      .MetaValueElementString("Input file")
                )
            ),
            "pattern" -> MetaValueElement.MetaValueElementObject(
                Map(
                    "help" -> MetaValueElement
                      .MetaValueElementString("The pattern to use to search in_file"),
                    "group" -> MetaValueElement
                      .MetaValueElementString("Common"),
                    "label" -> MetaValueElement
                      .MetaValueElementString("Search pattern")
                )
            ),
            "out_file" -> MetaValueElement.MetaValueElementObject(
                Map(
                    "patterns" -> MetaValueElement.MetaValueElementArray(
                        Vector(
                            MetaValueElement.MetaValueElementString("*.txt"),
                            MetaValueElement.MetaValueElementString("*.tsv")
                        )
                    ),
                    "group" -> MetaValueElement.MetaValueElementString("Common"),
                    "label" -> MetaValueElement.MetaValueElementString("Output file")
                )
            )
        )
    )
    val iDef = cgrepTask.inputs.find(_.name == "in_file").get
    iDef.parameterMeta shouldBe (Some(
        MetaValueElement.MetaValueElementObject(
            Map(
                "group" -> MetaValueElement
                  .MetaValueElementString("Common"),
                "help" -> MetaValueElement
                  .MetaValueElementString("The input file to be searched"),
                "patterns" -> MetaValueElement.MetaValueElementObject(
                    Map(
                        "class" -> MetaValueElement.MetaValueElementString("file"),
                        "tag" -> MetaValueElement.MetaValueElementArray(
                            Vector(MetaValueElement.MetaValueElementString("foo"),
                                   MetaValueElement.MetaValueElementString("bar"))
                        ),
                        "name" -> MetaValueElement.MetaValueElementArray(
                            Vector(MetaValueElement.MetaValueElementString("*.txt"),
                                   MetaValueElement.MetaValueElementString("*.tsv"))
                        )
                    )
                ),
                "label" -> MetaValueElement
                  .MetaValueElementString("Input file")
            )
        )
    ))
  }

  // Check parameter_meta `choices` keyword
  it should "recognize choices in parameters_meta via CVar for input CVars" in {
    val path = pathFromBasename("compiler", "choice_values.wdl")
    val retval = Main.compile(
        path.toString :: cFlags
    )
    retval shouldBe a[Main.SuccessfulTerminationIR]
    val bundle = retval match {
      case Main.SuccessfulTerminationIR(ir) => ir
      case _                                => throw new Exception("sanity")
    }

    val cgrepApplet = getAppletByName("choice_values_cgrep", bundle)
    cgrepApplet.inputs shouldBe Vector(
        IR.CVar(
            "in_file",
            WomSingleFileType,
            None,
            Some(
                Vector(
                    IR.IOAttrChoices(
                        Vector(
                            IR.ChoiceReprFile(
                                name = None,
                                value = "dx://file-Fg5PgBQ0ffP7B8bg3xqB115G"
                            ),
                            IR.ChoiceReprFile(
                                name = None,
                                value = "dx://file-Fg5PgBj0ffPP0Jjv3zfv0yxq"
                            )
                        )
                    )
                )
            )
        ),
        IR.CVar(
            "pattern",
            WomStringType,
            None,
            Some(
                Vector(
                    IR.IOAttrChoices(
                        Vector(
                            IR.ChoiceReprString(value = "A"),
                            IR.ChoiceReprString(value = "B")
                        )
                    )
                )
            )
        )
    )
  }

  // Check parameter_meta `choices` keyword with annotated values
  it should "recognize annotated choices in parameters_meta via CVar for input CVars" in {
    val path = pathFromBasename("compiler", "choice_obj_values.wdl")
    val retval = Main.compile(
        path.toString :: cFlags
    )
    retval shouldBe a[Main.SuccessfulTerminationIR]
    val bundle = retval match {
      case Main.SuccessfulTerminationIR(ir) => ir
      case _                                => throw new Exception("sanity")
    }

    val cgrepApplet = getAppletByName("choice_values_cgrep", bundle)
    cgrepApplet.inputs shouldBe Vector(
        IR.CVar(
            "in_file",
            WomSingleFileType,
            None,
            Some(
                Vector(
                    IR.IOAttrChoices(
                        Vector(
                            IR.ChoiceReprFile(
                                name = Some("file1"),
                                value = "dx://file-Fg5PgBQ0ffP7B8bg3xqB115G"
                            ),
                            IR.ChoiceReprFile(
                                name = Some("file2"),
                                value = "dx://file-Fg5PgBj0ffPP0Jjv3zfv0yxq"
                            )
                        )
                    )
                )
            )
        ),
        IR.CVar(
            "pattern",
            WomStringType,
            None,
            Some(
                Vector(
                    IR.IOAttrChoices(
                        Vector(
                            IR.ChoiceReprString(value = "A"),
                            IR.ChoiceReprString(value = "B")
                        )
                    )
                )
            )
        )
    )
  }

  // Check parameter_meta `suggestion` keyword fails when there is a type mismatch
  it should "throw exception when choice types don't match parameter types" in {
    val path = pathFromBasename("compiler", "choices_type_mismatch.wdl")
    val retval = Main.compile(
        path.toString :: cFlags
    )
    retval shouldBe a[Main.UnsuccessfulTermination]
    // TODO: make assertion about exception message
  }

  // Check parameter_meta `suggestions` keyword
  it should "recognize suggestions in parameters_meta via CVar for input CVars" in {
    val path = pathFromBasename("compiler", "suggestion_values.wdl")
    val retval = Main.compile(
        path.toString :: cFlags
    )
    retval shouldBe a[Main.SuccessfulTerminationIR]
    val bundle = retval match {
      case Main.SuccessfulTerminationIR(ir) => ir
      case _                                => throw new Exception("sanity")
    }

    val cgrepApplet = getAppletByName("suggestion_values_cgrep", bundle)
    cgrepApplet.inputs shouldBe Vector(
        IR.CVar(
            "in_file",
            WomSingleFileType,
            None,
            Some(
                Vector(
                    IR.IOAttrSuggestions(
                        Vector(
                            IR.SuggestionReprFile(
                                name = None,
                                value = Some("dx://file-Fg5PgBQ0ffP7B8bg3xqB115G"),
                                project = None,
                                path = None
                            ),
                            IR.SuggestionReprFile(
                                name = None,
                                value = Some("dx://file-Fg5PgBj0ffPP0Jjv3zfv0yxq"),
                                project = None,
                                path = None
                            )
                        )
                    )
                )
            )
        ),
        IR.CVar(
            "pattern",
            WomStringType,
            None,
            Some(
                Vector(
                    IR.IOAttrSuggestions(
                        Vector(
                            IR.SuggestionReprString(value = "A"),
                            IR.SuggestionReprString(value = "B")
                        )
                    )
                )
            )
        )
    )
  }

  // Check parameter_meta `suggestions` keyword with annotated values
  it should "recognize annotated suggestions in parameters_meta via CVar for input CVars" in {
    val path = pathFromBasename("compiler", "suggestion_obj_values.wdl")
    val retval = Main.compile(
        path.toString :: cFlags
    )
    retval shouldBe a[Main.SuccessfulTerminationIR]
    val bundle = retval match {
      case Main.SuccessfulTerminationIR(ir) => ir
      case _                                => throw new Exception("sanity")
    }

    val cgrepApplet = getAppletByName("suggestion_values_cgrep", bundle)
    cgrepApplet.inputs shouldBe Vector(
        IR.CVar(
            "in_file",
            WomSingleFileType,
            None,
            Some(
                Vector(
                    IR.IOAttrSuggestions(
                        Vector(
                            IR.SuggestionReprFile(
                                name = Some("file1"),
                                value = Some("dx://file-Fg5PgBQ0ffP7B8bg3xqB115G"),
                                project = None,
                                path = None
                            ),
                            IR.SuggestionReprFile(
                                name = Some("file2"),
                                value = None,
                                project = Some("project-FGpfqjQ0ffPF1Q106JYP2j3v"),
                                path = Some("/test_data/f2.txt.gz")
                            )
                        )
                    )
                )
            )
        ),
        IR.CVar(
            "pattern",
            WomStringType,
            None,
            Some(
                Vector(
                    IR.IOAttrSuggestions(
                        Vector(
                            IR.SuggestionReprString(value = "A"),
                            IR.SuggestionReprString(value = "B")
                        )
                    )
                )
            )
        )
    )
  }

  // Check parameter_meta `suggestions` keyword fails when there is a parameter mismatch
  it should "throw exception when suggestion types don't match parameter types" in {
    val path = pathFromBasename("compiler", "suggestions_type_mismatch.wdl")
    val retval = Main.compile(
        path.toString :: cFlags
    )
    retval shouldBe a[Main.UnsuccessfulTermination]
    // TODO: make assertion about exception message
  }

  // Check parameter_meta `suggestions` keyword fails when there is a missing keyword
  it should "throw exception when file suggestion is missing a keyword" in {
    val path = pathFromBasename("compiler", "suggestions_missing_arg.wdl")
    val retval = Main.compile(
        path.toString :: cFlags
    )
    retval shouldBe a[Main.UnsuccessfulTermination]
    // TODO: make assertion about exception message
  }

  // Check parameter_meta `dx_type` keyword
  it should "recognize dx_type in parameters_meta via CVar for input CVars" in {
    val path = pathFromBasename("compiler", "add_dx_type.wdl")
    val retval = Main.compile(
        path.toString :: cFlags
    )
    retval shouldBe a[Main.SuccessfulTerminationIR]
    val bundle = retval match {
      case Main.SuccessfulTerminationIR(ir) => ir
      case _                                => throw new Exception("sanity")
    }

    val cgrepApplet = getAppletByName("add_dx_type", bundle)
    cgrepApplet.inputs shouldBe Vector(
        IR.CVar(
            "a",
            WomSingleFileType,
            None,
            Some(
                Vector(
                    IR.IOAttrType(IR.ConstraintReprString("fastq"))
                )
            )
        ),
        IR.CVar(
            "b",
            WomSingleFileType,
            None,
            Some(
                Vector(
                    IR.IOAttrType(
                        IR.ConstraintReprOper(
                            ConstraintOper.AND,
                            Vector(
                                IR.ConstraintReprString("fastq"),
                                IR.ConstraintReprOper(
                                    ConstraintOper.OR,
                                    Vector(
                                        IR.ConstraintReprString("Read1"),
                                        IR.ConstraintReprString("Read2")
                                    )
                                )
                            )
                        )
                    )
                )
            )
        )
    )
  }

  // Check parameter_meta `dx_type` keyword fails when specified for a non-file parameter
  it should "throw exception when dx_type is used on non-file parameter" in {
    val path = pathFromBasename("compiler", "dx_type_nonfile.wdl")
    val retval = Main.compile(
        path.toString :: cFlags
    )
    retval shouldBe a[Main.UnsuccessfulTermination]
    // TODO: make assertion about exception message
  }

  // Check parameter_meta `default` keyword
  it should "recognize default in parameters_meta via CVar for input CVars" in {
    val path = pathFromBasename("compiler", "add_default.wdl")
    val retval = Main.compile(
        path.toString :: cFlags
    )
    retval shouldBe a[Main.SuccessfulTerminationIR]
    val bundle = retval match {
      case Main.SuccessfulTerminationIR(ir) => ir
      case _                                => throw new Exception("sanity")
    }

    val cgrepApplet = getAppletByName("add_default", bundle)
    cgrepApplet.inputs shouldBe Vector(
        IR.CVar(
            "a",
            WomIntegerType,
            Some(WomInteger(1)),
            None
        ),
        IR.CVar(
            "b",
            WomOptionalType(WomIntegerType),
            None,
            Some(Vector(IR.IOAttrDefault(IR.DefaultReprInteger(2))))
        )
    )
  }

  // Check parameter_meta `default` keyword fails when there is a type mismatch
  it should "throw exception when default types don't match parameter types" in {
    val path = pathFromBasename("compiler", "default_type_mismatch.wdl")
    val retval = Main.compile(
        path.toString :: cFlags
    )
    retval shouldBe a[Main.UnsuccessfulTermination]
    // TODO: make assertion about exception message
  }

  it should "recognize help, group, and label in parameters_meta via WOM" in {
    val path = pathFromBasename("compiler", "help_input_params.wdl")
    val retval = Main.compile(
        path.toString :: cFlags
    )
    retval shouldBe a[Main.SuccessfulTerminationIR]
    val bundle = retval match {
      case Main.SuccessfulTerminationIR(ir) => ir
      case _                                => throw new Exception("sanity")
    }

    val cgrepTask = getTaskByName("help_input_params_cgrep", bundle)
    cgrepTask.parameterMeta shouldBe (
        Map(
            "in_file" -> MetaValueElement.MetaValueElementObject(
                Map(
                    "help" -> MetaValueElement
                      .MetaValueElementString("The input file to be searched"),
                    "group" -> MetaValueElement.MetaValueElementString("Common"),
                    "label" -> MetaValueElement.MetaValueElementString("Input file")
                )
            ),
            "pattern" -> MetaValueElement.MetaValueElementObject(
                Map(
                    "description" -> MetaValueElement
                      .MetaValueElementString("The pattern to use to search in_file"),
                    "group" -> MetaValueElement.MetaValueElementString("Common"),
                    "label" -> MetaValueElement.MetaValueElementString("Search pattern")
                )
            ),
            "s" -> MetaValueElement.MetaValueElementString("This is help for s")
        )
    )
    val iDef = cgrepTask.inputs.find(_.name == "in_file").get
    iDef.parameterMeta shouldBe (Some(
        MetaValueElement.MetaValueElementObject(
            Map(
                "help" -> MetaValueElement
                  .MetaValueElementString("The input file to be searched"),
                "group" -> MetaValueElement.MetaValueElementString("Common"),
                "label" -> MetaValueElement.MetaValueElementString("Input file")
            )
        )
    ))

    val diffTask = getTaskByName("help_input_params_diff", bundle)
    diffTask.parameterMeta shouldBe (
        Map(
            "a" -> MetaValueElement.MetaValueElementObject(
                Map(
                    "help" -> MetaValueElement.MetaValueElementString("lefthand file"),
                    "group" -> MetaValueElement.MetaValueElementString("Files"),
                    "label" -> MetaValueElement.MetaValueElementString("File A")
                )
            ),
            "b" -> MetaValueElement.MetaValueElementObject(
                Map(
                    "help" -> MetaValueElement.MetaValueElementString("righthand file"),
                    "group" -> MetaValueElement.MetaValueElementString("Files"),
                    "label" -> MetaValueElement.MetaValueElementString("File B")
                )
            )
        )
    )
  }

  it should "recognize help in parameters_meta via CVar for input CVars" in {
    val path = pathFromBasename("compiler", "help_input_params.wdl")
    val retval = Main.compile(
        path.toString :: cFlags
    )
    retval shouldBe a[Main.SuccessfulTerminationIR]
    val bundle = retval match {
      case Main.SuccessfulTerminationIR(ir) => ir
      case _                                => throw new Exception("sanity")
    }

    val cgrepApplet = getAppletByName("help_input_params_cgrep", bundle)
    cgrepApplet.inputs shouldBe Vector(
        IR.CVar(
            "s",
            WomStringType,
            None,
            Some(
                Vector(
                    IR.IOAttrHelp("This is help for s")
                )
            )
        ),
        IR.CVar(
            "in_file",
            WomSingleFileType,
            None,
            Some(
                Vector(
                    IR.IOAttrHelp("The input file to be searched"),
                    IR.IOAttrGroup("Common"),
                    IR.IOAttrLabel("Input file")
                )
            )
        ),
        IR.CVar(
            "pattern",
            WomStringType,
            None,
            Some(
                Vector(
                    IR.IOAttrHelp("The pattern to use to search in_file"),
                    IR.IOAttrGroup("Common"),
                    IR.IOAttrLabel("Search pattern")
                )
            )
        )
    )
  }

  // This is actually more of a test to confirm that symbols that are not input
  // variables are ignored. WOM doesn't include a paramMeta member for the output
  // var class anyways, so it's basically impossible for this to happen
  it should "ignore help in parameters_meta via CVar for output CVars" in {
    val path = pathFromBasename("compiler", "help_output_params.wdl")
    val retval = Main.compile(
        path.toString :: cFlags
    )
    retval shouldBe a[Main.SuccessfulTerminationIR]
    val bundle = retval match {
      case Main.SuccessfulTerminationIR(ir) => ir
      case _                                => throw new Exception("sanity")
    }

    val cgrepApplet = getAppletByName("help_output_params_cgrep", bundle)
    cgrepApplet.outputs shouldBe Vector(
        IR.CVar(
            "count",
            WomIntegerType,
            None,
            None
        )
    )
  }

  it should "recognize app metadata" in {
    val path = pathFromBasename("compiler", "add_app_meta.wdl")
    val retval = Main.compile(
        path.toString :: cFlags
    )
    retval shouldBe a[Main.SuccessfulTerminationIR]
    val bundle = retval match {
      case Main.SuccessfulTerminationIR(ir) => ir
      case _                                => throw new Exception("sanity")
    }

    val cgrepApplet = getAppletByName("add", bundle)
    cgrepApplet.meta shouldBe Some(
        Vector(
            IR.TaskAttrDeveloperNotes("Check out my sick bash expression! Three dolla signs!!!"),
            IR.TaskAttrDescription(
                "Adds two int together. This app adds together two integers and returns the sum"
            ),
            IR.TaskAttrTags(Vector("add", "ints")),
            IR.TaskAttrOpenSource(true),
            IR.TaskAttrVersion("1.0"),
            IR.TaskAttrProperties(Map("foo" -> "bar")),
            IR.TaskAttrCategories(Vector("Assembly")),
            IR.TaskAttrDetails(
                Map(
                    "contactEmail" -> "joe@dev.com",
                    "upstreamVersion" -> "1.0",
                    "upstreamAuthor" -> "Joe Developer",
                    "upstreamUrl" -> "https://dev.com/joe",
                    "upstreamLicenses" -> Vector("MIT"),
                    "whatsNew" -> Vector(
                        Map(
                            "version" -> "1.1",
                            "changes" -> Vector("Added parameter --foo", "Added cowsay easter-egg")
                        ),
                        Map(
                            "version" -> "1.0",
                            "changes" -> Vector("Initial version")
                        )
                    )
                )
            ),
            IR.TaskAttrTitle("Add Ints"),
            IR.TaskAttrTypes(Vector("Adder"))
        )
    )
  }

  it should "handle streaming files" in {
    val path = pathFromBasename("compiler", "streaming_files.wdl")
    val retval = Main.compile(
        path.toString :: cFlags
    )
    retval shouldBe a[Main.SuccessfulTerminationIR]
    val bundle = retval match {
      case Main.SuccessfulTerminationIR(ir) => ir
      case _                                => throw new Exception("sanity")
    }

    val cgrepTask = getTaskByName("cgrep", bundle)
    cgrepTask.parameterMeta shouldBe (Map(
        "in_file" -> MetaValueElement.MetaValueElementString("stream")
    ))
    val iDef = cgrepTask.inputs.find(_.name == "in_file").get
    iDef.parameterMeta shouldBe (Some(
        MetaValueElement.MetaValueElementString("stream")
    ))

    val diffTask = getTaskByName("diff", bundle)
    diffTask.parameterMeta shouldBe (Map(
        "a" -> MetaValueElement.MetaValueElementString("stream"),
        "b" -> MetaValueElement.MetaValueElementString("stream")
    ))
  }

  it should "recognize the streaming object annotation" in {
    val path = pathFromBasename("compiler", "streaming_files_obj.wdl")
    val retval = Main.compile(
        path.toString :: cFlags
    )
    retval shouldBe a[Main.SuccessfulTerminationIR]
    val bundle = retval match {
      case Main.SuccessfulTerminationIR(ir) => ir
      case _                                => throw new Exception("sanity")
    }

    val cgrepTask = getTaskByName("cgrep", bundle)
    cgrepTask.parameterMeta shouldBe (Map(
        "in_file" -> MetaValueElement.MetaValueElementObject(
            Map("stream" -> MetaValueElement.MetaValueElementBoolean(true))
        )
    ))
    val iDef = cgrepTask.inputs.find(_.name == "in_file").get
    iDef.parameterMeta shouldBe (Some(
        MetaValueElement.MetaValueElementObject(
            Map("stream" -> MetaValueElement.MetaValueElementBoolean(true))
        )
    ))

    val diffTask = getTaskByName("diff", bundle)
    diffTask.parameterMeta shouldBe (
        Map(
            "a" -> MetaValueElement.MetaValueElementObject(
                Map("stream" -> MetaValueElement.MetaValueElementBoolean(true))
            ),
            "b" -> MetaValueElement.MetaValueElementObject(
                Map("stream" -> MetaValueElement.MetaValueElementBoolean(true))
            )
        )
    )
  }

  it should "recognize the streaming annotation for wdl draft2" in {
    val path = pathFromBasename("draft2", "streaming.wdl")
    val retval = Main.compile(
        path.toString :: cFlags
    )
    retval shouldBe a[Main.SuccessfulTerminationIR]
    val bundle = retval match {
      case Main.SuccessfulTerminationIR(ir) => ir
      case _                                => throw new Exception("sanity")
    }
    val diffTask = getTaskByName("diff", bundle)
    diffTask.parameterMeta shouldBe (Map("a" -> MetaValueElement.MetaValueElementString("stream"),
                                         "b" -> MetaValueElement.MetaValueElementString("stream")))
  }

  it should "handle an empty workflow" in {
    val path = pathFromBasename("util", "empty_workflow.wdl")
    val retval = Main.compile(
        path.toString :: cFlags
    )
    retval shouldBe a[Main.SuccessfulTerminationIR]
  }

  it should "handle structs" in {
    val path = pathFromBasename("struct", "Person.wdl")
    val retval = Main.compile(
        path.toString :: cFlags
    )
    retval shouldBe a[Main.SuccessfulTerminationIR]
  }

  it should "recognize that an argument with a default can be omitted at the call site" in {
    val path = pathFromBasename("compiler", "call_level2.wdl")
    val retval = Main.compile(path.toString :: cFlags)
    retval shouldBe a[Main.SuccessfulTerminationIR]
  }

  it should "check for reserved symbols" in {
    val path = pathFromBasename("compiler", "reserved.wdl")
    val retval = Main.compile(path.toString :: cFlags)

    inside(retval) {
      case Main.UnsuccessfulTermination(errMsg) =>
        errMsg should include("reserved substring ___")
    }
  }

  it should "do nested scatters" in {
    val path = pathFromBasename("compiler", "nested_scatter.wdl")
    val retval = Main.compile(path.toString :: cFlags)
    retval shouldBe a[Main.SuccessfulTerminationIR]
  }

  it should "handle struct imported several times" in {
    val path = pathFromBasename("struct/struct_imported_twice", "file3.wdl")
    val retval = Main.compile(path.toString :: cFlags)
    retval shouldBe a[Main.SuccessfulTerminationIR]
  }

  it should "handle file constants in a workflow" in {
    val path = pathFromBasename("compiler", "wf_constants.wdl")
    val retval = Main.compile(path.toString :: cFlags)
    retval shouldBe a[Main.SuccessfulTerminationIR]
  }

  it should "respect import flag" in {
    val path = pathFromBasename("compiler/imports", "A.wdl")
    val libraryPath = path.getParent.resolve("lib")
    val retval = Main.compile(path.toString :: "--imports" :: libraryPath.toString :: cFlags)
    retval shouldBe a[Main.SuccessfulTerminationIR]
  }

  it should "respect import -p flag" in {
    val path = pathFromBasename("compiler/imports", "A.wdl")
    val libraryPath = path.getParent.resolve("lib")
    val retval = Main.compile(path.toString :: "--p" :: libraryPath.toString :: cFlags)
    retval shouldBe a[Main.SuccessfulTerminationIR]
  }

  it should "pass environment between deep stages" in {
    val path = pathFromBasename("compiler", "environment_passing_deep_nesting.wdl")
    val retval = Main.compile(path.toString :: cFlags)
    retval shouldBe a[Main.SuccessfulTerminationIR]
  }

  it should "handle multiple struct definitions" in {
    val path = pathFromBasename("struct/DEVEX-1196-struct-resolution-wrong-order", "file3.wdl")
    val retval = Main.compile(path.toString :: cFlags)
    retval shouldBe a[Main.SuccessfulTerminationIR]
  }

  it should "retain all characters in a WDL task" in {
    val path = pathFromBasename("bugs", "missing_chars_in_task.wdl")
    val retval = Main.compile(
        path.toString
//                                      :: "--verbose"
//                                      :: "--verboseKey" :: "GenerateIR"
          :: cFlags
    )
    retval shouldBe a[Main.SuccessfulTerminationIR]

    val commandSection =
      """|  command {
         |  echo 1 hello world | sed 's/world/wdl/'
         |  echo 2 hello \
         |  world \
         |  | sed 's/world/wdl/'
         |  echo 3 hello \
         |  world | \
         |  sed 's/world/wdl/'
         |  }
         |""".stripMargin

    inside(retval) {
      case Main.SuccessfulTerminationIR(bundle) =>
        bundle.allCallables.size shouldBe (1)
        val (_, callable) = bundle.allCallables.head
        callable shouldBe a[IR.Applet]
        val task = callable.asInstanceOf[IR.Applet]
        task.womSourceCode should include(commandSection)
    }
  }

  it should "correctly flatten a workflow with imports" in {
    val path = pathFromBasename("compiler", "wf_to_flatten.wdl")
    val retval = Main.compile(path.toString :: cFlags)
    retval shouldBe a[Main.SuccessfulTerminationIR]
  }

  it should "detect a request for GPU" in {
    val path = pathFromBasename("compiler", "GPU.wdl")
    val retval = Main.compile(
        path.toString
        //                                      :: "--verbose"
        //                                      :: "--verboseKey" :: "GenerateIR"
          :: cFlags
    )
    retval shouldBe a[Main.SuccessfulTerminationIR]

    inside(retval) {
      case Main.SuccessfulTerminationIR(bundle) =>
        bundle.allCallables.size shouldBe (1)
        val (_, callable) = bundle.allCallables.head
        callable shouldBe a[IR.Applet]
        val task = callable.asInstanceOf[IR.Applet]
        task.instanceType shouldBe (IR.InstanceTypeConst(Some("mem3_ssd1_gpu_x8"),
                                                         None,
                                                         None,
                                                         None,
                                                         None))
    }
  }

  it should "compile a scatter with a sub-workflow that has an optional argument" taggedAs (EdgeTest) in {
    val path = pathFromBasename("compiler", "scatter_subworkflow_with_optional.wdl")
    val retval = Main.compile(
        path.toString
//                                      :: "--verbose"
//                                      :: "--verboseKey" :: "GenerateIR"
          :: cFlags
    )
    retval shouldBe a[Main.SuccessfulTerminationIR]

    val bundle = retval match {
      case Main.SuccessfulTerminationIR(bundle) => bundle
      case _                                    => throw new Exception("sanity")
    }

    val wfs: Vector[IR.Workflow] = bundle.allCallables
      .map {
        case (name, wf: IR.Workflow) if wf.locked && wf.level == IR.Level.Sub => Some(wf)
        case (_, _)                                                           => None
      }
      .flatten
      .toVector
    wfs.length shouldBe (1)
    val subwf = wfs(0)

    val samtools = subwf.inputs.find { case (cVar, _) => cVar.name == "samtools_memory" }
    inside(samtools) {
      case Some((cVar, _)) =>
        cVar.womType shouldBe (WomOptionalType(WomStringType))
    }
  }

  it should "pass as subworkflows do not have expression statement in output block" taggedAs (EdgeTest) in {
    val path = pathFromBasename("subworkflows", basename = "trains.wdl")

    val retval = Main.compile(
        path.toString :: cFlags
    )
    retval shouldBe a[Main.SuccessfulTerminationIR]
  }

  // this is currently failing.
  it should "pass with subworkflows having expression" taggedAs (EdgeTest) in {
    val path = pathFromBasename("subworkflows", basename = "ensure_trains.wdl")

    val retval = Main.compile(
        path.toString
//                :: "--verbose"
//                :: "--verboseKey" :: "GenerateIR"
          :: cFlags
    )
    retval shouldBe a[Main.SuccessfulTerminationIR]
  }

}
