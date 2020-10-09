package dx.translator

import java.nio.file.{Path, Paths}

import dx.Tags.EdgeTest
import dx.api._
import dx.compiler.Main
import dx.compiler.Main.SuccessIR
import dx.core.ir.Type._
import dx.core.ir.Value._
import dx.core.ir._
import dx.translator.CallableAttributes._
import dx.translator.ParameterAttributes._
import dx.translator.RunSpec._
import dx.core.CliUtils.{Failure, UnsuccessfulTermination}
import dx.translator.wdl.WdlDocumentSource
import org.scalatest.Inside._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import wdlTools.generators.code.WdlGenerator
import wdlTools.util.Logger

// These tests involve compilation -without- access to the platform.
//
class TranslatorTest extends AnyFlatSpec with Matchers {
  private val dxApi = DxApi(Logger.Quiet)

  private def pathFromBasename(dir: String, basename: String): Path = {
    val p = getClass.getResource(s"/${dir}/${basename}").getPath
    Paths.get(p)
  }

  private val dxProject = dxApi.currentProject

  // task compilation
  private val cFlags =
    List("--compileMode", "ir", "-quiet", "--locked", "--project", dxProject.id)

  private val cFlagsUnlocked =
    List("--compileMode", "ir", "-quiet", "--project", dxProject.id)

  val dbgFlags = List("--compileMode",
                      "ir",
                      "--verbose",
                      "--verboseKey",
                      "GenerateIR",
                      "--locked",
                      "--project",
                      dxProject.id)

  private def getApplicationByName(name: String, bundle: Bundle): Application =
    bundle.allCallables(name) match {
      case a: Application => a
      case _              => throw new Exception(s"${name} is not an applet")
    }

  it should "IR compile a single WDL task" in {
    val path = pathFromBasename("compiler", "add.wdl")
    val args = path.toString :: cFlags
    Main.compile(args.toVector) shouldBe a[SuccessIR]
  }

  it should "IR compile a task with docker" in {
    val path = pathFromBasename("compiler", "BroadGenomicsDocker.wdl")
    val args = path.toString :: cFlags
    Main.compile(args.toVector) shouldBe a[SuccessIR]
  }

  // workflow compilation
  it should "IR compile a linear WDL workflow without expressions" in {
    val path = pathFromBasename("compiler", "wf_linear_no_expr.wdl")
    val args = path.toString :: cFlags
    Main.compile(args.toVector) shouldBe a[SuccessIR]
  }

  it should "IR compile a linear WDL workflow" in {
    val path = pathFromBasename("compiler", "wf_linear.wdl")
    val args = path.toString :: cFlags
    Main.compile(args.toVector) shouldBe a[SuccessIR]
  }

  it should "IR compile unlocked workflow" in {
    val path = pathFromBasename("compiler", "wf_linear.wdl")
    val args = path.toString :: cFlagsUnlocked
    Main.compile(args.toVector) shouldBe a[SuccessIR]
  }

  it should "IR compile a non trivial linear workflow with variable coercions" in {
    val path = pathFromBasename("compiler", "cast.wdl")
    val args = path.toString :: cFlags
    Main.compile(args.toVector) shouldBe a[SuccessIR]
  }

  it should "IR compile a workflow with two consecutive calls" in {
    val path = pathFromBasename("compiler", "strings.wdl")
    val args = path.toString :: cFlags
    Main.compile(args.toVector) shouldBe a[SuccessIR]
  }

  it should "IR compile a workflow with a scatter without a call" in {
    val path = pathFromBasename("compiler", "scatter_no_call.wdl")
    val args = path.toString :: cFlags
    Main.compile(args.toVector) shouldBe a[SuccessIR]
  }

  it should "IR compile optionals" in {
    val path = pathFromBasename("compiler", "optionals.wdl")
    val args = path.toString :: cFlags
    //                :: "--verbose"
    //                :: "--verboseKey" :: "GenerateIR"
    Main.compile(args.toVector) shouldBe a[SuccessIR]
  }

  it should "support imports" in {
    val path = pathFromBasename("compiler", "check_imports.wdl")
    val args = path.toString :: cFlags
    Main.compile(args.toVector) shouldBe a[SuccessIR]
  }

  it should "IR compile a draft2 workflow" in {
    val path = pathFromBasename("draft2", "shapes.wdl")
    val args = path.toString :: cFlags
    val retval = Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
  }

  it should "expressions in an output block" in {
    val path = pathFromBasename("compiler", "expr_output_block.wdl")
    val args = path.toString :: cFlags
    Main.compile(args.toVector) shouldBe a[SuccessIR]
  }

  /*  ignore should "scatters over maps" in {
    val path = pathFromBasename("compiler", "dict2.wdl")
    val args =         path.toString :: cFlags
Main.compile(args.toVector) shouldBe a[SuccessIR]
  }*/

  it should "skip missing optional arguments" in {
    val path = pathFromBasename("util", "missing_inputs_to_direct_call.wdl")
    val args = path.toString :: cFlags
    Main.compile(args.toVector) shouldBe a[SuccessIR]
  }

  it should "handle calling subworkflows" in {
    val path = pathFromBasename("subworkflows", "trains.wdl")
    val args = path.toString :: cFlags
    val retval = Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
    val irwf = retval match {
      case SuccessIR(irwf, _) => irwf
      case _                  => throw new Exception("unexpected")
    }
    val primaryWf: Workflow = irwf.primaryCallable match {
      case Some(wf: Workflow) => wf
      case _                  => throw new Exception("unexpected")
    }
    primaryWf.stages.size shouldBe 2
  }

  it should "compile a sub-block with several calls" in {
    val path = pathFromBasename("compiler", "subblock_several_calls.wdl")
    val args = path.toString :: cFlags
    Main.compile(args.toVector) shouldBe a[SuccessIR]
  }

  it should "missing workflow inputs" in {
    val path = pathFromBasename("input_file", "missing_args.wdl")
    val args = path.toString :: List("--compileMode", "ir", "--quiet", "--project", dxProject.id)
    Main.compile(args.toVector) shouldBe a[SuccessIR]
  }

  // Nested blocks
  it should "compile two level nested workflow" in {
    val path = pathFromBasename("nested", "two_levels.wdl")
    val args = path.toString :: cFlags
    Main.compile(args.toVector) shouldBe a[SuccessIR]
  }

  it should "handle passing closure arguments to nested blocks" in {
    val path = pathFromBasename("nested", "param_passing.wdl")
    val args = path.toString :: cFlags
    Main.compile(args.toVector) shouldBe a[SuccessIR]
  }

  it should "compile a workflow calling a subworkflow as a direct call" in {
    val path = pathFromBasename("draft2", "movies.wdl")
    val args = path.toString :: cFlags
    val bundle: Bundle = Main.compile(args.toVector) match {
      case SuccessIR(bundle, _) => bundle
      case other =>
        Logger.error(other.toString)
        throw new Exception(s"Failed to compile ${path}")
    }
    val wf: Workflow = bundle.primaryCallable match {
      case Some(wf: Workflow) =>
        wf
      case _ => throw new Exception("bad value in bundle")
    }
    val stage = wf.stages.head
    stage.description shouldBe "review"
  }

  it should "compile a workflow calling a subworkflow as a direct call with 2.0 version" in {
    val path = pathFromBasename("v2", "movies.wdl")
    val args = path.toString :: cFlags
    val bundle: Bundle = Main.compile(args.toVector) match {
      case SuccessIR(bundle, _) => bundle
      case other =>
        Logger.error(other.toString)
        throw new Exception(s"Failed to compile ${path}")
    }
    val wf: Workflow = bundle.primaryCallable match {
      case Some(wf: Workflow) =>
        wf
      case _ => throw new Exception("bad value in bundle")
    }
    val stage = wf.stages.head
    stage.description shouldBe "review"
  }

  it should "compile a workflow calling a subworkflow with native DNANexus applet as a direct call with 2.0 version" in {
    val path = pathFromBasename("v2", "call_dnanexus_applet.wdl")
    val args = path.toString :: cFlags
    val bundle: Bundle = Main.compile(args.toVector) match {
      case SuccessIR(bundle, _) => bundle
      case other =>
        Logger.error(other.toString)
        throw new Exception(s"Failed to compile ${path}")
    }
    val wf: Workflow = bundle.primaryCallable match {
      case Some(wf: Workflow) =>
        wf
      case _ => throw new Exception("bad value in bundle")
    }
    wf.stages.size shouldBe 2
    wf.stages(0).description shouldBe "native_sum_012"
    wf.stages(1).description shouldBe "native_sum_wf"
  }

  it should "three nesting levels" in {
    val path = pathFromBasename("nested", "three_levels.wdl")
    val args = path.toString :: cFlags
    val retval = Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
    val bundle = retval match {
      case SuccessIR(ir, _) => ir
      case _                => throw new Exception("unexpected")
    }
    val primary: Callable = bundle.primaryCallable.get
    val wf = primary match {
      case wf: Workflow => wf
      case _            => throw new Exception("unexpected")
    }

    wf.stages.size shouldBe 1

    val level2 = bundle.allCallables(wf.name)
    level2 shouldBe a[Workflow]
    val wfLevel2 = level2.asInstanceOf[Workflow]
    wfLevel2.stages.size shouldBe 1
  }

  it should "four nesting levels" in {
    val path = pathFromBasename("nested", "four_levels.wdl")
    val args = path.toString :: cFlags
    val retval = Main.compile(args.toVector)
    /*        inside(retval) {
            case Main.UnsuccessfulTermination(errMsg) =>
                errMsg should include ("nested scatter")
 }*/
    retval shouldBe a[SuccessIR]
  }

  // Check parameter_meta `pattern` keyword
  it should "recognize pattern in parameters_meta via Parameter for input Parameters" in {
    val path = pathFromBasename("compiler", "pattern_params.wdl")
    val args = path.toString :: cFlags
    val retval = Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
    val bundle = retval match {
      case SuccessIR(ir, _) => ir
      case _                => throw new Exception("unexpected")
    }

    val cgrepApplication = getApplicationByName("pattern_params_cgrep", bundle)
    cgrepApplication.inputs.iterator sameElements Vector(
        Parameter(
            "in_file",
            Type.TFile,
            None,
            Vector(
                HelpAttribute("The input file to be searched"),
                PatternsAttribute(PatternsArray(Vector("*.txt", "*.tsv"))),
                GroupAttribute("Common"),
                LabelAttribute("Input file")
            )
        ),
        Parameter(
            "pattern",
            TString,
            None,
            Vector(
                HelpAttribute("The pattern to use to search in_file"),
                GroupAttribute("Common"),
                LabelAttribute("Search pattern")
            )
        )
    )
    cgrepApplication.outputs.iterator sameElements Vector(
        Parameter("count", TInt, None, Vector.empty),
        Parameter(
            "out_file",
            TFile,
            None,
            Vector(
                PatternsAttribute(PatternsArray(Vector("*.txt", "*.tsv"))),
                GroupAttribute("Common"),
                LabelAttribute("Output file")
            )
        )
    )
  }

  // Check parameter_meta `pattern` keyword
  it should "recognize pattern object in parameters_obj_meta via Parameter for input Parameters" in {
    val path = pathFromBasename("compiler", "pattern_obj_params.wdl")
    val args = path.toString :: cFlags
    val retval = Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
    val bundle = retval match {
      case SuccessIR(ir, _) => ir
      case _                => throw new Exception("unexpected")
    }

    val cgrepApplication = getApplicationByName("pattern_params_obj_cgrep", bundle)
    cgrepApplication.inputs.iterator sameElements Vector(
        Parameter(
            "in_file",
            TFile,
            None,
            Vector(
                HelpAttribute("The input file to be searched"),
                PatternsAttribute(
                    PatternsObject(
                        Vector("*.txt", "*.tsv"),
                        Some("file"),
                        Vector("foo", "bar")
                    )
                ),
                GroupAttribute("Common"),
                LabelAttribute("Input file")
            )
        ),
        Parameter(
            "pattern",
            TString,
            None,
            Vector(
                HelpAttribute("The pattern to use to search in_file"),
                GroupAttribute("Common"),
                LabelAttribute("Search pattern")
            )
        )
    )
    cgrepApplication.outputs.iterator sameElements Vector(
        Parameter("count", TInt, None, Vector.empty),
        Parameter(
            "out_file",
            TFile,
            None,
            Vector(
                PatternsAttribute(PatternsArray(Vector("*.txt", "*.tsv"))),
                GroupAttribute("Common"),
                LabelAttribute("Input file")
            )
        )
    )
  }

  // Check parameter_meta `choices` keyword
  it should "recognize choices in parameters_meta via Parameter for input Parameters" in {
    val path = pathFromBasename("compiler", "choice_values.wdl")
    val args = path.toString :: cFlags
    val retval = Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
    val bundle = retval match {
      case SuccessIR(ir, _) => ir
      case _                => throw new Exception("unexpected")
    }

    val cgrepApplication = getApplicationByName("choice_values_cgrep", bundle)
    cgrepApplication.inputs.iterator sameElements Vector(
        Parameter(
            "in_file",
            TFile,
            None,
            Vector(
                ChoicesAttribute(
                    Vector(
                        FileChoice(
                            name = None,
                            value = "dx://file-Fg5PgBQ0ffP7B8bg3xqB115G"
                        ),
                        FileChoice(
                            name = None,
                            value = "dx://file-Fg5PgBj0ffPP0Jjv3zfv0yxq"
                        )
                    )
                )
            )
        ),
        Parameter(
            "pattern",
            TString,
            None,
            Vector(
                ChoicesAttribute(
                    Vector(
                        SimpleChoice(value = VString("A")),
                        SimpleChoice(value = VString("B"))
                    )
                )
            )
        )
    )
  }

  // Check parameter_meta `choices` keyword with annotated values
  it should "recognize annotated choices in parameters_meta via Parameter for input Parameters" in {
    val path = pathFromBasename("compiler", "choice_obj_values.wdl")
    val args = path.toString :: cFlags
    val retval = Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
    val bundle = retval match {
      case SuccessIR(ir, _) => ir
      case _                => throw new Exception("unexpected")
    }

    val cgrepApplication = getApplicationByName("choice_values_cgrep", bundle)
    cgrepApplication.inputs.iterator sameElements Vector(
        Parameter(
            "in_file",
            TFile,
            None,
            Vector(
                ChoicesAttribute(
                    Vector(
                        FileChoice(
                            name = Some("file1"),
                            value = "dx://file-Fg5PgBQ0ffP7B8bg3xqB115G"
                        ),
                        FileChoice(
                            name = Some("file2"),
                            value = "dx://file-Fg5PgBj0ffPP0Jjv3zfv0yxq"
                        )
                    )
                )
            )
        ),
        Parameter(
            "pattern",
            TString,
            None,
            Vector(
                ChoicesAttribute(
                    Vector(
                        SimpleChoice(value = VString("A")),
                        SimpleChoice(value = VString("B"))
                    )
                )
            )
        )
    )
  }

  // Check parameter_meta `suggestion` keyword fails when there is a type mismatch
  it should "throw exception when choice types don't match parameter types" in {
    val path = pathFromBasename("compiler", "choices_type_mismatch.wdl")
    val args = path.toString :: cFlags
    val retval = Main.compile(args.toVector)
    retval shouldBe a[UnsuccessfulTermination]
    // TODO: make assertion about exception message
  }

  // Check parameter_meta `suggestions` keyword
  it should "recognize suggestions in parameters_meta via Parameter for input Parameters" in {
    val path = pathFromBasename("compiler", "suggestion_values.wdl")
    val args = path.toString :: cFlags
    val retval = Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
    val bundle = retval match {
      case SuccessIR(ir, _) => ir
      case _                => throw new Exception("unexpected")
    }

    val cgrepApplication = getApplicationByName("suggestion_values_cgrep", bundle)
    cgrepApplication.inputs.iterator sameElements Vector(
        Parameter(
            "in_file",
            TFile,
            None,
            Vector(
                SuggestionsAttribute(
                    Vector(
                        FileSuggestion(
                            name = None,
                            value = Some("dx://file-Fg5PgBQ0ffP7B8bg3xqB115G"),
                            project = None,
                            path = None
                        ),
                        FileSuggestion(
                            name = None,
                            value = Some("dx://file-Fg5PgBj0ffPP0Jjv3zfv0yxq"),
                            project = None,
                            path = None
                        )
                    )
                )
            )
        ),
        Parameter(
            "pattern",
            TString,
            None,
            Vector(
                SuggestionsAttribute(
                    Vector(
                        SimpleSuggestion(value = VString("A")),
                        SimpleSuggestion(value = VString("B"))
                    )
                )
            )
        )
    )
  }

  // Check parameter_meta `suggestions` keyword with annotated values
  it should "recognize annotated suggestions in parameters_meta via Parameter for input Parameters" in {
    val path = pathFromBasename("compiler", "suggestion_obj_values.wdl")
    val args = path.toString :: cFlags
    val retval = Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
    val bundle = retval match {
      case SuccessIR(ir, _) => ir
      case _                => throw new Exception("unexpected")
    }

    val cgrepApplication = getApplicationByName("suggestion_values_cgrep", bundle)
    cgrepApplication.inputs.iterator sameElements Vector(
        Parameter(
            "in_file",
            TFile,
            None,
            Vector(
                SuggestionsAttribute(
                    Vector(
                        FileSuggestion(
                            name = Some("file1"),
                            value = Some("dx://file-Fg5PgBQ0ffP7B8bg3xqB115G"),
                            project = None,
                            path = None
                        ),
                        FileSuggestion(
                            name = Some("file2"),
                            value = None,
                            project = Some("project-FGpfqjQ0ffPF1Q106JYP2j3v"),
                            path = Some("/test_data/f2.txt.gz")
                        )
                    )
                )
            )
        ),
        Parameter(
            "pattern",
            TString,
            None,
            Vector(
                SuggestionsAttribute(
                    Vector(
                        SimpleSuggestion(value = VString("A")),
                        SimpleSuggestion(value = VString("B"))
                    )
                )
            )
        )
    )
  }

  // Check parameter_meta `suggestions` keyword fails when there is a parameter mismatch
  it should "throw exception when suggestion types don't match parameter types" in {
    val path = pathFromBasename("compiler", "suggestions_type_mismatch.wdl")
    val args = path.toString :: cFlags
    val retval = Main.compile(args.toVector)
    retval shouldBe a[UnsuccessfulTermination]
    // TODO: make assertion about exception message
  }

  // Check parameter_meta `suggestions` keyword fails when there is a missing keyword
  it should "throw exception when file suggestion is missing a keyword" in {
    val path = pathFromBasename("compiler", "suggestions_missing_arg.wdl")
    val args = path.toString :: cFlags
    val retval = Main.compile(args.toVector)
    retval shouldBe a[UnsuccessfulTermination]
    // TODO: make assertion about exception message
  }

  // Check parameter_meta `dx_type` keyword
  it should "recognize dx_type in parameters_meta via Parameter for input Parameters" in {
    val path = pathFromBasename("compiler", "add_dx_type.wdl")
    val args = path.toString :: cFlags
    val retval = Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
    val bundle = retval match {
      case SuccessIR(ir, _) => ir
      case _                => throw new Exception("unexpected")
    }

    val cgrepApplication = getApplicationByName("add_dx_type", bundle)
    cgrepApplication.inputs shouldBe Vector(
        Parameter(
            "a",
            TFile,
            None,
            Vector(
                TypeAttribute(StringConstraint("fastq"))
            )
        ),
        Parameter(
            "b",
            TFile,
            None,
            Vector(
                TypeAttribute(
                    CompoundConstraint(
                        ConstraintOper.And,
                        Vector(
                            StringConstraint("fastq"),
                            CompoundConstraint(
                                ConstraintOper.Or,
                                Vector(
                                    StringConstraint("Read1"),
                                    StringConstraint("Read2")
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
    val args = path.toString :: cFlags
    val retval = Main.compile(args.toVector)
    retval shouldBe a[UnsuccessfulTermination]
    // TODO: make assertion about exception message
  }

  // Check parameter_meta `default` keyword
  it should "recognize default in parameters_meta via Parameter for input Parameters" in {
    val path = pathFromBasename("compiler", "add_default.wdl")
    val args = path.toString :: cFlags
    val retval = Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
    val bundle = retval match {
      case SuccessIR(ir, _) => ir
      case _                => throw new Exception("unexpected")
    }

    val cgrepApplication = getApplicationByName("add_default", bundle)
    cgrepApplication.inputs shouldBe Vector(
        Parameter(
            "a",
            TInt,
            Some(VInt(1)),
            Vector.empty
        ),
        Parameter(
            "b",
            TOptional(TInt),
            None,
            Vector(DefaultAttribute(VInt(2)))
        )
    )
  }

  // Check parameter_meta `default` keyword fails when there is a type mismatch
  it should "throw exception when default types don't match parameter types" in {
    val path = pathFromBasename("compiler", "default_type_mismatch.wdl")
    val args = path.toString :: cFlags
    val retval =
      Main.compile(args.toVector)
    retval shouldBe a[UnsuccessfulTermination]
    // TODO: make assertion about exception message
  }

  it should "recognize help in parameters_meta via Parameter for input Parameters" in {
    val path = pathFromBasename("compiler", "help_input_params.wdl")
    val args = path.toString :: cFlags
    val retval =
      Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
    val bundle = retval match {
      case SuccessIR(ir, _) => ir
      case _                => throw new Exception("unexpected")
    }

    val cgrepApplication = getApplicationByName("help_input_params_cgrep", bundle)
    cgrepApplication.inputs.iterator sameElements Vector(
        Parameter(
            "s",
            TString,
            None,
            Vector(
                HelpAttribute("This is help for s")
            )
        ),
        Parameter(
            "in_file",
            TFile,
            None,
            Vector(
                HelpAttribute("The input file to be searched"),
                GroupAttribute("Common"),
                LabelAttribute("Input file")
            )
        ),
        Parameter(
            "pattern",
            TString,
            None,
            Vector(
                HelpAttribute("The pattern to use to search in_file"),
                GroupAttribute("Common"),
                LabelAttribute("Search pattern")
            )
        )
    )
  }

  // This is actually more of a test to confirm that symbols that are not input
  // variables are ignored. WDL doesn't include a paramMeta member for the output
  // var class anyways, so it's basically impossible for this to happen
  it should "ignore help in parameters_meta via Parameter for output Parameters" in {
    val path = pathFromBasename("compiler", "help_output_params.wdl")
    val args = path.toString :: cFlags
    val retval =
      Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
    val bundle = retval match {
      case SuccessIR(ir, _) => ir
      case _                => throw new Exception("unexpected")
    }

    val cgrepApplication = getApplicationByName("help_output_params_cgrep", bundle)
    cgrepApplication.outputs.iterator sameElements Vector(
        Parameter(
            "count",
            TInt,
            None,
            Vector.empty
        )
    )
  }

  it should "recognize app metadata" in {
    val path = pathFromBasename("compiler", "add_app_meta.wdl")
    val args = path.toString :: cFlags
    val retval =
      Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
    val bundle = retval match {
      case SuccessIR(ir, _) => ir
      case _                => throw new Exception("unexpected")
    }

    val cgrepApplication = getApplicationByName("add", bundle)
    cgrepApplication.attributes.iterator sameElements
      Vector(
          DeveloperNotesAttribute("Check out my sick bash expression! Three dolla signs!!!"),
          DescriptionAttribute(
              "Adds two int together. This app adds together two integers and returns the sum"
          ),
          TagsAttribute(Vector("add", "ints")),
          OpenSourceAttribute(true),
          VersionAttribute("1.0"),
          PropertiesAttribute(Map("foo" -> "bar")),
          CategoriesAttribute(Vector("Assembly")),
          DetailsAttribute(
              Map(
                  "contactEmail" -> VString("joe@dev.com"),
                  "upstreamVersion" -> VString("1.0"),
                  "upstreamAuthor" -> VString("Joe Developer"),
                  "upstreamUrl" -> VString("https://dev.com/joe"),
                  "upstreamLicenses" -> VArray(
                      Vector(
                          VString("MIT")
                      )
                  ),
                  "whatsNew" -> VArray(
                      Vector(
                          VHash(
                              Map(
                                  "version" -> VString("1.1"),
                                  "changes" -> VArray(
                                      Vector(
                                          VString("Added parameter --foo"),
                                          VString("Added cowsay easter-egg")
                                      )
                                  )
                              )
                          ),
                          VHash(
                              Map(
                                  "version" -> VString("1.0"),
                                  "changes" -> VArray(
                                      Vector(
                                          VString("Initial version")
                                      )
                                  )
                              )
                          )
                      )
                  )
              )
          ),
          TitleAttribute("Add Ints"),
          TypesAttribute(Vector("Adder"))
      )
  }

  it should "recognize runtime hints" in {
    val path = pathFromBasename("compiler", "add_runtime_hints.wdl")
    val args = path.toString :: cFlags
    val retval =
      Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
    val bundle = retval match {
      case SuccessIR(ir, _) => ir
      case _                => throw new Exception("unexpected")
    }

    val cgrepApplication = getApplicationByName("add_runtime_hints", bundle)
    cgrepApplication.requirements.iterator sameElements
      Vector(
          IgnoreReuseRequirement(true),
          RestartRequirement(
              max = Some(5),
              default = Some(1),
              errors = Map("UnresponsiveWorker" -> 2, "ExecutionError" -> 2)
          ),
          TimeoutRequirement(hours = Some(12), minutes = Some(30)),
          AccessRequirement(network = Vector("*"), developer = Some(true))
      )
  }

  it should "ignore dx_instance_type when evaluating runtime hints" in {
    val path = pathFromBasename("compiler", "instance_type_test.wdl")
    val args = path.toString :: cFlags
    val retval =
      Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
    retval match {
      case SuccessIR(ir, _) => ir
      case _                => throw new Exception("unexpected")
    }
  }

  it should "handle an empty workflow" in {
    val path = pathFromBasename("util", "empty_workflow.wdl")
    val args = path.toString :: cFlags
    val retval =
      Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
  }

  it should "handle structs" in {
    val path = pathFromBasename("struct", "Person.wdl")
    val args = path.toString :: cFlags
    val retval =
      Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
  }

  it should "recognize that an argument with a default can be omitted at the call site" in {
    val path = pathFromBasename("compiler", "call_level2.wdl")
    val args = path.toString :: cFlags
    val retval = Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
  }

  it should "check for reserved symbols" in {
    val path = pathFromBasename("compiler", "reserved.wdl")
    val args = path.toString :: cFlags
    val retval = Main.compile(args.toVector)
    inside(retval) {
      case Failure(_, Some(e)) =>
        e.getMessage should include("using the substring '___'")
    }
  }

  it should "do nested scatters" in {
    val path = pathFromBasename("compiler", "nested_scatter.wdl")
    val args = path.toString :: cFlags
    val retval = Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
  }

  it should "handle struct imported several times" in {
    val path = pathFromBasename("struct/struct_imported_twice", "file3.wdl")
    val args = path.toString :: cFlags
    val retval = Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
  }

  it should "handle file constants in a workflow" in {
    val path = pathFromBasename("compiler", "wf_constants.wdl")
    val args = path.toString :: cFlags
    val retval = Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
  }

  it should "respect import flag" in {
    val path = pathFromBasename("compiler/imports", "A.wdl")
    val libraryPath = path.getParent.resolve("lib")
    val args = path.toString :: "--imports" :: libraryPath.toString :: cFlags
    val retval = Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
  }

  it should "respect import -p flag" in {
    val path = pathFromBasename("compiler/imports", "A.wdl")
    val libraryPath = path.getParent.resolve("lib")
    val args = path.toString :: "--p" :: libraryPath.toString :: cFlags
    val retval = Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
  }

  it should "pass environment between deep stages" in {
    val path = pathFromBasename("compiler", "environment_passing_deep_nesting.wdl")
    val args = path.toString :: cFlags
    val retval = Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
  }

  it should "handle multiple struct definitions" in {
    val path = pathFromBasename("struct/DEVEX-1196-struct-resolution-wrong-order", "file3.wdl")
    val args = path.toString :: cFlags
    val retval = Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
  }

  it should "retain all characters in a WDL task" in {
    val path = pathFromBasename("bugs", "missing_chars_in_task.wdl")
    val args = path.toString :: cFlags
    //                                      :: "--verbose"
    //                                      :: "--verboseKey" :: "GenerateIR"
    val retval = Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]

    val commandSection =
      """|  command <<<
         |    echo 1 hello world | sed 's/world/wdl/'
         |    echo 2 hello \
         |    world \
         |    | sed 's/world/wdl/'
         |    echo 3 hello \
         |    world | \
         |    sed 's/world/wdl/'
         |  >>>
         |""".stripMargin

    inside(retval) {
      case SuccessIR(bundle, _) =>
        bundle.allCallables.size shouldBe 1
        val (_, callable) = bundle.allCallables.head
        callable shouldBe a[Application]
        val task = callable.asInstanceOf[Application]
        val generator = WdlGenerator()
        val wdlDoc = task.document match {
          case WdlDocumentSource(doc, _) => doc
          case _                         => throw new Exception("expected a WDL document")
        }
        val taskSource = generator.generateDocument(wdlDoc).mkString("\n")
        taskSource should include(commandSection)
    }
  }

  it should "correctly flatten a workflow with imports" in {
    val path = pathFromBasename("compiler", "wf_to_flatten.wdl")
    val args = path.toString :: cFlags
    val retval = Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
  }

  it should "detect a request for GPU" in {
    val path = pathFromBasename("compiler", "GPU.wdl")
    val args = path.toString :: cFlags
    //                                      :: "--verbose"
    //                                      :: "--verboseKey" :: "GenerateIR"
    val retval = Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]

    inside(retval) {
      case SuccessIR(bundle, _) =>
        bundle.allCallables.size shouldBe 1
        val (_, callable) = bundle.allCallables.head
        callable shouldBe a[Application]
        val task = callable.asInstanceOf[Application]
        task.instanceType shouldBe StaticInstanceType(Some("mem3_ssd1_gpu_x8"),
                                                      None,
                                                      None,
                                                      None,
                                                      None,
                                                      None)
    }
  }

  it should "compile a scatter with a sub-workflow that has an optional argument" in {
    val path = pathFromBasename("compiler", "scatter_subworkflow_with_optional.wdl")
    val args = path.toString :: cFlags
    //                                      :: "--verbose"
    //                                      :: "--verboseKey" :: "GenerateIR"
    val retval = Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]

    val bundle = retval match {
      case SuccessIR(bundle, _) => bundle
      case _                    => throw new Exception("unexpected")
    }

    val wfs: Vector[Workflow] = bundle.allCallables.flatMap {
      case (_, wf: Workflow) if wf.locked && wf.level == Level.Sub => Some(wf)
      case (_, _)                                                  => None
    }.toVector
    wfs.length shouldBe 1
    val wf = wfs.head

    val samtools = wf.inputs.find { case (cVar, _) => cVar.name == "samtools_memory" }
    inside(samtools) {
      /*case Some((cVar, _)) =>
       cVar.wdlType shouldBe (TOptional(TString))*/
      case None => ()
    }
  }
  it should "compile a workflow taking arguments from a Pair" in {
    val path = pathFromBasename("draft2", "pair.wdl")
    val args = path.toString :: cFlags
    //                                      :: "--verbose"
    //                                      :: "--verboseKey" :: "GenerateIR"
    val retval = Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
  }

  it should "pass as subworkflows do not have expression statement in output block" in {
    val path = pathFromBasename("subworkflows", basename = "trains.wdl")
    val args = path.toString :: cFlags
    val retval = Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
  }

  // this is currently failing.
  it should "pass with subworkflows having expression" in {
    val path = pathFromBasename("subworkflows", basename = "ensure_trains.wdl")

    /* ensure_trains workflow
     * trains        workflow
     * check_route   workflow
     * concat        task
     */
    val args = path.toString :: cFlags
    //          :: "--verbose"
    //          :: "--verboseKey" :: "GenerateIR"
    val retval = Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
  }

  it should "recognize workflow metadata" in {
    val path = pathFromBasename("compiler", "wf_meta.wdl")
    val args = path.toString :: cFlags
    val retval = Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
    val bundle = retval match {
      case SuccessIR(ir, _) => ir
      case _                => throw new Exception("unexpected")
    }
    val workflow = bundle.primaryCallable match {
      case Some(wf: Workflow) => wf
      case _                  => throw new Exception("primaryCallable is not a workflow")
    }
    workflow.attributes.iterator sameElements
      Vector(
          DescriptionAttribute("This is a workflow that defines some metadata"),
          TagsAttribute(Vector("foo", "bar")),
          VersionAttribute("1.0"),
          PropertiesAttribute(Map("foo" -> "bar")),
          DetailsAttribute(Map("whatsNew" -> VString("v1.0: First release"))),
          TitleAttribute("Workflow with metadata"),
          TypesAttribute(Vector("calculator")),
          SummaryAttribute("A workflow that defines some metadata")
      )
  }

  it should "recognize workflow parameter metadata" in {
    val path = pathFromBasename("compiler", "wf_param_meta.wdl")
    val args = path.toString :: cFlags
    val retval = Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
    val bundle = retval match {
      case SuccessIR(ir, _) => ir
      case _                => throw new Exception("unexpected")
    }
    val workflow = bundle.primaryCallable match {
      case Some(wf: Workflow) => wf
      case _                  => throw new Exception("primaryCallable is not a workflow")
    }
    val input_cvars: Vector[Parameter] = workflow.inputs.map {
      case (c: Parameter, _) => c
      case _                 => throw new Exception("Invalid workflow input ${other}")
    }
    input_cvars.sortWith(_.name < _.name) shouldBe Vector(
        Parameter(
            "x",
            TInt,
            Some(VInt(3)),
            Vector(
                LabelAttribute("Left-hand side"),
                DefaultAttribute(VInt(3))
            )
        ),
        Parameter(
            "y",
            TInt,
            Some(VInt(5)),
            Vector(
                LabelAttribute("Right-hand side"),
                DefaultAttribute(VInt(5))
            )
        )
    )
  }

  it should "handle adjunct files in workflows and tasks" in {
    val path = pathFromBasename("compiler", "wf_readme.wdl")
    val args = path.toString :: cFlags
    val retval = Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]

    val bundle = retval match {
      case SuccessIR(ir, _) => ir
      case _                => throw new Exception("unexpected")
    }

    val workflow = bundle.primaryCallable match {
      case Some(wf: Workflow) => wf
      case _                  => throw new Exception("primaryCallable is not a workflow")
    }
    workflow.attributes match {
      case Vector(DescriptionAttribute(desc)) =>
        desc shouldBe "This is the readme for the wf_linear workflow."
      case _ =>
        throw new Exception("Expected one workflow description attribute")
    }

    val addApp = getApplicationByName("add", bundle)
    addApp.attributes.size shouldBe 2
    addApp.attributes.foreach {
      case DescriptionAttribute(text) =>
        text shouldBe "This is the readme for the wf_linear add task."
      case DeveloperNotesAttribute(text) =>
        text shouldBe "Developer notes defined in WDL"
      case other => throw new Exception(s"Invalid  for add task ${other}")
    }

    val mulApp = getApplicationByName("mul", bundle)
    mulApp.attributes match {
      case Vector(DescriptionAttribute(text)) =>
        text shouldBe "Description defined in WDL"
      case other =>
        throw new Exception(s"expected one description attribute, not ${other}")
    }

    val incApp = getApplicationByName("inc", bundle)
    incApp.attributes.size shouldBe 0
  }

  it should "work correctly with pairs in a scatter" taggedAs EdgeTest in {
    val path = pathFromBasename("subworkflows", basename = "scatter_subworkflow_with_optional.wdl")
    val cFlagsNotQuiet = cFlags.filter(_ != "-quiet")
    val args = path.toString :: cFlagsNotQuiet
    //          :: "--verbose"
    //          :: "--verboseKey" :: "GenerateIR"
    val retval = Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
  }

  // Check parameter_meta pattern: ["array"]
  it should "recognize pattern in parameters_meta via WDL" in {
    val path = pathFromBasename("compiler", "pattern_params.wdl")
    val args = path.toString :: cFlags
    val retval = Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
    val bundle = retval match {
      case SuccessIR(ir, _) => ir
      case _                => throw new Exception("unexpected")
    }

    val cgrepTask = getApplicationByName("pattern_params_cgrep", bundle)
    cgrepTask.inputs.map(param => param.name -> param.attributes).iterator sameElements Vector(
        "in_file" -> Vector(
            HelpAttribute("The input file to be searched"),
            PatternsArray(Vector("*.txt", "*.tsv")),
            GroupAttribute("Common"),
            LabelAttribute("Input file")
        ),
        "pattern" -> Vector(
            HelpAttribute("The pattern to use to search in_file"),
            GroupAttribute("Common"),
            LabelAttribute("Search pattern")
        ),
        "out_file" -> Vector(
            PatternsArray(
                Vector("*.txt", "*.tsv")
            ),
            GroupAttribute("Common"),
            LabelAttribute("Output file")
        )
    )
  }

  // Check parameter_meta pattern: {"object"}
  it should "recognize pattern object in parameters_meta via WDL" in {
    val path = pathFromBasename("compiler", "pattern_obj_params.wdl")
    val args = path.toString :: cFlags
    val retval = Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
    val bundle = retval match {
      case SuccessIR(ir, _) => ir
      case _                => throw new Exception("unexpected")
    }

    val cgrepTask = getApplicationByName("pattern_params_obj_cgrep", bundle)
    cgrepTask.inputs.map(param => param.name -> param.attributes).iterator sameElements Vector(
        "in_file" -> Vector(
            HelpAttribute("The input file to be searched"),
            PatternsAttribute(
                PatternsObject(
                    Vector("*.txt", "*.tsv"),
                    Some("file"),
                    Vector("foo", "bar")
                )
            ),
            GroupAttribute("Common"),
            LabelAttribute("Input file")
        ),
        "pattern" -> Vector(
            HelpAttribute("The pattern to use to search in_file"),
            GroupAttribute("Common"),
            LabelAttribute("Search pattern")
        ),
        "out_file" -> Vector(
            PatternsArray(Vector("*.txt", "*.tsv")),
            GroupAttribute("Common"),
            LabelAttribute("Output file")
        )
    )
  }

  it should "recognize help, group, and label in parameters_meta via WDL" in {
    val path = pathFromBasename("compiler", "help_input_params.wdl")
    val args = path.toString :: cFlags
    val retval = Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
    val bundle = retval match {
      case SuccessIR(ir, _) => ir
      case _                => throw new Exception("unexpected")
    }

    val cgrepTask = getApplicationByName("help_input_params_cgrep", bundle)
    cgrepTask.inputs.map(param => param.name -> param.attributes).iterator sameElements Vector(
        "in_file" -> Vector(
            HelpAttribute("The input file to be searched"),
            GroupAttribute("Common"),
            LabelAttribute("Input file")
        ),
        "pattern" -> Vector(
            DescriptionAttribute("The pattern to use to search in_file"),
            GroupAttribute("Common"),
            LabelAttribute("Search pattern")
        ),
        "s" -> Vector(HelpAttribute("This is help for s"))
    )

    val diffTask = getApplicationByName("help_input_params_diff", bundle)
    diffTask.inputs.map(param => param.name -> param.attributes).iterator sameElements Vector(
        "a" -> Vector(
            HelpAttribute("lefthand file"),
            GroupAttribute("Files"),
            LabelAttribute("File A")
        ),
        "b" -> Vector(
            HelpAttribute("righthand file"),
            GroupAttribute("Files"),
            LabelAttribute("File B")
        )
    )
  }

  // `paramter: "stream"` should not be converted to an attribute - it is only accessed at runtime
  it should "ignore stream attribute" in {
    val path = pathFromBasename("compiler", "streaming_files.wdl")
    val args = path.toString :: cFlags
    val retval = Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
    val bundle = retval match {
      case SuccessIR(ir, _) => ir
      case _                => throw new Exception("unexpected")
    }
    val cgrepTask = getApplicationByName("cgrep", bundle)
    cgrepTask.inputs.map(param => param.name -> param.attributes).iterator sameElements Vector(
        "in_file" -> Vector()
    )
    val diffTask = getApplicationByName("diff", bundle)
    diffTask.inputs.map(param => param.name -> param.attributes).iterator sameElements Vector(
        "a" -> Vector(),
        "b" -> Vector()
    )
  }

  // `paramter: {stream: true}` should not be converted to an attribute - it is only accessed at runtime
  it should "ignore the streaming object annotation" in {
    val path = pathFromBasename("compiler", "streaming_files_obj.wdl")
    val args = path.toString :: cFlags
    val retval =
      Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
    val bundle = retval match {
      case SuccessIR(ir, _) => ir
      case _                => throw new Exception("unexpected")
    }
    val cgrepTask = getApplicationByName("cgrep", bundle)
    cgrepTask.inputs.map(param => param.name -> param.attributes).iterator sameElements Vector(
        "in_file" -> Vector()
    )
    val diffTask = getApplicationByName("diff", bundle)
    diffTask.inputs.map(param => param.name -> param.attributes).iterator sameElements Vector(
        "a" -> Vector(),
        "b" -> Vector()
    )
  }

  it should "ignore the streaming annotation for wdl draft2" in {
    val path = pathFromBasename("draft2", "streaming.wdl")
    val args = path.toString :: cFlags
    val retval =
      Main.compile(args.toVector)
    retval shouldBe a[SuccessIR]
    val bundle = retval match {
      case SuccessIR(ir, _) => ir
      case _                => throw new Exception("unexpected")
    }
    val diffTask = getApplicationByName("diff", bundle)
    diffTask.inputs.map(param => param.name -> param.attributes).iterator sameElements Vector(
        "a" -> Vector(),
        "b" -> Vector()
    )
  }
}
