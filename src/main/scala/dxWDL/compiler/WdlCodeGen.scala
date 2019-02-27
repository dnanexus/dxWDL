package dxWDL.compiler

import scala.util.matching.Regex
import wom.graph._
import wom.types._
import wom.values._

import dxWDL.util._


// A bunch of WDL source lines
case class WdlCodeSnippet(value : String)

case class WdlCodeGen(verbose: Verbose) {

    private def genDefaultValueOfType(wdlType: WomType) : WomValue = {
        wdlType match {
            case WomBooleanType => WomBoolean(true)
            case WomIntegerType => WomInteger(0)
            case WomFloatType => WomFloat(0.0)
            case WomStringType => WomString("")
            case WomSingleFileType => WomSingleFile("")

                // We could convert an optional to a null value, but that causes
                // problems for the pretty printer.
                // WomOptionalValue(wdlType, None)
            case WomOptionalType(t) => genDefaultValueOfType(t)

            case WomObjectType =>
                // This fails when trying to do a 'toWomString'
                // operation.
                //WomObject(Map("_" -> WomString("_")), WomObjectType)
                WomObject(Map.empty)


                // The WomMap type HAS to appear before the array types, because
                // otherwise it is coerced into an array. The map has to
                // contain at least one key-value pair, otherwise you get a type error.
            case WomMapType(keyType, valueType) =>
                val k = genDefaultValueOfType(keyType)
                val v = genDefaultValueOfType(valueType)
                WomMap(WomMapType(keyType, valueType), Map(k -> v))

                // an empty array
            case WomMaybeEmptyArrayType(t) =>
                WomArray(WomMaybeEmptyArrayType(t), List())

                // Non empty array
            case WomNonEmptyArrayType(t) =>
                WomArray(WomNonEmptyArrayType(t), List(genDefaultValueOfType(t)))

            case WomPairType(lType, rType) => WomPair(genDefaultValueOfType(lType),
                                                      genDefaultValueOfType(rType))
            case _ => throw new Exception(s"Unhandled type ${wdlType.toDisplayString}")
        }
    }

/*
Create a stub for an applet. This is an empty task
that includes the input and output definitions. It is used
to allow linking to native DNAx applets (and workflows in the future).

For example, the stub for the Add task:
task Add {
    input {
      Int a
      Int b
    }
    command {
    command <<<
        python -c "print(${a} + ${b})"
    >>>
    output {
        Int result = read_int(stdout())
    }
}

is:
task Add {
   input {
     Int a
     Int b
   }
   command {}
   output {
       Int result
   }
  }
*/
    def appletStub(callable: IR.Callable,
                   language: Language.Value) : WdlCodeSnippet = {
        /*Utils.trace(verbose.on,
                    s"""|genAppletStub  callable=${callable.name}
                        |  inputs= ${callable.inputVars.map(_.name)}
                        |  outputs= ${callable.outputVars.map(_.name)}"""
                        .stripMargin)*/

        val inputs = callable.inputVars.map{ cVar =>
            s"    ${cVar.womType.toDisplayString} ${cVar.name}"
        }.mkString("\n")

        val outputs = callable.outputVars.map{ cVar =>
            val defaultVal = genDefaultValueOfType(cVar.womType)
            s"    ${cVar.womType.toDisplayString} ${cVar.name} = ${defaultVal.toWomString}"
        }.mkString("\n")

        language match {
            case Language.WDLvDraft2 =>
                // Draft-2 does not support the input block.
                WdlCodeSnippet(
                    s"""|task ${callable.name} {
                        |${inputs}
                        |
                        |  command {}
                        |  output {
                        |${outputs}
                        |  }
                        |}""".stripMargin
                )
            case Language.WDLv1_0 =>
                WdlCodeSnippet(
                    s"""|task ${callable.name} {
                        |  input {
                        |${inputs}
                        |  }
                        |  command {}
                        |  output {
                        |${outputs}
                        |  }
                        |}""".stripMargin
                )
            case other =>
                throw new Exception(s"Unsupported language version ${other}")
        }
    }

    // A workflow can import other libraries:
    //
    // import "library.wdl" as lib
    // workflow foo {
    //   call lib.Multiply as mul { ... }
    //   call lib.Add { ... }
    //   call lib.Nice as nice { ... }
    // }
    //
    // rewrite the workflow, and remove the calls to external libraries.
    //
    // workflow foo {
    //   call Multiply as mul { ... }
    //   call Add { ... }
    //   call Nice as nice { ... }
    // }
    //
    private val callLibrary: Regex = "^(\\s*)call(\\s+)(\\w+)\\.(\\w+)(\\s+)(.+)".r
    private def flattenWorkflow(wdlWfSource: String) : String = {
        val originalLines = wdlWfSource.split("\n").toList
        val cleanLines = originalLines.map { line =>
            val allMatches = callLibrary.findAllMatchIn(line).toList
            assert(allMatches.size <= 1)
            if (allMatches.isEmpty) {
                line
            } else {
                val m = allMatches(0)
                val callee : String = m.group(4)
                val rest = m.group(6)
                s"call ${callee} ${rest}"
            }
        }
        cleanLines.mkString("\n")
    }

        // A self contained WDL workflow
    def versionString(language: Language.Value) : String = {
        language match {
            case Language.WDLvDraft2 => ""
            case Language.WDLv1_0 => "version 1.0"
            case other =>
                throw new Exception(s"Unsupported language version ${other}")
        }
    }

    // A workflow must have definitions for all the tasks it
    // calls. However, a scatter calls tasks, that are missing from
    // the WDL file we generate. To ameliorate this, we add stubs for
    // called tasks. The generated tasks are named by their
    // unqualified names, not their fully-qualified names. This works
    // because the WDL workflow must be "flattenable".
    def standAloneWorkflow( originalWorkflowSource: String,
                            allCalls : Vector[IR.Callable],
                            language: Language.Value) : WdlCodeSnippet = {
        val taskStubs: Map[String, WdlCodeSnippet] =
            allCalls.foldLeft(Map.empty[String, WdlCodeSnippet]) { case (accu, callable) =>
                if (accu contains callable.name) {
                    // we have already created a stub for this call
                    accu
                } else {
                    // no existing stub, create it
                    val taskSourceCode =  appletStub(callable, language)
                    accu + (callable.name -> taskSourceCode)
                }
            }
        val tasks = taskStubs.map{case (name, wdlCode) => wdlCode.value}.mkString("\n\n")

        val wfWithoutImportCalls = flattenWorkflow(originalWorkflowSource)
        val wdlWfSource = s"""|${versionString(language)}
                              |
                              |${tasks}
                              |
                              |${wfWithoutImportCalls}
                              |
                              |""".stripMargin

        // Make sure this is actually valid WDL 1.0
        ParseWomSourceFile.validateWdlWorkflow(wdlWfSource, language)

        WdlCodeSnippet(wdlWfSource)
    }

    // We have an unlocked workflow, and need an applet to aggregate all the inputs.
    // If this inputs are:
    //   Int quality
    //   String referenceGenome
    //
    // We need an applet like this to evaluate:
    //
    // workflow common {
    //   input {
    //     Int quality
    //     String referenceGenome
    //   }
    //   output {
    //     Int xxx_quality = quality
    //     String xxx_referenceGenome = referenceGenome
    //   }
    // }
    //
    // We need to circumvent the WDL restriction that an output cannot have the same name
    // as an output.
    def evalOnlyWfFromWorkflowInputs(name : String,
                                     inputVars: Vector[IR.CVar],
                                     language: Language.Value) : WdlCodeSnippet = {
        val inputs: Vector[String] = inputVars.map { cVar =>
            s"${cVar.womType.toDisplayString} ${cVar.dxVarName}"
        }
        val prefix = Utils.OUTPUT_VAR_PREFIX
        val outputs: Vector[String] = inputVars.map { cVar =>
            s"${cVar.womType.toDisplayString} ${prefix}${cVar.dxVarName} = ${cVar.dxVarName}"
        }

        val twoSpaces = Utils.genNSpaces(2)
        val fourSpaces = Utils.genNSpaces(4)
        val code = language match {
            case Language.WDLvDraft2 =>
                // Draft-2 does not support the input block.
                s"""|workflow ${name} {
                    |${inputs.map(x => twoSpaces + x).mkString("\n")}
                    |  output {
                    |${outputs.map(x => fourSpaces + x).mkString("\n")}
                    |  }
                    |}""".stripMargin
            case Language.WDLv1_0 =>
                s"""|${versionString(language)}
                    |
                    |workflow ${name} {
                    |  input {
                    |${inputs.map(x => fourSpaces + x).mkString("\n")}
                    |  }
                    |  output {
                    |${outputs.map(x => fourSpaces + x).mkString("\n")}
                    |  }
                    |}""".stripMargin
            case other =>
                throw new Exception(s"Unsupported language version ${other}")
        }
        WdlCodeSnippet(code)
    }

    // The inputs and outputs for this mini-workflows are likely to
    // be the same. We add a unique prefix to the outputs, which
    // is stripped at runtime.
    //
    // For example, this is illegal:
    // workflow foo {
    //   input {
    //     Int errorRate
    //   }
    //   output {
    //     Int errorRate = errorRate
    //   }
    // }
    //
    // So we rewrite it as:
    // workflow foo {
    //   input {
    //     Int errorRate
    //   }
    //   output {
    //     Int xyz_errorRate = errorRate
    //   }
    // }
    //
    // At runtime we strip the "xyz_" prefix.
    //
    // Note: this still doesn't handle the case of outputs
    // referencing each other. For example, foo will fail.
    //
    // workflow foo {
    //   input {
    //      Int a
    //   }
    //   output {
    //      Int x = a + 1
    //      Int y = x + 7
    //   }
    // }
    def evalOnlyWfFromWorkflowOutputs(name : String,
                                      inputVars: Vector[IR.CVar],
                                      outputNodes: Vector[GraphOutputNode],
                                      language: Language.Value) : WdlCodeSnippet = {
        val inputs: Vector[String] = inputVars.map { cVar =>
            s"${cVar.womType.toDisplayString} ${cVar.dxVarName}"
        }
        val prefix = Utils.OUTPUT_VAR_PREFIX
        val outputs: Vector[String] = outputNodes.map {
            case PortBasedGraphOutputNode(id, womType, sourcePort) =>
                val name = id.workflowLocalName
                s"${womType.toDisplayString} ${prefix}${name} = ${name}"
            case expr :ExpressionBasedGraphOutputNode =>
                val name = expr.identifier.localName.value
                s"${expr.womType.toDisplayString} ${prefix}${name} = ${expr.womExpression.sourceString}"
            case other =>
                throw new Exception(s"unhandled output ${other}")
        }

        val twoSpaces = Utils.genNSpaces(2)
        val fourSpaces = Utils.genNSpaces(4)
        val code = language match {
            case Language.WDLvDraft2 =>
                // Draft-2 does not support the input block.
                s"""|workflow ${name} {
                    |${inputs.map(x => twoSpaces + x).mkString("\n")}
                    |  output {
                    |${outputs.map(x => fourSpaces + x).mkString("\n")}
                    |  }
                    |}""".stripMargin
            case Language.WDLv1_0 =>
                s"""|${versionString(language)}
                    |
                    |workflow ${name} {
                    |  input {
                    |${inputs.map(x => fourSpaces + x).mkString("\n")}
                    |  }
                    |  output {
                    |${outputs.map(x => fourSpaces + x).mkString("\n")}
                    |  }
                    |}""".stripMargin
            case other =>
                throw new Exception(s"Unsupported language version ${other}")
        }
        WdlCodeSnippet(code)
    }
}
