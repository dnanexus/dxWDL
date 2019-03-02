package dxWDL.compiler

import scala.util.matching.Regex
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

    def genDnanexusAppletStub(id: String,
                              appletName: String,
                              inputSpec: Map[String, WomType],
                              outputSpec: Map[String, WomType],
                              language: Language.Value) : WdlCodeSnippet = {
        val inputs = inputSpec.map{ case (name, womType) =>
            s"    ${womType.toDisplayString} ${name}"
        }.mkString("\n")
        val outputs = outputSpec.map{ case (name, womType) =>
            val defaultVal = genDefaultValueOfType(womType)
            s"    ${womType.toDisplayString} $name} = ${defaultVal.toWomString}"
        }.mkString("\n")

        val runtimeSection =
            s"""|  runtime {
                |     type : "native"
                |     id : "${id}"
                |  }""".stripMargin

        language match {
            case Language.WDLvDraft2 =>
                // Draft-2 does not support the input block.
                WdlCodeSnippet(
                    s"""|task ${appletName} {
                        |${inputs}
                        |
                        |  command {}
                        |  output {
                        |${outputs}
                        |  }
                        |${runtimeSection}
                        |}""".stripMargin
                )
            case Language.WDLv1_0 =>
                WdlCodeSnippet(
                    s"""|task ${appletName} {
                        |  input {
                        |${inputs}
                        |  }
                        |  command {}
                        |  output {
                        |${outputs}
                        |  }
                        |${runtimeSection}
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
}
