package dxWDL.compiler

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
        // we currently support only WDL 1.0. It should be easy to add
        // support for draft 2.
        assert(language == Language.WDLv1_0)

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

        // We are using WDL version 1.0 here. The input syntax is not
        // available prior.
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
    }


    // A workflow must have definitions for all the tasks it
    // calls. However, a scatter calls tasks, that are missing from
    // the WDL file we generate. To ameliorate this, we add stubs for
    // called tasks. The generated tasks are named by their
    // unqualified names, not their fully-qualified names. This works
    // because the WDL workflow must be "flattenable".
    def standAloneWorkflow( originalWorkflowSource: String,
                            allCalls : Vector[IR.Callable]) : WdlCodeSnippet = {
        val taskStubs: Map[String, WdlCodeSnippet] =
            allCalls.foldLeft(Map.empty[String, WdlCodeSnippet]) { case (accu, callable) =>
                if (accu contains callable.name) {
                    // we have already created a stub for this call
                    accu
                } else {
                    // no existing stub, create it
                    val taskSourceCode =  appletStub(callable, Language.WDLv1_0)
                    accu + (callable.name -> taskSourceCode)
                }
            }
        val tasks = taskStubs.map{case (name, wdlCode) => wdlCode.value}.mkString("\n\n")

        // A self contained WDL workflow
        //
        // This currently assumes WDL 1.0, due to the tasks.
        val wdlWfSource =
            s"""|version 1.0
                |
                |${tasks}
                |
                |${originalWorkflowSource}
                |
                |""".stripMargin

        // Make sure this is actually valid WDL 1.0
        ParseWomSourceFile.validateWdlWorkflow(wdlWfSource)

        WdlCodeSnippet(wdlWfSource)
    }
}
