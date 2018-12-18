package dxWDL.compiler

import wom.expression.WomExpression
import wom.graph._
import wom.graph.expression._
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
    def appletStub(callable: IR.Callable) : WdlCodeSnippet = {
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

    // write a set of nodes into the body of a WDL workflow.
    //
    def blockToWdlSource(block: Block) : String = {
        val lines = block.nodes.flatMap {
            case ssc : ScatterNode =>
                throw new NotImplementedError("scatter")

            case cond : ConditionalNode =>
                throw new NotImplementedError("condition")

            case call : CommandCallNode => {
                val inputs : String = call.upstream.toVector.collect {
                    case expr : TaskCallInputExpressionNode =>
                        val exprStr = expr.womExpression.sourceString
                        s"${expr.identifier.localName.value} = ${exprStr}"
                }.mkString(", ")
                Some(s"call ${call.identifier.localName.value} { input: ${inputs} }")
            }

            case expr : ExposedExpressionNode => {
                val exprStr = expr.womExpression.sourceString
                Some(s"${expr.womType.toDisplayString} ${expr.identifier.localName.value} = ${exprStr}")
            }

            case _ :TaskCallInputExpressionNode =>
                // Expressions that are inputs to a call. These are printed out as part of CommandCallNode
                // handling.
                None

            case other =>
                // ignore all other nodes, they do not map directly to a WDL
                // statement
                val otherDesc: String = WomPrettyPrint.apply(other)
                Utils.warning(verbose, s"ignored graph node ${otherDesc}")
                None
        }
        lines.mkString("\n")
    }

    // A workflow must have definitions for all the tasks it
    // calls. However, a scatter calls tasks, that are missing from
    // the WDL file we generate. To ameliorate this, we add stubs for
    // called tasks. The generated tasks are named by their
    // unqualified names, not their fully-qualified names. This works
    // because the WDL workflow must be "flattenable".
    def standAloneWorkflow( inputNodes: Vector[GraphInputNode],    // inputs
                            blocks: Vector[Block],             // blocks
                            outputNodes: Vector[GraphOutputNode],   // outputs
                            allCalls : Vector[IR.Callable]) : WdlCodeSnippet = {
        val inputs: String = inputNodes.map{
            case RequiredGraphInputNode(id, womType, _, _) =>
                s"${womType.toDisplayString} ${id.workflowLocalName}"
            case OptionalGraphInputNode(id, womType, _, _) =>
                s"${womType.toDisplayString} ${id.workflowLocalName}"
            case OptionalGraphInputNodeWithDefault(id, womType, expr : WomExpression, _, _) =>
                s"${womType.toDisplayString} ${id.workflowLocalName}"
            case other =>
                throw new Exception(s"unhandled input ${other}")
        }.mkString("\n")

        val outputs: String = outputNodes.map{
            case expr : ExpressionBasedGraphOutputNode =>
                s"${expr.womType.toDisplayString} ${expr.identifier.localName.value} = ${expr.womExpression.sourceString}"
            case other =>
                throw new Exception(s"unhandled output ${other}")
        }.mkString("\n")

        val wfBody = blocks.map{ block => blockToWdlSource(block) }
            .map{_.value}
            .mkString("\n")

        val taskStubs: Map[String, WdlCodeSnippet] =
            allCalls.foldLeft(Map.empty[String, WdlCodeSnippet]) { case (accu, callable) =>
                if (accu contains callable.name) {
                    // we have already created a stub for this call
                    accu
                } else {
                    // no existing stub, create it
                    val taskSourceCode =  appletStub(callable)
                    accu + (callable.name -> taskSourceCode)
                }
            }
        val tasks = taskStubs.map{case (name, wdlCode) => wdlCode.value}.mkString("\n\n")

        // A self contained WDL workflow
        val wdlWfSource =
            s"""|version 1.0
                |
                |workflow w {
                |  input {
                |${inputs}
                |  }
                |
                |${wfBody}
                |
                |  output {
                |${outputs}
                |  }
                |}
                |
                |${tasks}
                |""".stripMargin

        // Make sure this is actually valid WDL 1.0
        ParseWomSourceFile.validateWdlWorkflow(wdlWfSource)

        WdlCodeSnippet(wdlWfSource)
    }
}
