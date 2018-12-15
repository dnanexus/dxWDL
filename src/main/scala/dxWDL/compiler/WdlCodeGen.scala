package dxWDL.compiler

import wom.types._
import wom.values._
import wom.graph._
import wom.graph.expression._

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

    def apply(cVar : IR.CVar, ioRef : IORef.Value) : String = {
        ioRef match {
            case IORef.Input =>
                s"${cVar.womType.toDisplayString} ${cVar.dxVarName}"
            case IORef.Output if cVar.dxVarName == cVar.name =>
                s"${cVar.womType.toDisplayString} ${cVar.name}___ = ${cVar.name}"
            case IORef.Output =>
                s"${cVar.womType.toDisplayString} ${cVar.dxVarName} = ${cVar.name}"
        }
    }


    // write a set of nodes into the body of a WDL workflow.
    // Rename variables from a provided dictionary. For example:
    //
    // Translations
    //   Map(add.result -> add___result)
    //
    // Original block
    //   Int z = add.result + 1
    //   call mul { input: a=z, b=5 }
    //
    // Result block
    //   Int z = add___result + 1
    //   call mul { input: a=z, b=5 }
    def blockRenameVars(block: Block,
                        wordTranslations : Map[String, String]) : String = {
        val va = new VarAnalysis(wordTranslations, verbose)

        val lines = block.nodes.flatMap {
            case ssc : ScatterNode =>
                throw new NotImplementedError("scatter")

            case cond : ConditionalNode =>
                throw new NotImplementedError("condition")

            case call : CommandCallNode => {
                val inputs : String = call.upstream.toVector.collect {
                    case expr : TaskCallInputExpressionNode =>
                        val rnExpr = va.exprRenameVars(expr.womExpression)
                        s"${expr.identifier.localName.value} = ${rnExpr}"
                }.mkString(", ")
                Some(s"call ${call.identifier.localName.value} { input: ${inputs} }")
            }

            case expr : ExposedExpressionNode => {
                val rnExpr = va.exprRenameVars(expr.womExpression)
                Some(s"${expr.womType.toDisplayString} ${expr.identifier.localName.value} = ${rnExpr}")
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

    // a debugging method
    def blockToWdlSource(block: Block) : String =
        blockRenameVars(block, Map.empty)
}
