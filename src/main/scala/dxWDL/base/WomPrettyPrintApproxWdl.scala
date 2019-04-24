/** Pretty printing WOM as, approximately, the original WDL.
  */
package dxWDL.base

import wom.callable.Callable._
import wom.graph._
import wom.graph.expression._
import wom.types._

object WomPrettyPrintApproxWdl {

    // Get a human readable type name
    // Int ->   "Int"
    // Array[Int] -> "Array[Int]"
    def typeName(t: WomType) : String = {
        t match {
            // Base case: primitive types.
            case WomNothingType => "Nothing"
            case WomBooleanType => "Boolean"
            case WomIntegerType => "Int"
            case WomLongType => "Long"
            case WomFloatType => "Float"
            case WomStringType => "String"
            case WomSingleFileType => "File"

            // compound types
            case WomMaybeEmptyArrayType(memberType) =>
                val inner = typeName(memberType)
                s"Array[${inner}]"
            case WomMapType(keyType, valueType) =>
                val k = typeName(keyType)
                val v = typeName(valueType)
                s"Map[$k, $v]"
            case WomNonEmptyArrayType(memberType) =>
                val inner = typeName(memberType)
                s"Array[${inner}]+"
            case WomOptionalType(memberType) =>
                val inner = typeName(memberType)
                s"$inner?"
            case WomPairType(lType, rType) =>
                val ls = typeName(lType)
                val rs = typeName(rType)
                s"Pair[$ls, $rs]"

            // structs
            case WomCompositeType(_, None) =>
                throw new Exception("unnamed struct")
            case WomCompositeType(_, Some(structName)) =>
                structName

            // catch-all for other types not currently supported
            case _ =>
                throw new Exception(s"Unsupported WOM type ${t}, ${t.stableName}")
        }
    }

    // Convert a fully qualified name to a local name.
    // Examples:
    //   SOURCE         RESULT
    //   lib.concat     concat
    //   lib.sum_list   sum_list
    private def getUnqualifiedName(fqn: String) : String = {
        if (fqn contains ".")
            fqn.split("\\.").last
        else
            fqn
    }

    private def applyGNode(node: GraphNode,
                           indent : String) : Option[String] = {
        node match {
            case sct : ScatterNode =>
                val varNames = sct.scatterVariableNodes.map{svn => svn.identifier.localName.value}
                assert(varNames.size == 1)
                val varName = varNames.head
                val svNode: ScatterVariableNode = sct.scatterVariableNodes.head
                val collection = svNode.scatterExpressionNode.womExpression.sourceString
                val innerBlock =
                    sct.innerGraph.nodes.flatMap{ node =>
                        applyGNode(node, indent + "  ")
                    }.mkString("\n")
                Some(s"""|${indent}scatter (${varName} in ${collection}) {
                         |${innerBlock}
                         |${indent}}
                         |""".stripMargin)

            case cnd : ConditionalNode =>
                val innerBlock =
                    cnd.innerGraph.nodes.flatMap{ node =>
                        applyGNode(node, indent + "  ")
                    }.mkString("\n")
                Some(s"""|${indent}if (${cnd.conditionExpression.womExpression.sourceString}) {
                         |${innerBlock}
                         |${indent}}
                         |""".stripMargin)

            case call : CommandCallNode =>
                val inputNames = call.upstream.collect{
                    case exprNode: ExpressionNode =>
                        val paramName = getUnqualifiedName(exprNode.identifier.localName.value)
                        s"${paramName} = ${exprNode.womExpression.sourceString}"
                }.mkString(",")
                val calleeName = getUnqualifiedName(call.callable.name)
                val callName = call.identifier.localName.value
                val inputs =
                    if (inputNames.isEmpty) ""
                    else s"{ input: ${inputNames} }"
                if (callName == calleeName)
                    Some(s"${indent}call ${callName} ${inputs}")
                else
                    Some(s"${indent}call ${calleeName} as ${callName} ${inputs}")

            case expr :ExpressionBasedGraphOutputNode =>
                val exprSource = expr.womExpression.sourceString
                Some(s"${indent}${typeName(expr.womType)} ${expr.identifier.localName.value} = ${exprSource}")

            case expr : ExposedExpressionNode =>
                Some(s"${indent}${expr.identifier.localName.value} =  ${expr.womExpression.sourceString}")

            case expr : ExpressionNode =>
                //Some(expr.womExpression.sourceString)
                None

            case _ : OuterGraphInputNode =>
                None

            case PortBasedGraphOutputNode(id, womType, sourcePort) =>
                None

            case _ : GraphInputNode =>
                None

            case other =>
                Some(other.toString)
        }
    }

    def apply(node: GraphNode) : String = {
        applyGNode(node, "") match {
            case None => ""
            case Some(x) => x
        }
    }

    def apply(inputDef : InputDefinition) : String = {
        inputDef match {
            case RequiredInputDefinition(iName, womType, _, _) =>
                s"${typeName(womType)} ${iName}"

            case OverridableInputDefinitionWithDefault(iName, womType, defaultExpr, _, _) =>
                s"${typeName(womType)} ${iName} = ${defaultExpr.sourceString}"

            // An input whose value should always be calculated from the default, and is
            // not allowed to be overridden.
            case FixedInputDefinitionWithDefault(iName, womType, defaultExpr, _, _) =>
                s"${typeName(womType)} ${iName} = ${defaultExpr.sourceString}"

            case OptionalInputDefinition(iName, WomOptionalType(womType), _, _) =>
                s"${typeName(womType)}? ${iName}"

            case other =>
                throw new Exception(s"${other} not handled")
        }
    }
}
