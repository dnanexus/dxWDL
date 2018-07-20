    /*
     Split a block of statements into sub-blocks, each of which contains:
     1) at most one call
     2) at most one toplevel if/scatter block
     Note that the if/scatter block could be deeply nested. Previous compiler passes
     must make sure that there is no more a single call inside an if/scatter block.

     For example:
     call x
     Int a
     Int b
     call y
     call z
     String buf
     Float x
     scatter {
       call V
     }

     =>

     [call x, Int a, Int b]
     [call y]
     [call z]
     [String buf, Float x, scatter]
     */

package dxWDL.compiler

import dxWDL.{Verbose}
import wdl.draft2.model._

// Find all the calls inside a statement block
case class Block(statements: Vector[Scope]) {
    def head : Scope =
        statements.head

    def size : Int =
        statements.size

    def append(stmt: Scope) : Block =
        Block(statements :+ stmt)

    def append(block2: Block) : Block =
        Block(statements ++ block2.statements)

    // Check if one of the statements is a scatter/if block
    def hasSubBlock : Boolean = {
        statements.foldLeft(false) {
            case (accu, _:Scatter) => true
            case (accu, _:If) => true
            case (accu, _) => accu
        }
    }

    private def _findCalls(statements1: Seq[Scope]) : Vector[WdlCall] = {
        statements1.foldLeft(Vector.empty[WdlCall]) {
            case (accu, call:WdlCall) =>
                accu :+ call
            case (accu, ssc:Scatter) =>
                accu ++ _findCalls(ssc.children)
            case (accu, ifStmt:If) =>
                accu ++ _findCalls(ifStmt.children)
            case (accu, wf:WdlWorkflow) =>
                accu ++ _findCalls(wf.children)
            case (accu, _) =>
                accu
        }.toVector
    }

    def findCalls: Vector[WdlCall] =
        _findCalls(statements)

    // Count how many calls (task or workflow) there are in a series
    // of statements.
    def countCalls : Int =
        findCalls.length


    // The block is a singleton with one statement which is a call. The call
    // has no subexpressions. Note that the call may not provide
    // all the callee's arguments.
    def isCallWithNoSubexpressions(van: VarAnalysis) : Boolean = {
        if (statements.size != 1)
            return false
        if (!statements.head.isInstanceOf[WdlCall])
            return false
        val call = statements.head.asInstanceOf[WdlCall]
        call.inputMappings.values.forall{ expr =>
            van.isTrivialExpression(expr)
        }
    }
}

object Block {
    def countCalls(statements: Seq[Scope]) : Int =
        Block(statements.toVector).countCalls

    def countCalls(statement: Scope) : Int =
        Block(Vector(statement)).countCalls

    def findCalls(statements: Seq[Scope]) : Vector[WdlCall] =
        Block(statements.toVector).findCalls

    def findCalls(statement: Scope) : Vector[WdlCall] =
        Block(Vector(statement)).findCalls

    // Is this a subblock? Declarations aren't subblocks, scatters and if's are.
    private def isSubBlock(scope:Scope) : Boolean = {
        scope match {
            case _:Scatter => true
            case _:If => true
            case _ => false
        }
    }

    // Split a workflow into blocks, where each block will compile to
    // a stage. When the workflow is unlocked, we create a separate
    // block for each toplevel call that has no subexpressions. These
    // will have their own stages.
    def splitIntoBlocks(children: Vector[Scope],
                        van : VarAnalysis) : Vector[Block] = {
        // base cases: zero and one children
        if (children.isEmpty)
            return Vector.empty
        if (children.length == 1)
            return Vector(Block(children))

        // Normal case, recurse into the first N-1 statements,
        // than append the Nth.
        val blocks = splitIntoBlocks(children.dropRight(1), van)
        val allBlocksButLast = blocks.dropRight(1)

        // see if the last statement should be incorporated into the last block,
        // of if we should open a new block.
        val lastBlock = blocks.last
        val lastStatement = children.last
        val lastChild = Block(Vector(lastStatement))
        var openNewBlock =
            (lastBlock.countCalls, lastChild.countCalls) match {
                case (0, 1) if (lastChild.isCallWithNoSubexpressions(van)) =>
                    // create a separate block for the last child; it will get its
                    // own stage. We are compiling a WDL call directly into a stage
                    // here.
                    true
                case (0, (0|1)) =>
                    // no calls were seen so far, extend the block
                    false
                case (1, (0|1)) =>
                    // The last block already contains a call, start a new block
                    true
                case (x ,y) =>
                    throw new Exception(s"Internal error: block has ${x} calls, and child has ${y} calls")
            }
        if (isSubBlock(lastStatement)) {
            // If the last statement is an if/scatter, always start a new block
            openNewBlock = true
        }
        val trailing: Vector[Block] =
            if (openNewBlock)
                Vector(lastBlock, lastChild)
            else
                Vector(lastBlock.append(lastChild))
        allBlocksButLast ++ trailing
    }


    // Is there a declaration after a call? For example:
    //
    // scatter (i in numbers) {
    //     call add { input: a=i, b=1 }
    //     Int n = add.result
    // }
    private def isDeclarationAfterCall(statements: Vector[Scope]) : Boolean = {
        statements.length match {
            case 0 => false
            case 1 =>
                if (isSubBlock(statements.head)) {
                    // recurse into the only child
                    isDeclarationAfterCall(statements.head.children.toVector)
                } else {
                    // base case: there is just one one child,
                    // and it is a call or a declaration.
                    false
                }
            case _ =>
                val numCalls = countCalls(statements.head)
                if (numCalls == 1) true
                else isDeclarationAfterCall(statements.tail)
        }
    }

    // A block is reducible if one of these conditions hold.
    // (1) it has two calls or more.
    // (2) It has one call, but there may be dependent
    // declarations after it. For example:
    //
    // scatter (i in numbers) {
    //     call add { input: a=i, b=1 }
    //     Int n = add.result
    // }
    private def isReducible(scope: Scope) : Boolean = {
        if (!isSubBlock(scope))
            return false
        val numCalls = countCalls(scope.children.toVector)
        if (numCalls == 0)
            return false
        if (numCalls >= 2)
            return true
        return isDeclarationAfterCall(scope.children.toVector)
    }


    // In a tree like:
    //    if ()
    //      if ()
    //        scatter ()
    //           Int i
    //           call A
    //           call B
    //
    // The scatter node is the end of the trunk.
    def findTrunkTop(stmt: Scope) : Scope = {
        stmt.children.size match {
            case 0 => throw new Exception("findTrunkTop on an empty tree")
            case 1 =>  findTrunkTop(stmt.children.head)
            case _ => stmt
        }
    }

    // There is a separable workflow in here, either
    // a fragment with one call, or a call-line with multiple calls.
    //
    // call line example:
    // if (cond) {
    //   call A
    //   call B
    //   call C
    // }
    //
    // a fragment example:
    // if (cond)
    //    if (cond2)
    //       scatter(x in xs)
    //         call A
    //
    def findReducibleChild(statements: Seq[Scope],
                           verbose: Verbose) : Option[Scope] = {
        val child = statements.find{
            case stmt => isReducible(stmt)
        }
        child match {
            case None =>
                // There is nothing to reduce here. This can happen for a sequence like:
                // workflow w
                //    call C
                //    call D
                //    String s = C.result
                None
            case Some(stmt) =>
                Some(findTrunkTop(stmt))
        }
    }
}
