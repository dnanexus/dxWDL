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

import wdl._

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
}

object Block {
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
    object Kind extends Enumeration {
        val CallLine, Fragment = Value
    }

    case class ReducibleChild(scope: Scope, kind: Kind.Value)

    def countCalls(statements: Seq[Scope]) : Int =
        Block(statements.toVector).countCalls

    def countCalls(statement: Scope) : Int =
        Block(Vector(statement)).countCalls

    def findCalls(statements: Seq[Scope]) : Vector[WdlCall] =
        Block(statements.toVector).findCalls

    def findCalls(statement: Scope) : Vector[WdlCall] =
        Block(Vector(statement)).findCalls

    def splitIntoBlocks(children: Vector[Scope]) : Vector[Block] = {
        // base cases: zero and one children
        if (children.isEmpty)
            return Vector.empty
        if (children.length == 1)
            return Vector(Block(children))

        // Normal case, recurse into the first N-1 statements,
        // than append the Nth.
        val blocks = splitIntoBlocks(children.dropRight(1))
        val allBlocksButLast = blocks.dropRight(1)

        val lastBlock = blocks.last
        val lastChild = Block(Vector(children.last))
        val trailing: Vector[Block] = (lastBlock.countCalls, lastChild.countCalls) match {
            case (0, (0|1)) =>
                Vector(lastBlock.append(lastChild))
            case (1, (0|1)) =>
                // The last block already contains a call, start a new block
                Vector(lastBlock, lastChild)
            case (x ,y) =>
                if (x > 1) {
                    System.err.println("GenerateIR, block:")
                    System.err.println(lastBlock)
                    throw new Exception(s"block has ${x} calls")
                }
                assert(y > 1)
                System.err.println("GenerateIR, child:")
                System.err.println(lastChild)
                throw new Exception(s"child has ${y} calls")
        }
        allBlocksButLast ++ trailing
    }


    // Is this a subblock? Declarations aren't subblocks, scatters and if's are.
    private def isSubBlock(scope:Scope) : Boolean = {
        scope match {
            case _:Scatter => true
            case _:If => true
            case _ => false
        }
    }

    // A block is large if it has two calls or more
    private def isReducible(scope: Scope) : Boolean = {
        if (isSubBlock(scope)) {
            Block(scope.children.toVector).countCalls >= 1
        } else {
            false
        }
    }

    // A block that looks like this:
    //
    //  if (cond) {
    //      call A
    //      call B
    //  }
    private def isLeafBlock(scope: Scope) : Boolean = {
        if (!isSubBlock(scope))
            return false
        val numCalls = countCalls(Vector(scope))
        val numTopLevelCalls = scope.children.foldLeft(0) {
            case (accu, call:WdlCall) =>
                accu + 1
            case (accu, _) =>
                accu
        }
        numCalls == numTopLevelCalls
    }

    // There is a separable workflow in here, either
    // a fragment with one call, or a call-line with multiple calls.
    //
    // call line example:
    //   call A
    //   call B
    //   call C
    //
    private def findReducibleChild(statements: Seq[Scope]) : Option[ReducibleChild] = {
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
            case Some(stmt) if (countCalls(stmt) == 1) =>
                // This is a fragment, we can stop here.
                Some(ReducibleChild(stmt, Kind.Fragment))
            case Some(stmt) if isLeafBlock(stmt) =>
                // this is a call-line
                Some(ReducibleChild(stmt, Kind.CallLine))
            case Some(stmt) =>
                // dig deeper, one of the children is reducible
                val retval = findReducibleChild(stmt.children)
                assert(retval != None)
                retval
        }
    }

    def findOneReducibleChild(statements: Vector[Scope]) : Option[ReducibleChild] = {
        val child = statements.find{
            case stmt => isReducible(stmt)
        }
        child match {
            case None => None
            case Some(c) =>
                // This child has more than one call. It either holds
                // a fragment or a call-line.
                findReducibleChild(Seq(c))
        }
    }
}
