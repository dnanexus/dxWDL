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
import dxWDL.{CompilerErrorFormatter}

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
    def countCalls(statments: Seq[Scope]) : Int =
        Block(statments.toVector).countCalls

    def findCalls(statments: Seq[Scope]) : Vector[WdlCall] =
        Block(statments.toVector).findCalls

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


    case class Partition(before: Vector[Scope],
                         lrgBlock: Scope,
                         after: Vector[Scope])

    // Is this a subblock? Declarations aren't subblocks, scatters and if's are.
    private def isSubBlock(scope:Scope) : Boolean = {
        scope match {
            case _:Scatter => true
            case _:If => true
            case _ => false
        }
    }

    // A block is large if it has two calls or more
    private def isLargeSubBlock(scope: Scope) : Boolean = {
        if (isSubBlock(scope)) {
            Block(scope.children.toVector).countCalls >= 2
        } else {
            false
        }
    }


    // Look for the first if/scatter block that has two calls or more.
    def findFirstLargeSubBlock(statements: Vector[Scope]) : Option[Partition] = {
        var before = Vector.empty[Scope]
        var lrgBlock: Option[Scope] = None
        var after = Vector.empty[Scope]

        for (stmt <- statements) {
            lrgBlock match {
                case None =>
                    if (isLargeSubBlock(stmt))
                        lrgBlock = Some(stmt)
                    else
                        before = before :+ stmt
                case Some(blk) =>
                    after = after :+ stmt
            }
        }
        assert (before.length + lrgBlock.size + after.length == statements.length)
        lrgBlock match {
            case None => None
            case Some(blk) => Some(Partition(before, blk, after))
        }
    }

    // In a block, split off the beginning declarations, from the rest.
    // For example, the scatter block below, will be split into
    // the top two declarations, and the other calls.
    // scatter (unmapped_bam in flowcell_unmapped_bams) {
    //    String sub_strip_path = "gs://.*/"
    //    String sub_strip_unmapped = unmapped_bam_suffix + "$"
    //    call SamToFastqAndBwaMem {..}
    //    call MergeBamAlignment {..}
    // }
    def splitBlockDeclarations(children: List[Scope]) :
            (List[Declaration], List[Scope]) = {
        def collect(topDecls: List[Declaration],
                    rest: List[Scope]) : (List[Declaration], List[Scope]) = {
            rest match {
                case hd::tl =>
                    hd match {
                        case decl: Declaration =>
                            collect(decl :: topDecls, tl)
                        // Next element is not a declaration
                        case _ => (topDecls, rest)
                    }
                // Got to the end of the children list
                case Nil => (topDecls, rest)
            }
        }

        val (decls, rest) = collect(Nil, children)
        (decls.reverse, rest)
    }

    // Checks if an if/scatter block contains only calls, and they are all
    // in the same block or sub-block.
    //
    // a) calls are inside the same block
    //     if (cond) {
    //       call A
    //       call B
    //     }
    //
    // a1) same as (a), but the nesting could be deep
    //    if (cond) {
    //      scatter (x in xs) {
    //        call A
    //        call B
    //      }
    //    }
    //
    // Declarations can be placed anywhere, but not after or between
    // the calls.
    case class Accu(firstCall: Boolean, mixed: Boolean)

    def callsInSameLevel(statments: Vector[Scope],
                         cef: CompilerErrorFormatter) : Accu = {
        statments.foldLeft(Accu(false, false)) {
            case (accu, _:Declaration) =>
                if (accu.firstCall) Accu(true, true)
                else Accu(false, false)

            case (accu, _:WdlCall) =>
                Accu(true, accu.mixed)

            case (accu, stmt:If) =>
                val retval = callsInSameLevel(stmt.children.toVector, cef)
                Accu(accu.firstCall || retval.firstCall,
                     accu.mixed || retval.mixed)

            case (accu, stmt:Scatter) =>
                val retval = callsInSameLevel(stmt.children.toVector, cef)
                Accu(accu.firstCall || retval.firstCall,
                     accu.mixed || retval.mixed)

            case (accu, x) =>
                throw new Exception(cef.compilerInternalError(
                                        x.ast,
                                        s"${x.getClass.getSimpleName}"))
        }
    }
}
