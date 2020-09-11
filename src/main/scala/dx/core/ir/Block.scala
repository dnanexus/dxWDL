package dx.core.ir

import wdlTools.util.Enum

/**
  * These are the kinds of blocks that are run by the workflow-fragment-runner.
  * A block can have expressions, input ports, and output ports in the beginning.
  * The block can be one of these types:
  *   - Purely expressions (no asynchronous calls at any nesting level).
  *   - Call
  *      - with no subexpressions to evaluate
  *      - with subexpressions requiring evaluation
  *      - Fragment with expressions and one call
  *   - Conditional block
  *      - with exactly one call
  *      - a complex subblock
  *   - Scatter block
  *      - with exactly one call
  *      - with a complex subblock
  */
object BlockKind extends Enum {
  type BlockKind = Value
  val ExpressionsOnly, CallDirect, CallWithSubexpressions, CallFragment, ConditionalOneCall,
      ConditionalComplex, ScatterOneCall, ScatterComplex = Value
}

trait Block {
  def kind: BlockKind.BlockKind

  /**
    * Create a human readable name for a block of statements
    * 1. Ignore all declarations
    * 2. If there is a scatter/if, use that
    * 3. if there is at least one call, use the first one.
    * 4. If the entire block is made up of expressions, return None
    * @return
    */
  def getName: Option[String]

  def outputNames: Set[String]

  def prettyFormat: String
}
