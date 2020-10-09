package dx.core.ir

/**
  * Abstraction of the source code associated with IR.
  */
trait SourceCode {

  /**
    * Generates the source code as a String.
    * @return
    */
  def toString: String
}
trait DocumentSource extends SourceCode
trait WorkflowSource extends SourceCode
