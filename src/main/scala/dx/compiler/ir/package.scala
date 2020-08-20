package dx.compiler

package object ir {
  // stages that the compiler uses in generated DNAx workflows
  val CommonStage = "common"
  val EvalStage = "eval"
  val OutputSection = "outputs"
  val Reorg = "reorg"
  val CustomReorgConfig = "reorg_config"
}
