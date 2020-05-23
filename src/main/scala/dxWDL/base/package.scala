package dxWDL.base

import java.nio.file.Path
import wdlTools.types.{TypedAbstractSyntax => TAT, WdlTypes}

// Exception used for AppInternError
class AppInternalException private (ex: RuntimeException) extends RuntimeException(ex) {
  def this(message: String) = this(new RuntimeException(message))
}

// Exception used for AppError
class AppException private (ex: RuntimeException) extends RuntimeException(ex) {
  def this(message: String) = this(new RuntimeException(message))
}

object IORef extends Enumeration {
  val Input, Output = Value
}

object CompilerFlag extends Enumeration {
  val All, IR, NativeWithoutRuntimeAsset = Value
}

// Encapsulation of verbosity flags.
//  on --       is the overall setting true/false
//  keywords -- specific words to trace
//  quiet:      if true, do not print warnings and informational messages
case class Verbose(on: Boolean, quiet: Boolean, keywords: Set[String]) {
  lazy val keywordsLo = keywords.map(_.toLowerCase).toSet

  // check in a case insensitive fashion
  def containsKey(word: String): Boolean = {
    keywordsLo contains word.toLowerCase
  }
}

// Tree printer types for the execTree option
sealed trait TreePrinter
case object JsonTreePrinter extends TreePrinter
case object PrettyTreePrinter extends TreePrinter

// Packing of all compiler flags in an easy to digest
// format
case class CompilerOptions(archive: Boolean,
                           compileMode: CompilerFlag.Value,
                           defaults: Option[Path],
                           extras: Option[Extras],
                           fatalValidationWarnings: Boolean,
                           force: Boolean,
                           importDirs: List[Path],
                           inputs: List[Path],
                           leaveWorkflowsOpen: Boolean,
                           locked: Boolean,
                           projectWideReuse: Boolean,
                           reorg: Boolean,
                           streamAllFiles: Boolean,
                           execTree: Option[TreePrinter],
                           runtimeDebugLevel: Option[Int],
                           verbose: Verbose)

// Different ways of using the mini-workflow runner.
//   Launch:     there are WDL calls, lanuch the dx:executables.
//   Collect:    the dx:exucutables are done, collect the results.
object RunnerWfFragmentMode extends Enumeration {
  val Launch, Collect = Value
}

object Language extends Enumeration {
  val WDLvDraft2, WDLv1_0, WDLv2_0, CWLv1_0 = Value
}

case class WomBundle(primaryCallable : Option[TAT.Callable],
                     allCallables : Map[String, TAT.Callable],
                     typeAliases : Map[String, WdlTypes.T])
