package dxWDL.base

import java.nio.file.Path
import wdlTools.syntax.WdlVersion
import wdlTools.types.{TypedAbstractSyntax => TAT, WdlTypes}

// Exception used for AppInternError
class AppInternalException(s : String) extends RuntimeException(s)

// Exception used for AppError
class AppException(s : String) extends RuntimeException(s)

class PermissionDeniedException(s: String) extends Exception(s)

class InvalidInputException(s: String) extends Exception(s)

class IllegalArgumentException(s: String) extends Exception(s)


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

  def toWdlVersion(value : Value) : WdlVersion = {
    value match {
      case WDLvDraft2 => WdlVersion.Draft_2
      case WDLv1_0 => WdlVersion.V1
      case WDLv2_0 => WdlVersion.V2
      case other => throw new Exception(s"${other} is not a wdl version")
    }
  }
  def fromWdlVersion(version : WdlVersion): Value = {
    version match {
      case WdlVersion.Draft_2  => Language.WDLvDraft2
      case WdlVersion.V1       => Language.WDLv1_0
      case WdlVersion.V2       => Language.WDLv2_0
      case other               => throw new Exception(s"Unsupported dielect ${other}")
    }
  }
}

case class WomBundle(primaryCallable : Option[TAT.Callable],
                     allCallables : Map[String, TAT.Callable],
                     typeAliases : Map[String, WdlTypes.T])
