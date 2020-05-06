package wdlTools.util

import java.net.URL
import java.nio.file.Path

/**
  * Enumeration for verbosity level.
  * The values are in increasing order, so that they can be compared using integer comparison
  * operators, e.g. `if (verbosity > Normal) { println("debugging") }`.
  */
object Verbosity extends Enumeration {
  type Verbosity = Value
  val Quiet, Normal, Verbose = Value
}

object TypeCheckingRegime extends Enumeration {
  type TypeCheckingRegime = Value
  val Strict, Moderate, Lenient = Value
}

/**
  * Common configuration options.
  * @param localDirectories local directories to search for imports.
  * @param followImports whether to follow imports when parsing.
  * @param verbosity verbosity level.
  * @param antlr4Trace whether to turn on tracing in the ANTLR4 parser.
  * @param typeChecking strictness of type-checking
  */
case class Options(localDirectories: Vector[Path] = Vector.empty,
                   followImports: Boolean = false,
                   verbosity: Verbosity.Verbosity = Verbosity.Normal,
                   antlr4Trace: Boolean = false,
                   typeChecking: TypeCheckingRegime.Value = TypeCheckingRegime.Moderate) {

  def getUrl(pathOrUrl: String, mustExist: Boolean = true): URL = {
    Util.getUrl(pathOrUrl, localDirectories, mustExist)
  }
}

/** Configuration for expression evaluation. Some operations perform file IO.
  *
  * @param homeDir  the root directory for relative paths, and the root directory for search (glob).
  * @param tmpDir   directory for placing temporary files.
  * @param stdout   the file that has a copy of standard output. This is used in the command section.
  * @param stderr   as above for standard error.
  */
case class EvalConfig(homeDir: Path, tmpDir: Path, stdout: Path, stderr: Path)
