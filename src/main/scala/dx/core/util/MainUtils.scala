package dx.core.util

import java.nio.file.{Path, Paths}

import wdlTools.util.{Logger, TraceLevel}

object MainUtils {
  def exceptionToString(e: Throwable): String = {
    val sw = new java.io.StringWriter
    e.printStackTrace(new java.io.PrintWriter(sw))
    sw.toString
  }

  private def failureMessage(message: String, exception: Option[Throwable]): String = {
    val exStr = exception.map(exceptionToString)
    (message, exStr) match {
      case (s, Some(e)) if s.nonEmpty =>
        s"""${message}
           |${e}""".stripMargin
      case ("", Some(e)) => e
      case (s, None)     => s
    }
  }

  trait Termination {
    def message: String
  }

  trait SuccessfulTermination extends Termination
  case class Success(override val message: String = "") extends SuccessfulTermination

  trait UnsuccessfulTermination extends Termination {
    val message: String
    val exception: Option[Throwable] = None
    override def toString: String = failureMessage(message, exception)
  }
  case class Failure(override val message: String = "",
                     override val exception: Option[Throwable] = None)
      extends UnsuccessfulTermination
  case class BadUsageTermination(override val message: String = "",
                                 override val exception: Option[Throwable] = None)
      extends UnsuccessfulTermination

  type OptionsMap = Map[String, Vector[String]]

  // This directory exists only at runtime in the cloud. Beware of using
  // it in code paths that run at compile time.
  lazy val baseDNAxDir: Path = Paths.get("/home/dnanexus")

  // Split arguments into sub-lists, one per each option.
  // For example:
  //    --sort relaxed --reorg --compile-mode IR
  // =>
  //    [[--sort, relaxed], [--reorg], [--compile-mode, IR]]
  //
  def splitCmdLine(arglist: List[String]): List[List[String]] = {
    def isKeyword(word: String): Boolean = word.startsWith("-")

    val keywordAndOptions: List[List[String]] = arglist.foldLeft(List.empty[List[String]]) {
      case (head :: tail, word) if isKeyword(word) =>
        List(word) :: head :: tail
      case (head :: tail, word) =>
        (word :: head) :: tail
      case (head, word) if isKeyword(word) =>
        List(word) :: head
      case (Nil, word) if isKeyword(word) => List(List(word))
      case (Nil, word) if !isKeyword(word) =>
        throw new Exception("Keyword must precede options")
    }
    keywordAndOptions.map(_.reverse).reverse
  }

  def normKey(s: String): String = {
    s.replaceAll("_", "").toUpperCase
  }

  def normKeyword(word: String): String = {
    // normalize a keyword, remove leading dashes
    // letters to lowercase.
    //
    // "--Archive" -> "archive"
    // "--archive-only -> "archiveonly"
    word.replaceAll("-", "")
  }

  def checkNumberOfArguments(keyword: String, expectedNumArgs: Int, subargs: List[String]): Unit = {
    if (expectedNumArgs != subargs.length)
      throw new Exception(s"""|Wrong number of arguments for ${keyword}.
                              |Expected ${expectedNumArgs}, input is
                              |${subargs}""".stripMargin.replaceAll("\n", " "))
  }

  def parseRuntimeDebugLevel(numberStr: String): Int = {
    val rtDebugLvl =
      try {
        numberStr.toInt
      } catch {
        case _: java.lang.NumberFormatException =>
          throw new Exception(
              s"""|the runtimeDebugLevel flag takes an integer input,
                  |${numberStr} is not of type int""".stripMargin
                .replaceAll("\n", " ")
          )
      }
    if (rtDebugLvl < 0 || rtDebugLvl > 2) {
      throw new Exception(
          s"""|the runtimeDebugLevel flag must be one of {0, 1, 2}.
              |Value ${rtDebugLvl} is out of bounds.""".stripMargin
            .replaceAll("\n", " ")
      )
    }
    rtDebugLvl
  }

  def getTraceLevel(runtimeDebugLevel: Option[Any], defaultLevel: Int = TraceLevel.None): Int = {
    runtimeDebugLevel match {
      case None                          => defaultLevel
      case Some(numberStr: String)       => parseRuntimeDebugLevel(numberStr)
      case Some(List(numberStr: String)) => parseRuntimeDebugLevel(numberStr)
      case _                             => throw new Exception("debug level specified twice")
    }
  }

  def terminate(termination: Termination, usageMessage: Option[String] = None): Unit = {
    val rc = termination match {
      case _: SuccessfulTermination =>
        if (termination.message.nonEmpty) {
          println(termination.message)
        }
        0
      case BadUsageTermination("", e) if usageMessage.isDefined =>
        Logger.error(failureMessage(usageMessage.get, e))
        1
      case term: UnsuccessfulTermination =>
        val msg = failureMessage(term.message, term.exception)
        if (msg.nonEmpty) {
          Logger.error(msg)
        }
        1
    }
    System.exit(rc)
  }
}
