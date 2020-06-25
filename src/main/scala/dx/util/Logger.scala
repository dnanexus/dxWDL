package dx.util

object TraceLevel {
  // show no trace messages
  val None: Int = 0
  // show verbose messages
  val Verbose: Int = 1
  // show extra-verbose messages
  val VVerbose: Int = 2
}

/**
  * Message logger.
  * @param quiet suppress info and warning messages
  * @param traceLevel level of trace detail to show - orthogonal to `quiet`, i.e. you can show trace messages
  *                   but not info/warning
  * @param keywords specific keywords for which to enable tracing
  * @param traceIndenting amount to indent trace messages
  */
case class Logger(quiet: Boolean,
                  traceLevel: Int,
                  keywords: Set[String] = Set.empty,
                  traceIndenting: Int = 0) {
  private val APPLET_LOG_MSG_LIMIT = 1000
  private lazy val keywordsLower: Set[String] = keywords.map(_.toLowerCase)

  lazy val isVerbose: Boolean = traceLevel >= TraceLevel.Verbose

  // check in a case insensitive fashion
  def containsKey(word: String): Boolean = {
    keywordsLower.contains(word.toLowerCase)
  }

  // returns a Logger that has `verbose = true` if `key` is in `keywords`
  def withTraceIfContainsKey(word: String, newTraceLevel: Int = TraceLevel.Verbose): Logger = {
    if (containsKey(word) && newTraceLevel > traceLevel) {
      copy(traceLevel = newTraceLevel)
    } else {
      this
    }
  }

  // returns a Logger with trace indent increased
  def withIncTraceIndent(steps: Int = 1): Logger = {
    copy(traceIndenting = traceIndenting + steps)
  }

  // print a message with no color - ignored if `quiet` is false
  def info(msg: String): Unit = {
    if (!quiet) {
      System.err.println(msg)
    }
  }

  // print a warning message in yellow - ignored if `quiet` is true and `force` is false
  def warning(msg: String, force: Boolean = false): Unit = {
    if (force || !quiet) {
      System.err.println(Console.YELLOW + msg + Console.RESET)
    }
  }

  // print an error message in red
  def error(msg: String): Unit = {
    Logger.error(msg)
  }

  private def traceEnabledFor(minLevel: Int, requiredKey: Option[String]): Boolean = {
    traceLevel >= minLevel && requiredKey.forall(containsKey)
  }

  private def printTrace(msg: String): Unit = {
    System.err.println(s"${" " * traceIndenting * 2}${msg}")
  }

  private def truncateMessage(msg: String, maxLength: Int) = {
    // TODO: truncate long messages
    if (msg.length > maxLength) {
      "Message is too long for logging"
    } else {
      msg
    }
  }

  // print a detailed message to the user; ignored if `traceLevel` < `level`
  def trace(msg: String,
            maxLength: Option[Int] = None,
            minLevel: Int = TraceLevel.Verbose,
            requiredKey: Option[String] = None): Unit = {
    if (traceEnabledFor(minLevel, requiredKey)) {
      if (maxLength.isDefined) {
        printTrace(truncateMessage(msg, maxLength.get))
      } else {
        printTrace(msg)
      }
    }
  }

  // Logging output for applets at runtime. Shortcut for `trace()` with a message `maxLength`
  // (defaults to `APPLET_LOG_MSG_LIMIT`)
  def traceLimited(msg: String,
                   limit: Int = APPLET_LOG_MSG_LIMIT,
                   minLevel: Int = TraceLevel.Verbose,
                   requiredKey: Option[String] = None): Unit = {
    trace(msg, Some(limit), minLevel, requiredKey)
  }

  // Ignore a value and print a trace message. This is useful for avoiding warnings/errors
  // on unused variables.
  def ignore[A](value: A,
                minLevel: Int = TraceLevel.Verbose,
                requiredKey: Option[String] = None): Unit = {
    if (traceEnabledFor(minLevel, requiredKey)) {
      printTrace(s"ignoring ${value}")
    }
  }
}

object Logger {
  lazy val Quiet: Logger = Logger(quiet = true, traceLevel = TraceLevel.None)
  lazy val Verbose: Logger = Logger(quiet = false, traceLevel = TraceLevel.Verbose)

  // print an error message in red
  def error(msg: String): Unit = {
    System.err.println(Console.RED + msg + Console.RESET)
  }
}
