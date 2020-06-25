package dxWDL

import dx.core.util.MainUtils.normKey

// Temporary main class to dispatch to either compiler or exec - until these are split into separate JARs.
object Main extends App {
  val argsSeq = args.toSeq
  if (argsSeq.nonEmpty && normKey(argsSeq.head) == normKey("internal")) {
    dx.exec.Main.main(argsSeq.tail)
  } else {
    dx.compiler.Main.main(argsSeq)
  }
}
