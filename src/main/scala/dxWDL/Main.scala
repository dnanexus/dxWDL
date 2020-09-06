package dxWDL

// Temporary main class to dispatch to either compiler or exec - until these are split into separate JARs.
object Main extends App {
  args.toVector match {
    case v if v.nonEmpty && Set("task", "frag").contains(v.head) =>
      dx.executor.Main.main(v)
    case v =>
      dx.compiler.Main.main(v)
  }
}
