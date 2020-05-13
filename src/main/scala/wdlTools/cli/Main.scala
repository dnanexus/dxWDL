package wdlTools.cli

object Main extends App {
  val conf = new WdlToolsConf(args.toVector)

  conf.subcommand match {
    case None => conf.printHelp()
    case Some(subcommand) =>
      val command: Command = subcommand match {
        case conf.check    => TypeCheck(conf)
        case conf.format   => Format(conf)
        case conf.lint     => Lint(conf)
        case conf.upgrade  => Upgrade(conf)
        case conf.generate => Generate(conf)
        case conf.readmes  => Readmes(conf)
        case conf.printAST => PrintAST(conf)
        case other         => throw new Exception(s"Unrecognized command $other")
      }
      command.apply()
  }
}
