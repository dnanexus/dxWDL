package dx.core.languages.wdl

import wdlTools.types.{TypedAbstractSyntax => TAT, WdlTypes}

case class Bundle(primaryCallable: Option[TAT.Callable],
                  allCallables: Map[String, TAT.Callable],
                  typeAliases: Map[String, WdlTypes.T])
