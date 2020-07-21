package dx.core.languages.wdl

import wdlTools.types.{WdlTypes, TypedAbstractSyntax => TAT}

case class Bundle(primaryCallable: Option[TAT.Callable],
                  allCallables: Map[String, TAT.Callable],
                  typeAliases: Map[String, WdlTypes.T])
