package dx.core.languages

import wdlTools.types.{WdlTypes, TypedAbstractSyntax => TAT}

package object wdl {
  def parseWdlTasks(
      wfSource: String
  ): (Map[String, TAT.Task], Map[String, WdlTypes.T], TAT.Document) = {
    val (tDoc, typeAliases) = WdlUtils.parseAndCheckSourceString(wfSource)
    val tasks = tDoc.elements.collect {
      case task: TAT.Task => task.name -> task
    }.toMap
    (tasks, typeAliases.toMap, tDoc)
  }
}
