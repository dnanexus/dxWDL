package dx.executor.wdl

import dx.core.io.DxWorkerPaths
import dx.core.languages.wdl.{Utils => WdlUtils}
import dx.executor.{ExecutorFactory, FileUploader, JobMeta}
import wdlTools.types.{WdlTypes, TypedAbstractSyntax => TAT}
import wdlTools.util.DefaultBindings

case class WdlExecutorFactory(jobMeta: JobMeta,
                              workerPaths: DxWorkerPaths,
                              fileUploader: FileUploader)
    extends ExecutorFactory {
  private def parse: (TAT.Document, DefaultBindings[WdlTypes.T_Struct]) = {
    WdlUtils.parseSource(jobMeta.sourceCode, jobMeta.fileResolver)
  }

  def createTaskExecutor: Option[WdlTaskExecutor] = {
    val (doc, typeAliases) =
      try {
        parse
      } catch {
        case _: Throwable =>
          return None
      }
    if (doc.workflow.isDefined) {
      throw new Exception("a workflow that shouldn't be a member of this document")
    }
    val tasks = doc.elements.collect {
      case task: TAT.Task => task.name -> task
    }.toMap
    if (tasks.isEmpty) {
      throw new Exception("no tasks in this WDL program")
    }
    if (tasks.size > 1) {
      throw new Exception("More than one task in this WDL program")
    }
    Some(
        WdlTaskExecutor(tasks.values.head,
                        doc.version.value,
                        typeAliases,
                        jobMeta,
                        workerPaths,
                        fileUploader)
    )
  }
}
