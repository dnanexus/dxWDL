package dx.translator.wdl

import java.nio.file.Path

import dx.core.ir.Parameter
import wdlTools.syntax.WdlVersion
import wdlTools.types.{TypedAbstractSyntax => TAT}
import wdlTools.util.{Adjuncts, LocalFileSource, Logger}

case class WdlBundle(version: WdlVersion,
                     primaryCallable: Option[TAT.Callable],
                     tasks: Map[String, TAT.Task],
                     workflows: Map[String, TAT.Workflow],
                     callableNames: Set[String],
                     sources: Map[String, TAT.Document],
                     adjunctFiles: Map[String, Vector[Adjuncts.AdjunctFile]])

object WdlBundle {

  /**
    * Check that a declaration name is not any dx-reserved names.
    */
  private def checkVariableName(decls: Vector[TAT.Variable]): Unit = {
    decls.foreach {
      case TAT.Declaration(name, _, _, _) if name == Parameter.ComplexValueKey =>
        throw new Exception(
            s"Variable ${name} is reserved by DNAnexus and cannot be used as a variable name "
        )
    }
  }

  // check that streaming annotations are only done for files.
  private def validateVariableNames(callable: TAT.Callable): Unit = {
    callable match {
      case wf: TAT.Workflow =>
        if (wf.parameterMeta.isDefined) {
          Logger.get.warning("dxWDL workflows ignore their parameter meta section")
        }
        checkVariableName(wf.inputs)
        checkVariableName(wf.outputs)
        val allDeclarations: Vector[TAT.Declaration] = wf.body.collect {
          case d: TAT.Declaration => d
        }
        checkVariableName(allDeclarations)
      case task: TAT.Task =>
        checkVariableName(task.inputs)
        checkVariableName(task.outputs)
    }
  }

  private def bundleInfoFromDoc(doc: TAT.Document): WdlBundle = {
    // Add source and adjuncts for main file
    val (sources, adjunctFiles) = doc.source match {
      case localFs: LocalFileSource =>
        val absPath: Path = localFs.localPath
        val sources = Map(absPath.toString -> doc)
        val adjunctFiles = Adjuncts.findAdjunctFiles(absPath)
        (sources, adjunctFiles)
      case fs =>
        val sources = Map(fs.toString -> doc)
        (sources, Map.empty[String, Vector[Adjuncts.AdjunctFile]])
    }
    val tasks: Map[String, TAT.Task] = doc.elements.collect {
      case x: TAT.Task =>
        validateVariableNames(x)
        x.name -> x
    }.toMap
    val workflows = doc.workflow.map { x =>
      validateVariableNames(x)
      x.name -> x
    }.toMap
    val primaryCallable: Option[TAT.Callable] = doc.workflow match {
      case None if tasks.size == 1 => Some(tasks.values.head)
      case wf                      => wf
    }
    WdlBundle(doc.version.value,
              primaryCallable,
              tasks,
              workflows,
              tasks.keySet ++ workflows.keySet,
              sources,
              adjunctFiles)
  }

  private def mergeBundleInfo(accu: WdlBundle, from: WdlBundle): WdlBundle = {
    val version = accu.version
    if (version != from.version) {
      throw new RuntimeException(s"Different WDL versions: ${version} != ${from.version}")
    }
    val intersection = (accu.callableNames & from.callableNames)
      .map { name =>
        val aCallable = accu.tasks.getOrElse(name, accu.workflows(name))
        val bCallable = from.tasks.getOrElse(name, from.workflows(name))
        name -> (aCallable, bCallable)
      }
      .filter {
        // The comparision is done with "toString", because otherwise two identical
        // definitions are somehow, through the magic of Scala, unequal.
        case (_, (ac, bc)) => ac == bc
      }
      .toMap
    if (intersection.nonEmpty) {
      intersection.foreach {
        case (name, (ac, bc)) =>
          Logger.error(s"""|name ${name} appears with two different callable definitions
                           |1)
                           |${ac}
                           |2)
                           |${bc}
                           |""".stripMargin)
      }
      throw new Exception(
          s"callable(s) ${intersection.keySet} appears multiple times with different definitions"
      )
    }
    WdlBundle(
        version,
        accu.primaryCallable.orElse(from.primaryCallable),
        accu.tasks ++ from.tasks,
        accu.workflows ++ from.workflows,
        accu.callableNames | from.callableNames,
        accu.sources ++ from.sources,
        accu.adjunctFiles ++ from.adjunctFiles
    )
  }

  // recurse into the imported packages
  // Check the uniqueness of tasks, Workflows, and Types
  // merge everything into one bundle.
  def flattenDepthFirst(tDoc: TAT.Document): WdlBundle = {
    val topLevelInfo = bundleInfoFromDoc(tDoc)
    val imports: Vector[TAT.ImportDoc] = tDoc.elements.collect {
      case x: TAT.ImportDoc => x
    }
    imports.foldLeft(topLevelInfo) {
      case (accu: WdlBundle, imp) =>
        val flatImportInfo = flattenDepthFirst(imp.doc)
        mergeBundleInfo(accu, flatImportInfo)
    }
  }
}
