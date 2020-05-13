package wdlTools.cli

import java.net.URL
import java.nio.file.{Path, Paths}

import wdlTools.generators.ProjectGenerator.{TaskModel, WorkflowModel}
import wdlTools.generators._
import wdlTools.util.Util

import scala.collection.mutable
import scala.language.reflectiveCalls

case class Generate(conf: WdlToolsConf) extends Command {
  override def apply(): Unit = {
    val generatedFiles: mutable.Map[URL, String] = mutable.HashMap.empty
    val opts = conf.getOptions
    val args = conf.generate
    val name = args.name()
    val outputDir: Path = args.outputDir.getOrElse(Paths.get(name))

    val generator = ProjectGenerator(
        opts,
        name,
        outputDir,
        wdlVersion = args.wdlVersion(),
        interactive = args.interactive(),
        readmes = args.readmes(),
        dockerfile = args.dockerfile(),
        tests = args.tests(),
        makefile = args.makefile(),
        generatedFiles = generatedFiles,
        dockerImage = args.docker.toOption
    )
    val workflow = if (args.workflow()) {
      Some(WorkflowModel(args.wdlVersion(), name = Some(name)))
    } else {
      None
    }
    val tasks =
      args.task.map(_.map(taskName => TaskModel(Some(taskName))).toVector).getOrElse(Vector.empty)

    generator.apply(workflow, tasks)

    Util.writeContentsToFiles(generator.generatedFiles.toMap, Some(outputDir), args.overwrite())
  }
}
