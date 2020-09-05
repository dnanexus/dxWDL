package dx.core.io

import java.nio.file.{Path, Paths}

import wdlTools.util.{FileUtils, Logger}

// configuration of paths. This is used in several distinct and seemingly disjoint
// cases:
//  - compile time
//  - task runtime
//  - workflow runtime
//  - unit test runtime
//
// Note all of these directories and paths have to reside under the
// user home directory. If a task runs under docker, the container
// will need access to temporary files created with stdlib calls like
// "write_lines".
//
case class DxWorkerPaths(homeDir: Path,
                         metaDir: Path,
                         // Running applets download files from the platform to this location
                         inputFilesDir: Path,
                         // Running applets place output files in this location
                         outputFilesDir: Path,
                         // scratch space for WDL stdlib operations like "write_lines"
                         tmpDir: Path,
                         // Where a JSON representation of the instance data base is stored
                         instanceTypeDB: Path,
                         // Source WDL code. We could get it from the details field, but that
                         // would require an additional API call. This is a private copy.
                         wdlSourceCodeEncoded: Path,
                         stdout: Path,
                         stderr: Path,
                         // bash script for running the docker image the user specified
                         // is deposited here.
                         dockerSubmitScript: Path,
                         // bash script is written to this location
                         script: Path,
                         // Status code returned from the shell command is written
                         // to this file
                         rcPath: Path,
                         // file where the docker container name is stored.
                         dockerCid: Path,
                         // bash commands for streaming files with 'dx cat' are located here
                         setupStreams: Path,
                         // Location of dx download agent (dxda) manifest. It will download all these
                         // files, if the file is non empty.
                         dxdaManifest: Path,
                         // Location of dxfuse manifest. It will mount all these
                         // files, if the file is non empty.
                         dxfuseManifest: Path,
                         dxfuseMountpoint: Path,
                         // file for storing the state between prolog and epilog of the task runner
                         runnerTaskEnv: Path,
                         // should we stream all files?
                         streamAllFiles: Boolean,
                         logger: Logger) {

  // create all the directory paths, so we can start using them.
  // This is used when running tasks, but NOT when compiling.
  def createCleanDirs(): Unit = {
    Vector(metaDir, inputFilesDir, outputFilesDir, tmpDir, dxfuseMountpoint).foreach(
        FileUtils.createDirectories
    )
  }
}

object DxWorkerPaths {
  // This directory exists only at runtime in the cloud. Beware of using
  // it in code paths that run at compile time.
  val HomeDir: Path = Paths.get("/home/dnanexus")

  def apply(streamAllFiles: Boolean, logger: Logger = Logger.get): DxWorkerPaths = {
    val metaDir: Path = HomeDir.resolve("meta")
    val inputFilesDir: Path = HomeDir.resolve("inputs")
    val outputFilesDir: Path = HomeDir.resolve("outputs")
    val tmpDir: Path = HomeDir.resolve("job_scratch_space")
    val instanceTypeDB = HomeDir.resolve("instance_type_db.json")
    val wdlSourceCodeEncoded = HomeDir.resolve("source.wdl.uu64")
    val stdout = metaDir.resolve("stdout")
    val stderr = metaDir.resolve("stderr")
    val script = metaDir.resolve("script")
    val dockerSubmitScript = metaDir.resolve("docker.submit")
    val setupStreams = metaDir.resolve("setup_streams")
    val dxdaManifest = metaDir.resolve("dxdaManifest.json")
    val dxfuseManifest = metaDir.resolve("dxfuseManifest.json")
    val dxfuseMountpoint = HomeDir.resolve("mnt")
    val rcPath = metaDir.resolve("rc")
    val dockerCid = metaDir.resolve("dockerCid")
    val runnerTaskEnv = metaDir.resolve("taskEnv.json")
    DxWorkerPaths(
        HomeDir,
        metaDir,
        inputFilesDir,
        outputFilesDir,
        tmpDir,
        instanceTypeDB,
        wdlSourceCodeEncoded,
        stdout,
        stderr,
        dockerSubmitScript,
        script,
        rcPath,
        dockerCid,
        setupStreams,
        dxdaManifest,
        dxfuseManifest,
        dxfuseMountpoint,
        runnerTaskEnv,
        streamAllFiles,
        logger
    )
  }
}
