package dx.core.io

import java.nio.file.{Path, Paths}

import wdlTools.exec.ExecPaths
import wdlTools.util.Logger

/**
  * Important paths on a DNAnexus worker.
  * This is used in several distinct and seemingly disjoint cases:
  *  - generating applets (compile time)
  *  - localizing/delocalizing files and evaluating expressions (task/workflow runtime)
  *  - unit tests
  * Note that if a task in a container, the container will need access to temporary
  * files created with stdlib calls like "write_lines".
  * @param rootDir the root directory - typically the user's home directory
  */
case class DxWorkerPaths(rootDir: Path)
    extends ExecPaths(rootDir, rootDir.resolve(DxWorkerPaths.TempDir)) {

  /**
    * Running applets download files from the platform to this location.
    */
  def getInputFilesDir(ensureExists: Boolean = false): Path = {
    getOrCreateDir(DxWorkerPaths.InputFilesDir,
                   getRootDir(ensureExists).resolve(DxWorkerPaths.InputFilesDir),
                   ensureExists)
  }

  /**
    * Running applets place output files in this location.
    */
  def getOutputFilesDir(ensureExists: Boolean = false): Path = {
    getOrCreateDir(DxWorkerPaths.OutputFilesDir,
                   getRootDir(ensureExists).resolve(DxWorkerPaths.OutputFilesDir),
                   ensureExists)
  }

  def getDxfuseMountDir(ensureExists: Boolean = false): Path = {
    getOrCreateDir(DxWorkerPaths.DxfuseMountDir,
                   getRootDir(ensureExists).resolve(DxWorkerPaths.DxfuseMountDir),
                   ensureExists)
  }

  /**
    * Where a JSON representation of the instance data base is stored.
    */
  def getInstanceTypeDbFile(ensureParentExists: Boolean = false): Path = {
    // TODO: any reason we can't put this in meta dir?
    getRootDir(ensureParentExists).resolve(DxWorkerPaths.InstanceTypeDbFile)
  }

  /**
    * Source WDL code. We could get it from the details field, but that
    * would require an additional API call. This is a private copy.
    */
  def getSourceEncodedFile(ensureParentExists: Boolean = false): Path = {
    // TODO: any reason we can't put this in meta dir?
    getRootDir(ensureParentExists).resolve(DxWorkerPaths.SourceEncodedFile)
  }

  /**
    * Bash commands for streaming files with 'dx cat' are located here.
    */
  def getSetupStreamsFile(ensureParentExists: Boolean = false): Path = {
    getMetaDir(ensureParentExists).resolve(DxWorkerPaths.SetupStreamsFile)
  }

  /**
    * Location of dx download agent (dxda) manifest. It will download all these
    * files, if the file is non empty.
    */
  def getDxdaManifestFile(ensureParentExists: Boolean = false): Path = {
    getMetaDir(ensureParentExists).resolve(DxWorkerPaths.DxdaManifestFile)
  }

  /**
    * Location of dxfuse manifest. It will mount all these  files, if the file
    * is non empty.
    */
  def getDxfuseManifestFile(ensureParentExists: Boolean = false): Path = {
    getMetaDir(ensureParentExists).resolve(DxWorkerPaths.DxfuseManifestFile)
  }

  /**
    * File for storing the state between prolog and epilog of the task runner.
    */
  def getTaskEnvFile(ensureParentExists: Boolean = false): Path = {
    getMetaDir(ensureParentExists).resolve(DxWorkerPaths.SetupStreamsFile)
  }

  // create all the directory paths, so we can start using them.
  // This is used when running tasks, but NOT when compiling.
  def createCleanDirs(): Unit = {
    Logger.get.ignore(
        Vector(
            getMetaDir(ensureExists = true),
            getTempDir(ensureExists = true),
            getInputFilesDir(ensureExists = true),
            getOutputFilesDir(ensureExists = true),
            getDxfuseMountDir(ensureExists = true)
        )
    )
  }
}

object DxWorkerPaths {
  // The home directory on a DNAnexus worker. This directory exists only at runtime in the cloud.
  // Beware of using it in code paths that run at compile time.
  val RootDir: Path = Paths.get("/home/dnanexus")
  val InputFilesDir = "inputs"
  val OutputFilesDir = "outputs"
  val TempDir = "job_scratch_space"
  val InstanceTypeDbFile = "instance_type_db.json"
  val SourceEncodedFile = "source.wdl.uu64"
  val SetupStreamsFile = "setup_streams"
  val DxdaManifestFile = "dxdaManifest.json"
  val DxfuseManifestFile = "dxdaManifest.json"
  val DxfuseMountDir = "mnt"
  val TaskEnvFile = "taskEnv.json"

  lazy val default: DxWorkerPaths = DxWorkerPaths(RootDir)
}
