package dxWDL.util

import java.nio.file.{Path}

import dxWDL.base.Utils

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
case class DxPathConfig(
    homeDir: Path,
    metaDir : Path,

    // Running applets download files from the platform to this location
    inputFilesDir : Path,

    // Running applets place output files in this location
    outputFilesDir : Path,

    // scratch space for WDL stdlib operations like "write_lines"
    tmpDir : Path,

    // Where a JSON representation of the instance data base is stored
    instanceTypeDB: Path,

    // Source WOM code. We could get it from the details field, but that
    // would require an additional API call. This is a private copy.
    womSourceCodeEncoded: Path,

    stdout: Path,
    stderr: Path,

    // bash script for running the docker image the user specified
    // is deposited here.
    dockerSubmitScript: Path,

    // bash script is written to this location
    script : Path,

    // Status code returned from the shell command is written
    // to this file
    rcPath : Path,

    // file where the docker container name is stored.
    dockerCid : Path,

    // bash commands for streaming files with 'dx cat' are located here
    setupStreams : Path,

    // file for storing the state between prolog and epilog of the task runner
    runnerTaskEnv: Path,
    verbose: Boolean) {

    // create all the directory paths, so we can start using them.
    // This is used when running tasks, but NOT when compiling.
    def createCleanDirs() : Unit = {
        Utils.safeMkdir(metaDir)
        Utils.safeMkdir(inputFilesDir)
        Utils.safeMkdir(outputFilesDir)
        Utils.safeMkdir(tmpDir)
    }
}

object DxPathConfig {
    def apply(homeDir: Path,
              verbose: Boolean) : DxPathConfig = {
        val metaDir: Path = homeDir.resolve("meta")
        val inputFilesDir: Path = homeDir.resolve("inputs")
        val outputFilesDir: Path = homeDir.resolve("outputs")
        val tmpDir : Path = homeDir.resolve("job_scratch_space")

        val instanceTypeDB = homeDir.resolve("instance_type_db.json")
        val womSourceCodeEncoded = homeDir.resolve("source.wdl.uu64")

        val stdout = metaDir.resolve("stdout")
        val stderr = metaDir.resolve("stderr")
        val script = metaDir.resolve("script")
        val dockerSubmitScript = metaDir.resolve("docker.submit")
        val setupStreams = metaDir.resolve("setup_streams")
        val rcPath = metaDir.resolve("rc")
        val dockerCid = metaDir.resolve("dockerCid")
        val runnerTaskEnv = metaDir.resolve("taskEnv.json")

        DxPathConfig(homeDir,
                     metaDir,
                     inputFilesDir,
                     outputFilesDir,
                     tmpDir,

            instanceTypeDB,
                     womSourceCodeEncoded,

            stdout,
                     stderr,
                     dockerSubmitScript,
                     script,
                     rcPath,
                     dockerCid,
                     setupStreams,
                     runnerTaskEnv,
                     verbose)
    }
}
