package dxWDL.util

import java.nio.file.Path

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

    // file for storing the state between prolog and epilog of the task runner
    runnerTaskEnv: Path) {

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
    def apply(homeDir: Path) : DxPathConfig = {
        val metaDir: Path = homeDir.resolve("meta")
        val inputFilesDir: Path = homeDir.resolve("inputs")
        val outputFilesDir: Path = homeDir.resolve("outputs")
        val tmpDir : Path = homeDir.resolve("job_scratch_space")

        val stdout = metaDir.resolve("stdout")
        val stderr = metaDir.resolve("stderr")
        val script = metaDir.resolve("script")
        val dockerSubmitScript = metaDir.resolve("docker.submit")
        val rcPath = metaDir.resolve("rc")
        val runnerTaskEnv = metaDir.resolve("taskEnv.json")

        DxPathConfig(homeDir,
                     metaDir,
                     inputFilesDir,
                     outputFilesDir,
                     tmpDir,
                     stdout,
                     stderr,
                     dockerSubmitScript,
                     script,
                     rcPath,
                     runnerTaskEnv)
    }
}
