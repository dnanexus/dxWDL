# A task "forgets" to create a file output. This should
# cause an OutputError.
task CreateFile {
    String basename = "XYZ"

    command {
    }
    output {
      File result = "${basename}.txt"
    }
}

workflow missing_output {
    call CreateFile

    output {
       CreateFile.result
    }
}
