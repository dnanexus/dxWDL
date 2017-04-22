# A task "forgets" to create a file output. This should
# cause an OutputError.
task iii_CreateFile {
    String basename = "XYZ"

    command {
    }
    output {
      File result = "${basename}.txt"
    }
}

workflow missing_output {
    call iii_CreateFile as CreateFile

    output {
       CreateFile.result
    }
}
