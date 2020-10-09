# this used to fail due to a bug in WOM where file arguments to
# a function were not coerced to strings - should be fixed with wdlTools
task write_lines_bug {
    Array[File] files
    command <<<
    python3 <<CODE
    # replace the random directory name with a deterministic one
    dirs = {}
    for f in [~{sep="<" write_lines(files)}]:
      parts = f.split("/")
      if parts[4] not in dirs:
        dir = f"input{len(dirs)}"
        dirs[parts[4]] = dir
      parts[4] = dirs[parts[4]]
      print(f"{'/'.join(parts)}\n")
    CODE
    >>>
    output {
        Array[String] result = read_lines(stdout())
    }
    runtime {
        docker: "ubuntu:16.04"
    }
}
