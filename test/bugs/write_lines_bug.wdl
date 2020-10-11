# this used to fail due to a bug in WOM where file arguments to
# a function were not coerced to strings - should be fixed with wdlTools
workflow write_lines_bug {
    Array[File] files

    call write_lines_task {
      input: files = files
    }

    call fix_paths {
      input: strings = write_lines_task.strings
    }

    output {
      Array[String] strings = fix_paths.fixed_strings
    }
}

task write_lines_task {
    Array[File] files

    command <<<
    filenames=${write_lines(files)}
    cat $filenames
    >>>

    output {
        Array[String] strings = read_lines(stdout())
    }
}

task fix_paths {
  Array[String] strings

  command <<<
    python3 <<CODE
    # replace the random directory name with a deterministic one
    dirs = {}
    for f in ["~{sep="','" strings}"]:
      parts = f.split("/")
      if parts[4] not in dirs:
        dir = f"input{len(dirs)}"
        dirs[parts[4]] = dir
      parts[4] = dirs[parts[4]]
      print(f"{'/'.join(parts)}\n")
    CODE
    >>>

    output {
        Array[String] fixed_strings = read_lines(stdout())
    }
}
