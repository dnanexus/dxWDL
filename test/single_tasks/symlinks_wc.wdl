version 1.0

task symlinks_wc {
  input {
    File a
  }

  # figure out how many lines there are in all three files together
  command <<<
    wc -l ~{a} | cut --delimiter=' ' --fields=1
  >>>

  output {
    Int result = read_int(stdout())
  }
}
