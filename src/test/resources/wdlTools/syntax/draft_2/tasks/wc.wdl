# A task that counts how many lines a file has
task wc {
  File inp_file
  # Just a random declaration
  Int i = 4 + 5

  output {  # comment after bracket
    # Int num_lines = read_int(stdout())
    Int num_lines = 3  # end of line comment
  }

  command {
    # this is inside the command and so not a WDL comment
    wc -l ${inp_file}
  }

  meta {
    # The comment below is empty
    #
    author : "Robin Hood"
  }

  parameter_meta {
    inp_file : "just because"
  }
}
