version 1.0

# check that various symbols occurring after a new line + "\" are not
# lost.
task echo_line_split {

  command {
  echo 1 hello world | sed 's/world/wdl/'
  echo 2 hello \
  world \
  | sed 's/world/wdl/'
  echo 3 hello \
  world | \
  sed 's/world/wdl/'
  }

  output {
    String results = read_string(stdout())
  }
}
