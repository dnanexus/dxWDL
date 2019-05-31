version 1.0

import "foo_def.wdl"

task bloop {
  input {
    Foo foo
  }

  command <<<
  echo ~{foo.bar}
  touch x
  >>>

  output {
    File x = "x"
  }
}
