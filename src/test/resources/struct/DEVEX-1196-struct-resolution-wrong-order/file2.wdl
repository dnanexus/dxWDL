version 1.0

import "file1.wdl"

task Bloop {
  input {
    Foo foo
    SampleReportsArray? rep
  }

  command <<<
  echo ~{foo.bar}
  touch x
  >>>
 
  output {
    File x = "x"
  }
}  
