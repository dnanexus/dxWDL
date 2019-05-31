version 1.0

import "file1.wdl"
import "file2.wdl" as bloop

workflow Blorf {
  input {
    Foo foo = object {
      bar: "bink"
    }
  }
  
  call bloop.Bloop {
    input:
      foo = foo
  }
  
  output {
    File x = Bloop.x
  }
}
