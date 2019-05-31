version 1.0

import "foo_def.wdl"
import "bloop.wdl" as bloop

workflow blorf {
  input {
    Foo foo = object {
      bar: "bink"
    }
  }

  call bloop.bloop {
    input:
      foo = foo
  }

  output {
    File x = bloop.x
  }
}
