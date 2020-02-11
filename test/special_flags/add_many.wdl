version 1.0

import "add2.wdl" as A

workflow add_many {
  input {
    Int a
    Int b
    Int c
    Int d
  }

  call A.add2 as X1 {input: a=a, b=b}
  call A.add2 as X2 {input: a=c, b=d}
  call A.add2 as X3 {input: a=X1.c, b=X2.c}

  output {
    Int result = X3.c
  }
}
