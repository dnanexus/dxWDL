version 1.0

import "tools/library.wdl"

workflow foo {
  call library.wc { input : f = "dummy.txt" }
}
