version 1.0

# a local file
import "I.wdl" as I

# an http address
import "https://raw.githubusercontent.com/dnanexus-rnd/wdlTools/master/src/test/resources/syntax/v1_0/tasks/wc.wdl" as wc

workflow foo {
  call I.biz { input : s = "anybody there?" }
  call I.undefined
  call wc
}
