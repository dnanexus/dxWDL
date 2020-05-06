# a local file
import "I.wdl" as biz

# an http address
import "https://raw.githubusercontent.com/dnanexus-rnd/wdlTools/master/src/test/resources/syntax/draft_2/tasks/wc.wdl" as wc

workflow foo {
  call I.biz { input : s = "anybody there?" }
  call I.undefined
  call wc
}
