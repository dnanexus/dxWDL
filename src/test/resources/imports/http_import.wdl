# check that we can import an HTTP url
import "https://raw.githubusercontent.com/dnanexus/dxWDL/master/doc/demo/hello_world.wdl" as hello
#import "https://raw.githubusercontent.com/dnanexus/dxWDL/develop/test/http/AddNumbers.wdl" as hello

workflow X {
    call hello.AddNumbers {input: x= 1, y=3}
    output {
        Int result = AddNumbers.result
    }
}
