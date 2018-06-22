import "hello.wdl" as hello

workflow outer {
    String? who
    call hello.wf { input: who = who }
    output {
        String result = wf.result
        String result2 = wf.result2
    }
}
