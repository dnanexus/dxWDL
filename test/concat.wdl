import "library_string.wdl" as lib

workflow concat {
    String s1
    String s2

    call lib.Concat as join2 {
        input: x = s1, y = s2
    }
    output {
        join2.result
    }
}
