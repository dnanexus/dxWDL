import "library_math.wdl" as lib

workflow complex {
    call lib.ComplexGen as c1 {input: a=2, b=1}
    call lib.ComplexGen as c2 {input: a=3, b=0}
    call lib.ComplexAdd {input:  y=c1.result,
                                 z=c2.result }
    output {
        Int a = c1.result.a
        Int b = c2.result.b
    }
}
