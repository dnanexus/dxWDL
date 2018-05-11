import "library_math.wdl" as lib

workflow complex {
    call lib.ComplexGen as c1 {input: a=2, b=1}
    call lib.ComplexGen as c2 {input: a=3, b=0}
    call lib.ComplexAdd as add {
      input:
        y=c1.result,
        z=c2.result
    }

    Int a1 = add.result.a
    Int b1 = add.result.b

    output {
        Int a = a1
        Int b = b1
    }
}
