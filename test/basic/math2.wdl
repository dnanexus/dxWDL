import "library_math.wdl" as lib

workflow math2 {
    Array[Int]+ numbers

    # conditional block with several calls and declarations
    if (length(numbers) > 0) {
        Int f0 = 2
        Int f1 = 3

        call lib.Add as fibo_add1 { input: a = f0, b = f1 }
        call lib.Add as fibo_add2 { input: a = fibo_add1.result, b=f1 }
    }

    output {
        Int? r = fibo_add2.result
    }
}
