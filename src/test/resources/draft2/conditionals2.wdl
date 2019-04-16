workflow conditionals2 {
    Int n = 67
    scatter (i in [1, 3, 5]) {
        Int k = i + 3
        call dummyOp {input: a=i, b=k, n=n}
    }

    output {
        Array[Int] r = dummyOp.result
    }
}

task dummyOp {
    Int a
    Int b
    Int n
    command {}
    output {
        Int result = a + b + n + 1
    }
}
