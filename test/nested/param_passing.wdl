version 1.0

workflow param_passing {
    input {
        Int num
        Boolean flag
    }

    scatter (i in [1,2,3]) {
        call zinc as inc1 { input: a = i + num}
        call zinc as inc2 { input: a = inc1.result}
    }

    if (flag) {
        call zinc as inc3 { input: a = num}
        call zinc as inc4 { input: a = num + 3 }
    }

    output {
        Array[Int] r1 = inc2.result
        Int? r2 = inc4.result
    }
}


task zinc {
    input {
        Int a
    }
    command {}
    output {
        Int result = a+ 1
    }
}
