version 1.0

workflow subblock_several_calls {
    input {
        Int n = 4
    }
    scatter (i in range(n)) {
        call inc as inc1 { input: i = i}
        call inc as inc2 { input: i = inc1.result}
    }
    output {
        Array[Int] result = inc2.result
    }
}

task inc {
    input {
        Int i
    }
    command {}
    output {
        Int result = i + 1
    }
}
