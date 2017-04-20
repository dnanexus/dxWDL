task inc {
    Int i

    command <<<
        python -c "print(${i} + 1)"
    >>>
    output {
        Int result = read_int(stdout())
    }
}

task twice {
    Int i

    command <<<
        python -c "print(${i} * 2)"
    >>>
    output {
        Int result = read_int(stdout())
    }
}

task mod7 {
    Int i

    command <<<
        python -c "print(${i} % 7)"
    >>>
    output {
        Int result = read_int(stdout())
    }
}

task sum {
    Array[Int] ints

    command <<<
        python -c "print(${sep="+" ints})"
    >>>
    output {
        Int sum = read_int(stdout())
    }
}

workflow sg_sum3 {
    Array[Int] integers

    scatter (k in integers) {
        call inc {input: i=k}
        call twice {input: i=inc.result}
        call mod7 {input: i=twice.result}
    }
    scatter (k in mod7.result) {
        call inc as inc2 {input: i=k}
    }
    call sum {input: ints = inc2.result}
}
