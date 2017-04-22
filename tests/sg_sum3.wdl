task ss_inc {
    Int i

    command <<<
        python -c "print(${i} + 1)"
    >>>
    output {
        Int result = read_int(stdout())
    }
}

task ss_twice {
    Int i

    command <<<
        python -c "print(${i} * 2)"
    >>>
    output {
        Int result = read_int(stdout())
    }
}

task ss_mod7 {
    Int i

    command <<<
        python -c "print(${i} % 7)"
    >>>
    output {
        Int result = read_int(stdout())
    }
}

task ss_sum {
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
        call ss_inc as inc {input: i=k}
        call ss_twice as twice {input: i=inc.result}
        call ss_mod7 as mod7 {input: i=twice.result}
    }
    scatter (k in mod7.result) {
        call ss_inc as inc2 {input: i=k}
    }
    call ss_sum as sum {input: ints = inc2.result}
}
