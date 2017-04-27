task rr_inc {
    Int i

    command <<<
        python -c "print(${i} + 1)"
    >>>
    output {
        Int result = read_int(stdout())
    }
}

task rr_twice {
    Int i

    command <<<
        python -c "print(${i} * 2)"
    >>>
    output {
        Int result = read_int(stdout())
    }
}

task rr_mod7 {
    Int i

    command <<<
        python -c "print(${i} % 7)"
    >>>
    output {
        Int result = read_int(stdout())
    }
}

task rr_sum {
    Array[Int] ints

    command <<<
        python -c "print(${sep="+" ints})"
    >>>
    output {
        Int sum = read_int(stdout())
    }
}

workflow sg_sum2 {
    Array[Int] integers

    scatter (i in integers) {
        call rr_inc as inc {input: i=i}
        call rr_twice as twice {input: i=inc.result}
        call rr_mod7 as mod7 {input: i=twice.result}
    }
    call rr_sum as sum {input: ints = mod7.result}
}
