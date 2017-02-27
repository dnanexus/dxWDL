task inc {
    Int i

    command <<<
        python -c "print(${i} + 1)"
    >>>
    output {
        Int incremented = read_int(stdout())
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

workflow sg_sum {
    Array[Int] integers

    scatter (i in integers) {
        call inc {input: i=i}
    }
    call sum {input: ints = inc.incremented}
}
