task uu_inc {
    Int i

    command <<<
        python -c "print(${i} + 1)"
    >>>
    output {
        Int incremented = read_int(stdout())
    }
}

task uu_sum {
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
        call uu_inc as inc {input: i=i}
    }
    call uu_sum as sum {input: ints = inc.incremented}
}
