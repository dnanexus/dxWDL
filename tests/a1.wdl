task a1_inc {
    Int i

    command <<<
        python -c "print(${i} + 1)"
    >>>
    output {
        Int result = read_int(stdout())
    }
}

task a1_twice {
    Int i

    command <<<
        python -c "print(${i} * 2)"
    >>>
    output {
        Int result = read_int(stdout())
    }
}

task a1_mod7 {
    Int i

    command <<<
        python -c "print(${i} % 7)"
    >>>
    output {
        Int result = read_int(stdout())
    }
}

task a1_sum {
    Array[Int] ints

    command <<<
        python -c "print(${sep="+" ints})"
    >>>
    output {
        Int sum = read_int(stdout())
    }
}

workflow a1 {
    Array[Int] integers
    Array[Array[Array[Int]]] aaai

    scatter (i in integers) {
        call a1_inc as inc {input: i=i}

        # declaration in the middle of a scatter should cause an exception
        String s = "abc"
        call a1_inc as inc2 {input: i=i}
    }
}
