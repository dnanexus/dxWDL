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

workflow a1 {
    Array[Int] integers
    Array[Array[Array[Int]]] aaai

    scatter (i in integers) {
        call inc {input: i=i}

        # declaration in the middle of a scatter should cause an exception
        String s = "abc"
        call inc as inc2 {input: i=i}
    }
}
