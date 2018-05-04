workflow map {
    Map[Int, Float] mIF  = {1: 1.2, 10: 113.0}

    scatter (pair in mIF) {
        call add {
            input:  a=pair.left, b=5
        }
    }

    Pair[Int, Int] p2  = (5, 8)
    call mul {
        input:  a=p2.left, b=p2.right
    }

}

task add {
    Int a
    Int b
    command {
        echo $((${a} + ${b}))
    }
    output {
        Int result = read_int(stdout())
    }
}

task mul {
    Int a
    Int b
    command {
    }
    output {
        Int result = a * b
    }
}
