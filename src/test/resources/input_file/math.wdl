workflow math {
    Int x

    scatter (i in [1, 2, 3 ]) {
        call add {input: a=i, b=x}
    }
    output {
        Array[Int] result = add.result
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
