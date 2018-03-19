task sub {
    Int a
    Int b
    command {
    }
    output {
        Int result = a - b
    }
}

task add {
    Int a
    Int b
    command {
    }
    output {
        Int result = a + b
    }
}

# A sub-block that is compiled into a subworkflow
workflow long_block {
    Array[Int] numbers = [1, 2, 5, 10]

    scatter (i in numbers) {
        call add { input: a=i, b=1 }

        Int k = add.result + 2

        call sub { input: a=k, b=3 }
    }

    output {
        add.result
        sub.result
    }
}
