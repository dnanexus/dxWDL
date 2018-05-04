workflow naming_scatters {
    Array[Int]+ numbers = [19, 23, 27]

    scatter (k in [2,3,5]) {
        call inc { input: a=k }
    }

    scatter (k in range(length(numbers))) {
        call dec {input: a= numbers[k]}
        call add {input: a= dec.result, b=numbers[k] }
    }

    output {
        Array[Int] inc_results = inc.result
        Array[Int] dec_results = dec.result
        Array[Int] add_results = add.result
    }
}

task inc {
    Int a
    command {
    }
    output {
        Int result = a + 1
    }
}

task dec {
    Int a
    command {
    }
    output {
        Int result = a - 1
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
