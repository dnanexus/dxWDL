# A
workflow declaration_after_call {
    Array[Int] numbers = [1, 2, 5, 10]

    scatter (i in numbers) {
        call add { input: a=i, b=1 }
        Int n = add.result
    }

    output {
        Array[Int] ns = n
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
