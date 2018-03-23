task sub {
    Int a
    Int b
    command {
    }
    output {
        Int result = a - b
    }
}

workflow nested_scatter_if {
    Array[Int] numbers = [1, 2, 5, 10]

    scatter (a in numbers) {
        if (a > 4) {
            call sub { input: a=a, b=4 }
        }
    }
    output {
        sub.result
    }
}
