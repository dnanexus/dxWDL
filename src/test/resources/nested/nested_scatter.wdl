task sub {
    Int a
    Int b
    command {
    }
    output {
        Int result = a - b
    }
}

workflow nested_scatter {
    Array[Int] numbers = [1, 2, 5, 10]

    scatter (a in numbers) {
        scatter (b in numbers) {
            call sub { input: a=a, b=b }
        }
    }
    output {
        sub.result
    }
}
