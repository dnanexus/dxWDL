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
    Int a = 3

    if (a > 4) {
        scatter (x in [1, 2, 5, 10]) {
            call sub { input: a=x, b=4 }
        }
    }
    output {
        sub.result
    }
}
