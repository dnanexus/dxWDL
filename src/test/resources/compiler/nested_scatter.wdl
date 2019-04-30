version 1.0

workflow nested_scatter {

    scatter (x in [1, 2, 3]) {
        scatter (y in [10, 11]) {
            call add{ input: a = x, b = y }
        }
    }

    output {
        Array[Array[Int]] result = add.result
    }
}


task add {
    input {
        Int a
        Int b
    }
    command {}
    output {
        Int result = a + b
    }
}
