version 1.0

# simple test for scatter

task times_two {
    input {
        Int i
    }
    command {}
    output {
        Int result = 2 * i
    }
}

workflow simple_scatter {
    input {
        Int num
    }

    scatter (item in range(num)) {
        call times_two { input: i = item }
    }

    output {
        Array[Int] result = times_two.result
    }
}
