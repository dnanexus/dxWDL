# A simple workflow that has:
# - A Task
# - A call with expressions
# - Dependencies between calls

task Add {
    Int a
    Int b

    command {
    }
    output {
        Int sum = a + b
    }
}

workflow call_expr_deps {
    call Add {
         input: a = 3, b = 2
    }
    call Add as Add2 {
         input: a = 2 * Add.sum, b = 3
    }
    output {
        Add2.sum
    }
}
