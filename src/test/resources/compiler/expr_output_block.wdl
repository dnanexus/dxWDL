version 1.0

workflow expr_output_block {
    input {
        Int a
    }

    output {
        Int x = a + 1
    }
}
