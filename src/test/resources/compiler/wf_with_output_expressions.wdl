version 1.0

workflow wf_with_output_expressions {
    input {
        Int a
        String b
    }
    output {
        Int x = a + 1
        String x2 = b + "_" + b
    }
}
