workflow call_with_defaults {
    Int x
    String output_name

    String quality_control_output_basename = output_name + "_qc"

    call SumWithDefaults { input: x = x }
    output {
        Int result = SumWithDefaults.result
    }
}

task SumWithDefaults {
    Int x = 2
    Int y = 3

    command {
    }
    output {
        Int result = x + y
    }
}
