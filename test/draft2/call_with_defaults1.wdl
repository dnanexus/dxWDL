workflow call_with_defaults1 {
    Int x
    Boolean flag

    if (flag) {
        call SumWithDefaults { input: x = x }
    }
    output {
        Int? result = SumWithDefaults.result
    }
}

task SumWithDefaults {
    Int x = 2
    Int? y = 3

  Int z = select_first([y])
    command {
    }
    output {
        Int result = x + z
    }
}
