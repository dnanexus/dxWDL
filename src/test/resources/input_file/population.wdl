# Check setting optional parameters for a task from
# the input file.

workflow population {
    # The initial population value must be between 0 and 1
    Float start
    call next_year{ input: pop = start }
}

task next_year {
    Float pop
    Float? factor = 3
    Float factor_x = select_first([factor])

    command {}
    output {
        Float result = factor_x * (1 - pop) * pop
    }
}
