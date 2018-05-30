# Check setting optional parameters for a task from
# the input file.

workflow population {
    # The initial population value must be between 0 and 1
    Float start
    call next_year{ input: pop = start }
    Int pop_ny =  ceil(1000 * next_year.result)

    output {
        Int result = pop_ny
    }
}

task next_year {
    Float pop
    Float factor = 3

    command {}
    output {
        Float result = factor * (1 - pop) * pop
    }
}
