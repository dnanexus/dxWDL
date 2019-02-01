version 1.0

task inc {
    input {
        Int a
    }
    command {
    }
    output {
        Int result = a
    }
}

# Check that we process correctly the first block.
# In this case, it is:
#  Int? rain = 13
workflow block_with_three_calls {
    input {
        Boolean flag
    }
    Int rain = 13

    if (flag) {
        call inc as inc1 { input: a=rain }
        call inc as inc2 { input: a=inc1.result }
        call inc as inc3 { input: a=inc2.result }
    }

    output {
        Int? r = inc3.result
    }
}
