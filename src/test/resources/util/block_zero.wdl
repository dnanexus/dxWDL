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
workflow block_zero {
    input {
        Boolean flag
    }
    Int rain = 19

    if (flag) {
        call inc { input: a=rain }
    }

    output {
        Int? r = inc.result
    }
}
