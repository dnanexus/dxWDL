version 1.0

# A trivial workflow, to test the basic sanity
# of a dxWDL release.

workflow linear_no_expressions {
    input {
        Int x = 3
        Int y = 5
    }

    call op as op1 {
        input: a=x, b=y
    }

    call op as op2 {
        input: a=op1.result, b=5
    }

    call op as op3 {
        input: a=op2.result, b=op1.result
    }

    output {
        Int r1 = op1.result
        Int r2 = op2.result
        Int r3 = op3.result
    }
}


task op {
    input {
        Int a
        Int b
    }
    command {
        python3 -c "print(~{a} + ~{b} + 1)"
    }
    output {
        Int result = read_int(stdout())
    }
}
