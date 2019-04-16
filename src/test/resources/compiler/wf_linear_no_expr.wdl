version 1.0

# A trivial workflow, to test the basic sanity
# of a dxWDL release.

workflow wf_linear_no_expr {
    input {
        Int x = 3
        Int y = 5
    }

    call add {
        input: a=x, b=y
    }

    call mul {
        input: a=add.result, b=5
    }

    call mul as mul2 {
        input: a=4, b = 8
    }

    output {
        Int r1 = add.result
        Int r2 = mul.result
        Int r3 = mul2.result
    }
}


task add {
    input {
        Int a
        Int b
    }
    command {
        echo $((${a} + ${b}))
    }
    output {
        Int result = read_int(stdout())
    }
}

task mul {
    input {
        Int a
        Int b
    }

    command {
        echo $((a * b))
    }
    output {
        Int result = a * b
    }
}

task inc {
    input {
        Int i
    }

    command <<<
        python -c "print(${i} + 1)"
    >>>
    output {
        Int result = read_int(stdout())
    }
}
