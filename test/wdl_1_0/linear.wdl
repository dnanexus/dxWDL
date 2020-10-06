version 1.0

# A WDL workflow with expressions, but without branches and loops.

workflow linear {
    input {
        Int x = 3
        Int y = 5
    }

    call add {
        input: a=x, b=y
    }

    Int z = add.result + 1
    call mul { input: a=z, b=5 }

    call inc { input: i= z + mul.result + 8}

    output {
        Int result = inc.result
    }
}


task add {
    input {
        Int a
        Int b
    }
    command {
        echo $((~{a} + ~{b}))
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
        echo $((~{a} * ~{b}))
    }
    output {
        Int result = a * b
    }
}

task inc {
    input {
        Int i
    }

    command {
        python3 -c "print(~{i} + 1)"
    }
    output {
        Int result = read_int(stdout())
    }
}
