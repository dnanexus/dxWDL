version 1.0

# A WDL workflow with expressions, but without branches and loops.

workflow wf_meta {
    input {
        Int x = 3
        Int y = 5
    }

    meta {
        title: "Workflow with metadata"
        summary: "A workflow that defines some metadata"
        description: "This is a workflow that defines some metadata"
        version: "1.0"
        details: {
            whatsNew: "v1.0: First release"
        }
        tags: ["foo", "bar"]
        properties: {
            foo: "bar"
        }
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
