task mul2 {
    Int i

    command {
        python -c "print(${i} + ${i})"
    }
    output {
        Int result = read_int(stdout())
    }
}

task add {
     Int a
     Int b

    command {
        python -c "print(${a} + ${b})"
    }
    output {
        Int result = read_int(stdout())
    }
}

task set_def {
    Int? i

    command {
        echo "${default=3 i}"
    }
    output {
        Int result = read_int(stdout())
    }
}

# A task that does not use its input arguments.
task unused_args {
    Int a
    Int b
    Int? c
    Int? d

    command {
        echo "A task that does not access its input arguments"
    }
    output {
        String result = read_string(stdout())
    }
}

workflow optionals {
    Int arg1

    # A call missing a compulsory argument
    call mul2
    call mul2 as mul2b { input: i=arg1 }
    call set_def
    call add
    call unused_args
    output {
        mul2.result
        set_def.result
        add.result
        unused_args.result
    }
}
