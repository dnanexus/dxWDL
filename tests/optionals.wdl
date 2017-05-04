task ppp_mul2 {
    Int i

    command {
        python -c "print(${i} + ${i})"
    }
    output {
        Int result = read_int(stdout())
    }
}

task ppp_add {
     Int a
     Int b

    command {
        python -c "print(${a} + ${b})"
    }
    output {
        Int result = read_int(stdout())
    }
}

task ppp_set_def {
    Int? i

    command {
        echo "${default=3 i}"
    }
    output {
        Int result = read_int(stdout())
    }
}

# A task that does not use its input arguments.
task ppp_unused_args {
    Int a
    Int? b
    Array[String]? ignore

    command {
        echo "A task that does not access its input arguments"
    }
    output {
        String result = read_string(stdout())
    }
}

workflow optionals {
    Int arg1
    Array[Int] integers = [1,2]

    # A call missing a compulsory argument
    call ppp_mul2 as mul2
    call ppp_mul2 as mul2b { input: i=arg1 }
    call ppp_set_def as set_def
    call ppp_add as add
    call ppp_unused_args as unused_args { input: a=1}
    call ppp_unused_args as unused_args_a { input: a=1, ignore="null"}

    scatter (x in integers) {
        call ppp_unused_args as unused_args_b { input: a=x }
        call ppp_unused_args as unused_args_c { input: a=x*2, ignore=["null"] }
    }
    output {
        mul2.result
        set_def.result
        add.result
        unused_args.result
    }
}
