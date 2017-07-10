import "library_math.wdl" as lib

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

task MaybeInt {
    Int? a
    command {
    }
    output {
        Int? result = a
    }
}

workflow optionals {
    Int arg1
    Array[Int] integers = [1,2]
    Int? rain

    call MaybeInt as mi1 { input: a=rain }
    call MaybeInt as mi2 { input: a=mi1.result}

    # A call missing a compulsory argument
    call lib.Twice as mul2
    call lib.Twice as mul2b { input: i=arg1 }
    call ppp_set_def as set_def
    call lib.Add as add
    call ppp_unused_args as unused_args { input: a=1}
    call ppp_unused_args as unused_args_a { input: a=1, ignore="null"}

    scatter (x in integers) {
        call ppp_unused_args as unused_args_b { input: a=x }
        call ppp_unused_args as unused_args_c { input: a=x*2, ignore=["null"] }

        # verify that unbound compulsory arguments are provided as scatter
        # inputs
        call lib.Add as add2 { input: a=x }

        # we need to pass {a, b}
        call lib.Add as add3
    }
    output {
        mul2.result
        set_def.result
        add.result
        unused_args.result
        add2.result
        mi2.result
    }
}
