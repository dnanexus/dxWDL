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

# A task that sets a default for an optional input
task MaybeString {
    String? instrument = "french horn"
    command {
    }
    output {
        String? result = instrument
    }
}

workflow optionals {
    Int arg1
    Array[Int] integers = [1,2]
    Int? rain = 13
    Int? snow = 34

    call MaybeInt as mi1 { input: a=rain }
    call MaybeInt as mi2 { input: a=mi1.result}
    call MaybeInt as mi3 { input: a=snow}
    call MaybeString as ms1
    call MaybeString as ms2 { input: instrument="flute" }

    # A call missing a compulsory argument
    call lib.Twice as mul2
    call lib.Twice as mul2b { input: i=arg1 }
    call ppp_set_def as set_def
    call lib.Add as add
    call ppp_unused_args as unused_args { input: a=1}

    # TODO: how is null represented in wdl4s?
    #call ppp_unused_args as unused_args_a { input: a=1, ignore=null}

    scatter (x in integers) {
        call ppp_unused_args as unused_args_b { input: a=x }
        call ppp_unused_args as unused_args_c { input: a=x*2, ignore=["pitch"] }

        # verify that unbound compulsory arguments are provided as scatter
        # inputs
        call lib.Add as add2 { input: a=x }

        # we need to pass {a, b}
        call lib.Add as add3

        Array[Int] series=[x,1]
    }
    output {
        mi2.result
        mi3.result
        ms1.result
        ms2.result
        mul2.result
        set_def.result
        add.result
        unused_args.result
        add2.result
        add3.result
        series
    }
}
