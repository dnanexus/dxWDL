import "library_math.wdl" as lib
import "library_string.wdl" as lib_str

workflow optionals {
    Array[String] sa = []
    Int arg1
    Array[Int]+ integers
    Int? rain = 13
    Int? snow = 34
    Boolean? flag
    Array[String] foodArray = ["our", "own", "peanut", "butter"]

    # scatter that returns an optional type
#    scatter (i in [1, 10, 100]) {
#        call isLargeInt { input: a=i }
#    }

    # Missing compulsory argument
    call lib.Inc as inc

    call lib_str.ConcatArray as concatArr {
        input: words = sa
    }
    call lib_str.ConcatArray as concatArr2 {
        input: words = foodArray
    }

    call lib_str.Concat as concat { input:
        x = if select_first([flag,false]) then 'OKAY' else 'FAIL',
        y = if defined(flag) then 'OKAY' else 'FAIL'
    }

    call lib.MaybeInt as mi1 { input: a=rain }
    call lib.MaybeInt as mi2 { input: a=mi1.result}
    call lib.MaybeInt as mi3 { input: a=snow}
    call MaybeString as ms1
    call MaybeString as ms2 { input: instrument="flute" }

    call lib.Twice as mul2 { input: i=arg1 }
    call ppp_set_def as set_def
    call ppp_unused_args as unused_args { input: a=1}

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
        inc.result
        concatArr.result
        concat.result
        mi2.result
        mi3.result
        ms1.result
        ms2.result
        mul2.result
        set_def.result
        unused_args.result
        add2.result
        add3.result
        series
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

# A task that sets a default for an optional input
task MaybeString {
    String? instrument = "french horn"
    command {
    }
    output {
        String? result = instrument
    }
}

#task isLargeInt {
#    Int a
#    command {}
#    output {
#        Int? result = {if (a > 1) then a}
#    }
#}
