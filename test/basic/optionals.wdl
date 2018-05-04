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
    call lib.Inc as inc { input: i=106 }

    call lib_str.ConcatArray as concatArr {
        input: words = sa
    }
    call lib_str.ConcatArray as concatArr2 {
        input: words = foodArray
    }

#    call lib_str.Concat as concat { input:
#        x = if select_first([flag,false]) then 'OKAY' else 'FAIL',
#        y = if defined(flag) then 'OKAY' else 'FAIL'
#    }

    call lib.MaybeInt as mi1 { input: a=rain }
    call lib.MaybeInt as mi2 { input: a=mi1.result}
    call lib.MaybeInt as mi3 { input: a=snow}
    call lib_str.MaybeString as ms1
    call lib_str.MaybeString as ms2 { input: instrument="flute" }

    call lib.Twice as mul2 { input: i=arg1 }
    call ppp_set_def as set_def
    call UnusedArgs as unused_args { input: a=1}

    scatter (x in integers) {
        call UnusedArgs as unused_args_b { input: a=x }
        call UnusedArgs as unused_args_c { input: a=x*2, ignore=["pitch"] }

        # verify that unbound compulsory arguments are provided as scatter
        # inputs
        #
        # Note: we temporarily added b=3, to make this work.
        call lib.Add as add2 { input: a=x, b=8 }

        # we need to pass {a, b}
        # FIXME: remove {a,b}
        call lib.Add as add3 {input: a=13, b=21}

        Array[Int] series=[x,1]
    }

    if (true) {
        # Missing argument [i]
        call lib.Inc as inc2 { input: i=107 }
    }

    output {
        Int inc_result = inc.result
        String concatArr_result = concatArr.result
#        String concat_result = concat.result
        Int? mi2_result = mi2.result
        Int? mi3_result = mi3.result
        String? ms1_result = ms1.result
        String? ms2_result = ms2.result
        Int mul2_result = mul2.result
        Int set_def_result = set_def.result
        String unused_args_result = unused_args.result
        Array[Int] add2_result = add2.result
        Array[Int] add3_result = add3.result
        Array[Array[Int]] out_series = series
        Int? inc2_result = inc2.result
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
task UnusedArgs {
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

#task isLargeInt {
#    Int a
#    command {}
#    output {
#        Int? result = {if (a > 1) then a}
#    }
#}
