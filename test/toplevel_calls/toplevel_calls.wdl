import "library_math.wdl" as lib

# Check that toplevel calls are compiled to workflow stages, when
# this workflow is compiled unlocked.
workflow toplevel_calls {
    Boolean flag
    Int prime

    # toplevel calls with missing argument
    call lib.Add {input: a = prime}
    call lib.MaybeInt

    # not a toplevel calls
    if (flag) {
        call lib.Inc {input: i=2 }
    }
    scatter (k in [1, 2]) {
        call lib.Inc as inc2 { input: i=k }
    }

    # top level call with subexpression
    call lib.Multiply {input: a=1, b = prime+1 }

    output {
        Int add_r = Add.result
        Int? maybe_int_r = MaybeInt.result
        Int multiply_r = Multiply.result
    }
}
