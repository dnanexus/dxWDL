import "library_math.wdl" as lib

# Check that toplevel calls are compiled to workflow stages, when
# this workflow is compiled unlocked.
workflow toplevel_calls {
    Boolean flag
    Int prime

    call Add {input: a = prime, b = 1 }

    # toplevel call with missing argument
    call Multiply {input: a = prime }

    # not a toplevel call
    if (flag) {
        call Sub
    }

    if (false) {
        Int i2 = 10
    }
    if (true) {
        Int i3 = 100
    }

    Array[Int?] powers10 = [i1, i2, i3]

    # scatter that returns an optional type
    scatter (i in powers10) {
        call lib.MaybeInt { input: a=i }
    }
    Array[Int] r =select_all(MaybeInt.result)
    output {
        Array[Int] results = r
    }
}
