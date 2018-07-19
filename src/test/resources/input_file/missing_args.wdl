# Check that toplevel calls are compiled to workflow stages, when
# this workflow is compiled unlocked.
workflow missing_args {
    Boolean flag
    Int prime

    # toplevel calls with missing argument
    call Add {input: a = prime}
    call MaybeInt

    # not a toplevel calls
    if (flag) {
        call Add as add2 {input: a=2, b = 3 }
    }
}


task Add {
    Int a
    Int b

    command {
        echo $((${a} + ${b}))
    }
    output {
        Int result = read_int(stdout())
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
