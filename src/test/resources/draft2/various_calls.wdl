# a workflow with various calls
workflow various_calls {
    Array[Int] powers10 = [1, 10]

    # scatter that returns an optional type
    scatter (i in powers10) {
        call MaybeInt { input: a=i }
    }
    Array[Int] r =select_all(MaybeInt.result)

    call ManyArgs { input: a = "hello", b = powers10 }

    output {
        Array[Int] results = r
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

task ManyArgs {
    String a
    Array[Int] b
    Boolean? flag

    command {}
    output {
    }
}
