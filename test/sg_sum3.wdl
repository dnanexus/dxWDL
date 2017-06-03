import "library_math.wdl" as lib

workflow sg_sum3 {
    Array[Int] numbers

    # - several calls in a scatter
    # - expression in the collection
    # - use of the range and length stdlib functions
    scatter (k in range(length(numbers))) {
        call lib.Inc as inc {input: i= numbers[k]}
        call lib.Twice as twice {input: i=inc.result}
        call lib.Mod7 as mod7 {input: i=twice.result}
    }

    # More than one scatter in a workflow
    scatter (k in mod7.result) {
        call lib.Inc as inc2 {input: i=k}
    }

    # expression in the collection
    scatter (k in [1,2,3]) {
        call lib.Inc as inc3 {input: i=k}
    }

    call lib.Sum as sum {input: ints = inc2.result}

    output {
        sum.result
    }
}
