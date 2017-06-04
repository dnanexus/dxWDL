import "library_math.wdl" as lib

workflow sg_sum {
    Array[Int] numbers = [4, 5, 7]

    scatter (i in numbers) {
        call lib.Inc as inc {input: i=i}
    }
    call lib.Sum as sum {input: ints = inc.result}

    output {
       sum.result
    }
}
