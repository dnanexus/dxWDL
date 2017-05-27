import "library.wdl" as lib

workflow sg_sum {
    Array[Int] integers

    scatter (i in integers) {
        call lib.Inc as inc {input: i=i}
    }
    call lib.Sum as sum {input: ints = inc.result}
}
