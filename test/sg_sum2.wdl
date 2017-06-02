import "library_math.wdl" as lib

workflow sg_sum2 {
    Array[Int] integers

    scatter (i in integers) {
        call lib.Inc as inc {input: i=i}
        call lib.Twice as twice {input: i=inc.result}
        call lib.Mod7 as mod7 {input: i=twice.result}
    }
    call lib.Sum as sum {input: ints = mod7.result}
}
