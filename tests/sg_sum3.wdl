import "math_lib.wdl" as lib

workflow sg_sum3 {
    Array[Int] integers

    scatter (k in integers) {
        call lib.Inc as inc {input: i=k}
        call lib.Twice as twice {input: i=inc.result}
        call lib.Mod7 as mod7 {input: i=twice.result}
    }
    scatter (k in mod7.result) {
        call lib.Inc as inc2 {input: i=k}
    }
    call lib.Sum as sum {input: ints = inc2.result}
}
