import "library_math.wdl" as lib

workflow math {
    Array[Int]+ numbers
    Int ai
    Int bi

    call lib.ArrayLength as aLen { input: ai=[] }

    # Conversion from Array[Int] to Array[Array[Int]]
    scatter (k in [2,3,5]) {
        call lib.RangeFromInt as rfi { input: len=k }
    }

    # simple If block
    if (ai < 10) {
        call lib.Inc as cond_inc { input: i=ai}
    }

    if (12 < 10) {
        # This is not supposed to run
        Int false_branch = 10
    }

    # conditional block with several calls and declarations
    if (length(numbers) > 0) {
        Int f0 = 2
        Int f1 = 3

        call lib.Add as fibo_add1 { input: a = f0, b = f1 }
        call lib.Add as fibo_add2 { input: a = fibo_add1.result, b=f1 }
    }

    call lib.IntOps as int_ops1 {
        input: a=ai, b=bi
    }
    call lib.IntOps as int_ops2 {
        input: a = (3 * 2), b = (5 + 1)
    }
    call lib.IntOps as int_ops3 {
        input: a = (3 + 2 - 1), b = 5 * 2
    }
    call lib.IntOps as int_ops4 {
        input: a = (ai * 2), b = (bi+3)
    }
    call lib.IntOps as int_ops5 {
        input: a = (int_ops4.result * 5), b = (int_ops4.result + 1)
    }

    # several calls in a scatter
    # expression in the collection
    # use of the range and length stdlib functions
    # expressions that should be collected at the top
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

    call lib.Sum as inc_sum {input: ints = inc2.result}
    Pair[Float,Int] p = (1.0, 19)

    output {
        Int zero_len = aLen.result
        Int? invalid = false_branch
        Int x = int_ops3.mul
        Int y = int_ops3.sub
        Int sum = int_ops5.sum
        Int div = int_ops5.div
        Int sum2 = inc_sum.result
        Int? ai_inc_maybe = cond_inc.result
        Int? fibo2 = fibo_add2.result
        Array[Array[Int]] rra = rfi.result

        # Expressions in output section
        Int expr1 = ai + bi
        Float z = p.left
    }
}
