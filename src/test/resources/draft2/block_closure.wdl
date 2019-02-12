# draft-2

# Test closure inference.

task inc {
    Int a
    command {
    }
    output {
        Int result = a + 1
    }
}

workflow block_closure {
    Boolean flag
    Int rain = 13

    call inc as inc1 { input: a=rain }

    if (flag) {
        call inc as inc2 { input: a=rain }
    }

    # optional block
    if (flag) {
        call inc as inc3 { input: a=inc1.result }
    }

    # scatter block
    scatter (num in [1, 2, 3]) {
        call inc as inc4 { input: a = rain }
    }

    # nested blocks
    scatter (num in [1, 2, 3]) {
        Int x = num
        if (flag) {
            call inc as inc5 { input: a = num + inc1.result + rain }
        }
    }

    output {
        Int r1 = inc1.result
        Int? r2 = inc2.result
        Int? r3 = inc3.result
        Array[Int] r4 = inc4.result
    }
}
