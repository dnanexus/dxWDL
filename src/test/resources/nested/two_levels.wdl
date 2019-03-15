version 1.0

workflow two_levels {
    input {
    }

    scatter (i in [1,2,3]) {
        call inc as inc1 { input: a = i}
        call inc as inc2 { input: a = inc1.result }

        Int b = inc2.result

        call inc as inc3 { input: a = b }
    }

    if (true) {
        call inc as inc4 { input: a = 3 }
    }

    call inc as inc5 {input: a=1}

    output {
        Array[Int] a = inc3.result
        Int? b = inc4.result
        Int c = inc5.result
    }
}


task inc {
    input {
        Int a
    }
    command {}
    output {
        Int result = a+ 1
    }
}
