version 1.0

workflow three_levels {
    input {
        Int score
    }

    if (score > 1) {
        if (score > 10) {
            if (score == 100) {
                call zinc as c1 { input: a=13 }
            }
            if (score == 101) {
                call zinc as c2 { input: a=14 }
            }
        }
    }


    output {
        Int? result1 = c1.result
        Int? result2 = c2.result
    }
}


task zinc {
    input {
        Int a
    }
    command {}
    output {
        Int result = a+ 1
    }
}
