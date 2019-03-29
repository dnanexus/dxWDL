task square {
    Int i
    command {}
    output {
        Int result = i * i
    }
}

task volume {
    Int i
    command {}
    output {
        Int result = i * i * i
    }
}

workflow shapes {
    Int num

    scatter (item in range(num)) {
        call square { input: i = item }
    }

    call volume { input: i = 10 }

    output {
        Array[Int] r1 = square.result
        Int r2 = volume.result
    }
}
