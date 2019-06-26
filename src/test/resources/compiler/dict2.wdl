version development

task add {
    input {
        Int a
        Int b
    }
    command {}
    output {
        Int result = a + b
    }
}

workflow dict2 {
    Map[Int, Float] mIF = {1: 1.2, 10: 113.0}

    scatter (p in as_pairs(mIF)) {
        call add {
            input: a=p.left, b=5
        }
    }

    output {
        Array[String] result = add.result
    }
}
