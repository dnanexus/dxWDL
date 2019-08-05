version development

task v2_add {
    input {
        Int a
        Int b
    }
    command {}
    output {
        Int result = a + b
    }
}

workflow v2_dict {
    Map[Int, Float] mIF = {1: 1.2, 10: 113.0}

    scatter (p in as_pairs(mIF)) {
        call v2_add {
            input: a=p.left, b=5
        }
    }

    output {
        Array[String] result = v2_add.result
    }
}
