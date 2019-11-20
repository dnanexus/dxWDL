version 1.0

struct WordStruct {
    String word
    Int len
}

workflow runtime_vs_static_type {
    call opt_int { input: x = 14 }

    call opt_array { input: xa = [14,15,20] }

    WordStruct manitoba = {
        "word": "Manitoba",
        "len": 8
    }
    call opt_struct { input : ao = [manitoba] }

    output {
        Int result = opt_int.result
        String result2 = opt_array.numbers
        String result3 = opt_struct.w
    }
}


task opt_int {
    input {
        Int? x
    }
    command {
        echo $(( ~{x} + 10 ))
    }
    output {
        Int result = read_int(stdout())
    }
}


task opt_array {
    input {
        Array[Int?] xa
    }
    Array[Int] a = select_all(xa)
    command {
        echo ~{sep=',' a}
    }
    output {
        String numbers = read_string(stdout())
    }
}

task opt_struct {
    input {
        Array[WordStruct?]? ao
    }
    Array[WordStruct?] a = select_first([ao])
    Array[WordStruct] a2 = select_all(a)
    command{}
    output {
        String w = a2[0].word
    }
}
