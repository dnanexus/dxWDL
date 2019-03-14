workflow expression_after_call {
    Array[String] numbers

    scatter (i in numbers) {
        call gen_str_array { input : a=i }
    }

    Array[String] f_results = flatten(gen_str_array.result)
    output {
        Array[String] results = f_results
    }
}

task gen_str_array {
    Int a

    command {}
    output {
        Array[String] result = ["a", "b"]
    }
}
