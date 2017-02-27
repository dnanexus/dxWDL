task apply {
    String s
    Int? num
    String? foo
    command {
        echo "${s} --K -S --flags --contamination ${default=0 num} --s ${default="foobar" foo}"
    }
    output {
        String result = read_string(stdout())
    }
}

workflow optionals {
    String species
    Int? i
    File? empty

    call apply {
        input: s=species, num=i
    }
    output {
        apply.result
    }
}
