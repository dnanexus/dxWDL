version 1.0

task cast {
    input {
        Int a
        Boolean b
    }
    String sa = a
    String sb = b

    command {
    }
    output {
        String result_a = sa
        String result_b = sb
    }
}
