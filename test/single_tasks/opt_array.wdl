version 1.0

task opt_array {
    input {
        Array[File]? a
        Array[File]? b
    }
    command {}
    output {
        Array[File] o = select_first([a, b])
    }
}
