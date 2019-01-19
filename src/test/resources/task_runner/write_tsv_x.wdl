task write_tsv_x {
    Array[Array[String]] words = [["A"], ["C"], ["G", "T"]]
    command {
        cat ${write_tsv(words)}
    }
    output {
        String result = read_string(stdout())
    }
}
