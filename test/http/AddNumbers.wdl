task AddNumbers {
    Int x
    Int y

    command {
        echo $((x + y))
    }
    output {
        Int result = read_int(stdout())
    }
}
