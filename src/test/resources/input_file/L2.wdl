workflow L2 {
    Int x
    Int y

    call subtract {input: x=x, y=y}
}

task subtract {
    Int x
    Int y

    command {}
    output {
        Int result = x + y
    }
}
