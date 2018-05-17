task TD {
    Float x

    command {}
    output {
        Int x_floor = floor(x)
        Int x_ceil = ceil(x)
        Int x_round = round(x)
    }
}
