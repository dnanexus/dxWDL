version 1.0

task float_arith {
    input {
        Float x
    }
    Float y = x + 1
    Float z = y / 2

    command {}

    output {
        Int y_floor = floor(y)
        Int z_ceil = ceil(x)
        Int z_round = round(x)
    }
}
