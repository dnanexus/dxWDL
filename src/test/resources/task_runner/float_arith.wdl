version 1.0

task float_arith {
    input {
        Float x
    }
    Float y = x + 1  # y = 2
    Float z = y / 2 # z = 1

    command {}

    output {
        Int x_round = round(x) # 1
        Int y_floor = floor(y)  # 2
        Int z_ceil = ceil(z) # 1
    }
}
