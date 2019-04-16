version 1.0

task opt_MaybeInt {
    input {
        Int? a
    }
    command {
    }
    output {
        Int? result = a
    }
}

workflow optionals {
    input {
        Boolean flag
    }
    Int? rain = 13

    if (flag) {
        call opt_MaybeInt as mi3 { input: a=rain }
    }

    output {
        Int? r_mi3 = mi3.result
    }
}
