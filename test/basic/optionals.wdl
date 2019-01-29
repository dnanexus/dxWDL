version 1.0

task opt_MaybeInt {
    Int? a
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

    call opt_MaybeInt as mi1 { input: a=rain }
    call opt_MaybeInt as mi2 { input: a=mi1.result}

    if (flag) {
        call opt_MaybeInt as mi3 { input: i=rain }
    }
    if (flag || false) {
        call opt_MaybeInt as mi4 { input: i=mi2.result }
    }


    output {
        Int? r_mi1 = mi1.result
        Int? r_mi2 = mi2.result
        Int? r_mi3 = mi3.result
        Int? r_mi4 = mi4.result
    }
}
