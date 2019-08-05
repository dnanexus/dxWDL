version 1.0

workflow path_not_taken {
    if (false) {
        call compare
    }
    output {
        Boolean? equality = compare.equality
    }
}

task compare {
    command {}
    output {
        Boolean equality = true
    }
}
