version 1.0

workflow nested_no_call {
    if (false && true) {
        scatter (x in [1, 2, 3]) {
            Int z = x + 2
        }
    }
}
