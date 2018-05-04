# Nested conditionals, should not create Optional[Optional] types

workflow D {
    scatter (p in [1, 2, 3, 5, 7]) {
        if (p > 1) {
            if (p < 5) {
                Int chosen = p
            }
        }
    }
    output {
        chosen
    }
}
