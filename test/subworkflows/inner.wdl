version 1.0

workflow inner {
    input {
        String lane
    }

    output {
        String blah = lane
    }
}
