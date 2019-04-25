version 1.0

workflow inner {
    input {
        String lane
    }

#    String tmp = lane

    output {
#        String blah = tmp
        String blah = lane
    }
}
