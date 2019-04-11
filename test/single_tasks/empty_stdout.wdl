# The stdout() call should return an empty string
# if nothing was written to standard output.
#
task empty_stdout {
    command {}
    output {
        String result = read_string(stdout())
    }
}
