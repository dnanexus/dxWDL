version 1.0

# Check that shell commands split into multiple lines work
#
task multi_line {

    command {
    ls /home \
          -l \
          -r \
          -h
    }

    output {
        String results = read_string(stdout())
    }
}
