task ps {
    command {
        ps aux
    }
    output {
        String procs = read_string(stdout())
    }
}
