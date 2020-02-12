task ps {
    command {
        ps aux | head -c 1000
    }
    output {
        String procs = read_string(stdout())
    }
}
