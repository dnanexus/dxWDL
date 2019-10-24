version 1.0

task GPU2 {
    input {}
    command {
        echo "On a GPU instance"
    }
    runtime {
        gpu : true
        memory : "2 GiB"
    }
    output {
        String retval = read_string(stdout())
    }
}
