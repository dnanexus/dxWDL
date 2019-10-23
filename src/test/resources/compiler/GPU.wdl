version 1.0

task GPU {
    input {}
    command {
        echo "On a GPU instance"
    }
    runtime {
        dx_instance_type : "mem3_ssd1_gpu_x8"
    }
    output {
        String retval = read_string(stdout())
    }
}
