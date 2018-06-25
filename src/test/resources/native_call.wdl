task native_call {
    File genomeindex_targz
    Float? sampling_rate
    File reads_fastqgz
    String flowcell
    command {
    }
    runtime {
        dx_instance_type: "mem1_ssd1_x32"
    }
    output {
        File sorted_bam = ""
    }
    meta {
        type: "native"
        id: "applet-FGX2b6j0F9pJF8YP9ZQjfbjv"
    }
}
