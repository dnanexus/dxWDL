task TC {
    # Inputs
    Int base_mem_mb
    Int disk_gb

    # runtime expressions
    Int machine_mem_mb = 15000 + base_mem_mb
    Int cpu = 4
    Int disk_gb_x = disk_gb + 1

    command {}
    runtime {
        memory: "${machine_mem_mb} MB"
        disks: "local-disk ${disk_gb_x} HDD"
        cpu: (cpu + 1)
    }
    output {}
}
