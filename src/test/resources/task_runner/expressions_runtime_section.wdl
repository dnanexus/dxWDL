version 1.0

task expressions_runtime_section {
    input {
        Int? cpu
        Int? mem_mb
        Int? time_hr
        String? disks
    }
    command {
    }
    runtime {
	cpu : select_first([cpu,2])
	memory : "${select_first([mem_mb, 1000])} MB"
	time : select_first([time_hr,24])
	disks : select_first([disks,"local-disk 100 HDD"])
    }
    output {
    }
}
