# Specify the disk space
task DiskSpaceSpec {
  Int disk_req_gb

  command <<<
    lines=$(df -t btrfs | grep dev)
    size_kb=$(echo $lines | cut -d ' ' -f 2)
    size_gb=$(echo "$size_kb / (1024 * 1024)" | bc)
    if [[ $size_gb -ge disk_req_gb ]]; then
       echo "true"
    else
       echo "false"
   fi

>>>
  runtime {
    disks: "local-disk " + disk_req_gb + " HDD"
  }
  output {
     String retval = read_string(stdout())
  }
}

task MemorySpec {
  Int memory_req_gb

  command <<<
    line=$(cat /proc/meminfo | grep MemTotal)
    size_kb=$(echo $line | cut -d ' ' -f 2)
    size_gb=$(echo "$size_kb / (1024 * 1024)" | bc)
    if [[ $size_gb -ge $memory_req_gb ]]; then
       echo "true"
    else
       echo "false"
   fi
  >>>
  runtime {
    memory: memory_req_gb + " GB"
  }
  output {
    String retval = read_string(stdout())
  }
}

task NumCoresSpec {
  Int num_cores_req

  command {
    num_cores=$(grep -c ^processor /proc/cpuinfo)
    if [[ $num_cores -ge $num_cores_req ]]; then
       echo "true"
    else
       echo "false"
    fi
  }
  runtime {
    cpu: num_cores_req
  }
  output {
    String retval = read_string(stdout())
  }
}

workflow instance_types {
  call DiskSpaceSpec { input: disk_req_gb=90 }
  call MemorySpec { input: memory_req_gb=12 }
  call NumCoresSpec { input: num_cores_req=5 }
  output {
    MemorySpec.retval
    DiskSpaceSpec.retval
    NumCoresSpec.retval
  }
}