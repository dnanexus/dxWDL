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

task RuntimeDockerChoice {
  String imageName

  command {
    python <<CODE
    import os
    import sys
    print("We are inside a python docker image")
    CODE
  }
  runtime {
    docker: imageName
    memory: "2 GB"
  }
  output {
    String retval = read_string(stdout())
  }
}

task RuntimeDockerChoice2 {
  String imageName

  command {
    python <<CODE
    import os
    import sys
    print("We are inside a python docker image")
    CODE
  }
  runtime {
    docker: "${imageName}"
    memory: "2 GB"
  }
  output {
    String retval = read_string(stdout())
  }
}

task Shortcut {
    command {
        line=$(cat /proc/meminfo | grep MemTotal)
        size_kb=$(echo $line | cut -d ' ' -f 2)
        size_gb=$(echo "$size_kb / (1024 * 1024)" | bc)
        echo $size_gb
    }
    runtime {
        dx_instance_type: "mem1_ssd2_x4"
        memory: "invalid memory specification --- should not be used"
        disks: "invalid disk specification --- should not be used"
    }
    output {
        String retval = read_string(stdout())
    }
}

workflow instance_types {
    call DiskSpaceSpec { input: disk_req_gb=90 }
    call MemorySpec { input: memory_req_gb=12 }
    call NumCoresSpec { input: num_cores_req=5 }
    call RuntimeDockerChoice { input: imageName="python:2.7" }
    call RuntimeDockerChoice2 { input: imageName="python:2.7" }
    call Shortcut
    output {
        MemorySpec.retval
        DiskSpaceSpec.retval
        NumCoresSpec.retval
        RuntimeDockerChoice.retval
        RuntimeDockerChoice2.retval
        Shortcut.retval
    }
}
