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

task DiskSpaceTaskDeclarations {
    Int dummy_arg
    File fruits

    # use provided disk number or dynamically size on our own,
    Int disk_req_gb = ceil(size(fruits, "GB")) + 50

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
        disks: "local-disk ${disk_req_gb} HDD"
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
    Int i
    File fruits

    ### Disk space tests
    call DiskSpaceSpec { input: disk_req_gb=90 }

    # The following is compiled into a fragment. This checks
    # the precalculation of instance types.
    call DiskSpaceSpec as diskSpec2 { input: disk_req_gb= 200 + i }

    call DiskSpaceTaskDeclarations as diskSpec3 {
        input: dummy_arg = 4 + i,
               fruits= fruits
    }

    ### Memory tests
    call MemorySpec { input: memory_req_gb=12 }
    call MemorySpec as memorySpec2 { input: memory_req_gb=1 }

    ### Number of cores
    call NumCoresSpec { input: num_cores_req=5 }
    call NumCoresSpec as numCoresSpec2 { input: num_cores_req=1 }

    ### Choosing a docker image at runtime
    call RuntimeDockerChoice { input: imageName="python:2.7" }
    call RuntimeDockerChoice2 { input: imageName="python:2.7" }

    call Shortcut

    output {
        String MemorySpec_retval = MemorySpec.retval
        String DiskSpaceSpec_retval = DiskSpaceSpec.retval
        String NumCoresSpec_retval = NumCoresSpec.retval
        String RuntimeDockerChoice_retval = RuntimeDockerChoice.retval
        String RuntimeDockerChoice2_retval = RuntimeDockerChoice2.retval
        String Shortcut_retval = Shortcut.retval
        String DiskSpaceSpec2_retval = diskSpec2.retval
    }
}
