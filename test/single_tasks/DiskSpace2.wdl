version 1.0

task DiskSpace2 {
    input {
        File fruits
    }
    # use provided disk number or dynamically size on our own,
    Int disk_req_gb = ceil(size(fruits, "GB")) + 50

  command <<<
    lines=$(df -t btrfs | grep dev)
    size_kb=$(echo $lines | cut -d ' ' -f 2)
    let "size_gb= $size_kb / (1024 * 1024)"
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
