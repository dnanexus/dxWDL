version 1.0

# Accessing undefined variables
task foo {
  command {}
  runtime {
    docker : ["ubuntu:latest", "broadinstitute/scala-baseimage"]
    disks : "local-disk " + disk_req_gb + " HDD"
    cores :  num_cpu
  }
}
