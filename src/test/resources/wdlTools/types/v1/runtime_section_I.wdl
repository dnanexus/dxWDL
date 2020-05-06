version 1.0

task foo {
  input {
    Int disk_req_gb
  }
  Int num_cpu = 10

  command {}
  runtime {
    docker : ["ubuntu:latest", "broadinstitute/scala-baseimage"]
    disks : "local-disk " + disk_req_gb + " HDD"
    cores :  num_cpu
  }
}
