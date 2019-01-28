# Check that native docker works
#
task broad_genomics {
  command {
    /usr/gitc/bwa 2>&1 | \
    grep -e '^Version' | \
    sed 's/Version: //'
  }
  runtime {
      docker: "broadinstitute/genomes-in-the-cloud:2.2.4-1469632282"
      memory: "3 GB"
      cpu: "1"
      disks: "local-disk 10 HDD"
  }
  output {
    String version = read_string(stdout())
  }
}
