version 1.0

# Check that native docker works
#
task broad_genomics {
  command {
    /usr/gitc/bwa 2>&1 | \
    grep -e '^Version' | \
    sed 's/Version: //'
  }
  runtime {
      docker: "broadinstitute/genomes-in-the-cloud:2.3.1-1512499786"
      memory: "3 GB"
      cpu: "1"
      disks: "local-disk 10 HDD"
  }
  output {
    String bwa_version = read_string(stdout())
  }
}
