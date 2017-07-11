# Get version of BWA
task GetBwaVersion {
  command {
    /usr/gitc/bwa 2>&1 | \
    grep -e '^Version' | \
    sed 's/Version: //'
  }
  runtime {
      docker: "broadinstitute/genomes-in-the-cloud:2.2.4-1469632282"
      entrypoint: "/bin/bash"
      memory: "3 GB"
      cpu: "1"
      disks: "local-disk 10 HDD"
  }
  output {
    String version = read_string(stdout())
  }
}

workflow bwa_version {
    call GetBwaVersion
    output {
        GetBwaVersion.version
    }
}
