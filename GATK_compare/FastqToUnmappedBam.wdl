# Convert a FASTQ file to unmapped bam with Picard
task FastqToUnmappedBam {
  File F1
  String sample_name

  command {
    java -Xmx2g -jar /usr/gitc/picard.jar \
      FastqToSam \
         F1=${F1} \
         O=${sample_name}.bam \
         SAMPLE_NAME=${sample_name}
  }
  runtime {
    docker: "broadinstitute/genomes-in-the-cloud:2.2.5-1486412288"
    memory: "4 GB"
    disks: "local-disk 100 HDD"
  }
  output {
    File unmapped_bam = "${sample_name}.bam"
  }
}
