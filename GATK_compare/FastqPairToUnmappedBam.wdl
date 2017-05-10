# Convert a pair of FASTQ files to unmapped bam with Picard
task FastqPairToUnmappedBam {
  File F1
  File F2
  String sample_name

  command {
    java -Xmx2g -jar /usr/gitc/picard.jar \
      FastqToSam \
         F1=${F1} \
         F2=${F2} \
         O=${sample_name}.bam \
         SAMPLE_NAME=${sample_name}
  }
  runtime {
    docker: "broadinstitute/genomes-in-the-cloud:2.2.5-1486412288"
    memory: "4 GB"
    disks: "local-disk 100 HDD"
  }
  output {
    File unmapped_bam = "${sample_name}.unmapped.bam"
  }
}
