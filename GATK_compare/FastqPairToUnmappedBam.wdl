# Convert a pair of FASTQ files to unmapped bam with Picard
task FastqPairToUnmappedBam {
  File F1
  File F2
  String sample_name
  String read_group
  String library_id
  String platform_unit

  command <<<
    java -Xmx2g -jar /usr/gitc/picard.jar FastqToSam \
      F1=${F1} \
      F2=${F2} \
      O=${sample_name}.X.bam \
      SAMPLE_NAME=${sample_name}
    rm -f ${F1} ${F2}
    java -jar /usr/gitc/picard.jar AddOrReplaceReadGroups \
      INPUT=${sample_name}.X.bam \
      OUTPUT=${sample_name}.bam \
      RGID=${read_group} \
      RGPL="Illumina" \
      RGLB=${library_id} \
      RGPU=${platform_unit} \
      RGSM=${sample_name}
  >>>
  runtime {
    docker: "broadinstitute/genomes-in-the-cloud:2.2.5-1486412288"
    memory: "4 GB"
    disks: "local-disk 500 HDD"
  }
  output {
    File unmapped_bam = "${sample_name}.bam"
  }
}
