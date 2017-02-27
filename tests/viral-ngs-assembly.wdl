task filter {
  String sample_name
  File reads_unmapped_bam
  File targets_fasta

  command <<<
    set -ex -o pipefail
    taxon_filter.py lastal_build_db ${targets_fasta} /tmp --outputFilePrefix targets.db
    sha256sum /tmp/targets.db*
    taxon_filter.py filter_lastal_bam ${reads_unmapped_bam} /tmp/targets.db ${sample_name}.filtered.bam
  >>>
  runtime {
    docker: "broadinstitute/viral-ngs:1.13.3"
  }
  output {
    File filtered_reads = "${sample_name}.filtered.bam"
  }
}

workflow viral_ngs_assembly {
    File reads_unmapped_bam
    File targets_fasta
    String sample_name

    call filter {
        input:
                sample_name = sample_name,
        reads_unmapped_bam = reads_unmapped_bam,
        targets_fasta = targets_fasta
    }
}
