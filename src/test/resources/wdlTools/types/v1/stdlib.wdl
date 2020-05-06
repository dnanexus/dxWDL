version 1.0

task foo {
  input {
    File workspace_tar
    File ref_fasta
    File dbsnp_vcf
  }
  Int disk_size = ceil(size(workspace_tar, "GiB") + size(ref_fasta, "GiB") + size(dbsnp_vcf, "GiB") * 3)

  command {}
  output {
    Int disk_size = disk_size
  }
}
