task deplete {
  String sample_name
  File reads_unmapped_bam
  Array[File] bmtagger_dbs
  Array[File] blastn_dbs
  Boolean? skip

  command <<<
    set -ex -o pipefail

    if [ "${skip}" == "true" ]; then
      cp "${reads_unmapped_bam}" "${sample_name}.cleaned.bam"
      exit
    fi

    # stage the databases for BMTagger and BLAST
    # assumptions: each database is stored in a tarball. If the database name
    # is X then the tarball is named X.bmtagger_db.tar.gz or X.blastndb.tar.gz.
    # The tarball contains the database files in the root (NOT in subdirectory
    # X/). The individual database files have X as their base name, e.g.
    # X.srprism.amp, X.nin
    stage_db() {
        local dbname=$(basename "$2" .$1.tar.gz)
        mkdir -p "/user-data/$1/$dbname"
        cat "$2" | gzip -dc | tar xv -C "/user-data/$1/$dbname"
        rm "$2"
    }
    export -f stage_db
    cat ${write_lines(bmtagger_dbs)} | xargs -i -n 1 -t bash -ex -o pipefail -c "stage_db bmtagger_db {}"
    cat ${write_lines(blastn_dbs)} | xargs -i -n 1 -t bash -ex -o pipefail -c "stage_db blastndb {}"

    # find 90% memory, for java
    jvm_mem_MiB=`head -n1 /proc/meminfo | awk '{print int($2*0.9/1024)}'`

    # run deplete_human
    taxon_filter.py deplete_human \
        --JVMmemory "$jvm_mem_MiB"m --threads `nproc` \
        "${reads_unmapped_bam}" "${sample_name}.raw.bam" "${sample_name}.bmtagger_depleted.bam" \
        "${sample_name}.rmdup.bam" "${sample_name}.cleaned.bam" \
        --bmtaggerDbs $(ls -1 /user-data/bmtagger_db | xargs -i -n 1 printf " /user-data/bmtagger_db/%s/%s " {} {}) \
        --blastDbs $(ls -1 /user-data/blastndb | xargs -i -n 1 printf " /user-data/blastndb/%s/%s " {} {})
  >>>

  runtime {
      docker: "broadinstitute/viral-ngs:1.15.3"
      memory: "30000 MB"
      disks: "local-disk 100 HDD"
  }

  output {
      File cleaned_reads_unmapped_bam = "${sample_name}.cleaned.bam"
  }
}

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
    docker: "broadinstitute/viral-ngs:1.15.3"
  }

  output {
    File filtered_reads_unmapped_bam = "${sample_name}.filtered.bam"
  }
}

task trinity {
    String sample_name
    File reads_unmapped_bam
    File contaminants_fasta
    Int? subsample

    command <<<
      assembly.py assemble_trinity ${reads_unmapped_bam} ${contaminants_fasta} \
        ${sample_name}.trinity.fasta --n_reads=${default=100000 subsample} --outReads ${sample_name}.subsampled.bam
    >>>

    runtime {
      docker: "broadinstitute/viral-ngs:1.15.3"
    }

    output {
      File contigs_fasta = "${sample_name}.trinity.fasta"
      File subsampled_reads_unmapped_bam = "${sample_name}.subsampled.bam"
    }
}

task scaffold {
    String sample_name
    File trinity_contigs_fasta
    File trinity_reads_unmapped_bam
    File reference_genome_fasta

    File? novocraft_license

    String? aligner
    Float? min_length_fraction
    Float? min_unambig
    Int? replace_length

    command <<<
      set -ex -o pipefail
      assembly.py order_and_orient ${trinity_contigs_fasta} ${reference_genome_fasta} ${sample_name}.intermediate_scaffold.fasta
      if [ -n "${novocraft_license}" ]; then
        cp ${novocraft_license} /tmp/novocraft.lic
      fi
      assembly.py impute_from_reference \
        ${sample_name}.intermediate_scaffold.fasta ${reference_genome_fasta} ${sample_name}.scaffold.fasta \
        --NOVOALIGN_LICENSE_PATH /tmp/novocraft.lic \
        --newName "${sample_name}" --replaceLength ${default=55 replace_length} \
        --minLengthFraction ${default="0.5" min_length_fraction} --minUnambig ${default="0.5" min_unambig} \
        --aligner ${default=muscle aligner}
    >>>

    runtime {
      docker: "broadinstitute/viral-ngs:1.15.3"
    }

    output {
      File scaffold_fasta = "${sample_name}.scaffold.fasta"
      File intermediate_scaffold_fasta = "${sample_name}.intermediate_scaffold.fasta"
    }
}

task refine {
    String sample_name
    File assembly_fasta
    File reads_unmapped_bam

    File gatk_tar_bz2
    File? novocraft_license

    String? novoalign_options
    Float? major_cutoff
    Float? min_coverage

    command <<<
      set -ex -o pipefail
      if [ -n "${novocraft_license}" ]; then
       cp ${novocraft_license} /tmp/novocraft.lic
      fi
      mkdir gatk/
      tar jxf ${gatk_tar_bz2} -C gatk/
      mv ${assembly_fasta} assembly.fasta
      novoindex assembly.nix assembly.fasta
      assembly.py refine_assembly assembly.fasta ${reads_unmapped_bam} ${sample_name}.refined_assembly.fasta \
        --outVcf ${sample_name}.sites.vcf.gz --min_coverage ${default=1 min_coverage} --major_cutoff ${default="0.5" major_cutoff} \
        --threads $(nproc) --GATK_PATH gatk/ \
        --novo_params '${default="-r Random -l 40 -g 40 -x 20 -t 100" novoalign_options}' --NOVOALIGN_LICENSE_PATH /tmp/novocraft.lic
    >>>

    runtime {
      docker: "broadinstitute/viral-ngs:1.15.3"
    }

    output {
      File refined_assembly_fasta = "${sample_name}.refined_assembly.fasta"
      File sites_vcf_gz = "${sample_name}.sites.vcf.gz"
    }
}

workflow viral_ngs_assembly {
  String sample_name
  File reads_unmapped_bam

  Array[File] bmtagger_dbs
  Array[File] blastn_dbs
  File targets_fasta

  File contaminants_fasta

  File reference_genome_fasta
  File gatk_tar_bz2

  call deplete {
    input:
      sample_name = sample_name,
      reads_unmapped_bam = reads_unmapped_bam,
      bmtagger_dbs = bmtagger_dbs,
      blastn_dbs = blastn_dbs_x
  }

  call filter {
    input:
      sample_name = sample_name,
      reads_unmapped_bam = deplete.cleaned_reads_unmapped_bam,
      targets_fasta = targets_fasta
  }

  call trinity {
    input:
      sample_name = sample_name,
      reads_unmapped_bam = filter.filtered_reads_unmapped_bam,
      contaminants_fasta = contaminants_fasta
      # TODO: optional pass-through of subsample
  }

  call scaffold {
    input:
      sample_name = sample_name,
      trinity_contigs_fasta = trinity.contigs_fasta,
      trinity_reads_unmapped_bam = trinity.subsampled_reads_unmapped_bam,
      reference_genome_fasta = reference_genome_fasta
      # TODO: pass through optional args
  }

  call refine as refine1 {
    input:
      sample_name = sample_name,
      assembly_fasta = scaffold.scaffold_fasta,
      reads_unmapped_bam = deplete.cleaned_reads_unmapped_bam,
      gatk_tar_bz2 = gatk_tar_bz2,
      novoalign_options = "-r Random -l 30 -g 40 -x 20 -t 502",
      min_coverage = 2
  }

  call refine as refine2 {
    input:
      sample_name = sample_name,
      assembly_fasta = refine1.refined_assembly_fasta,
      reads_unmapped_bam = deplete.cleaned_reads_unmapped_bam,
      gatk_tar_bz2 = gatk_tar_bz2,
      novoalign_options = "-r Random -l 40 -g 40 -x 20 -t 100",
      min_coverage = 3
  }
}
