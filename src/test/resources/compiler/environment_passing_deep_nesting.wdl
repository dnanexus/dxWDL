version 1.0

workflow environment_passing_deep_nesting {
    input {
        File dummy_file
    }

    call unify_sites as unify

    call vcf_shard_merge_pyspark as vcf_shard_merge

    if (false) {
        call cmp_seq_sharded as compare { input:
            unify_sites_sharded = unify.unified_sites,
            VCF_sharded_merged = vcf_shard_merge.pvcf_gz
        }

        call assert
    }
}


task unify_sites {
    command {}
    output {
        Array[File] unified_sites = glob("unified_sites/*.gz")
    }
}

task assert {
    command {
    }
}


task cmp_seq_sharded {
    input {
        Array[File] unify_sites_sharded
        Array[File] VCF_sharded_merged
    }
  command {}
  output {
    Boolean equality = true
    File? unified_sites_diff = "dummy.txt"
    Array[File] VCF_diff = []
  }
}

task vcf_shard_merge_pyspark {
  command {}
  output {
    Array[File] pvcf_gz = []
    Array[File] tbi = []
  }
}
