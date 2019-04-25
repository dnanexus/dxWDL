version 1.0

# level2.wdl

workflow call_level2 {
    input {
        String run_name
    }

    call call_cnvs_lane  {
        input:
            nothing=run_name,
    }

    output {
        File cnv_qual_metrics_json = call_cnvs_lane.cnv_qual_metrics_json
    }
}

task call_cnvs_lane {
    input {
        String nothing
        Int nprocs = 48
    }
    command <<<
        touch call_hmm_cnvs.metrics.lane.tar.gz
    >>>
    output {
      File cnv_qual_metrics_json = "call_hmm_cnvs.metrics.lane.tar.gz"
    }
}
