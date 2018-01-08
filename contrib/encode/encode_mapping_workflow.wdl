## This WDL maps FASTQ file(s) using ENCODE ChIP-seq mapping pipeline
##
## Requirements/expectations :
## - trimming length parameter (either "native" or length in string format)
## - fastq (either one SE or two PE fastq file(s))
## - reference (tar of reference files for bwa)
##
## example inputs.json would look like:
## {
##      "encode_mapping_workflow.fastqs": ["input_data/rep1-ENCFF000VOL-chr21.fq.gz"],
##      "encode_mapping_workflow.trimming_parameter": "native",
##      "encode_mapping_workflow.reference": "input_data/GRCh38_chr21_bwa.tar.gz"
## }



# TASK DEFINITIONS

task mapping {
    Array[File] fastq_files
    File reference_file
    String trimming_length

    command {

        python /image_software/pipeline-container/src/encode_map.py \
         ${reference_file} \
         ${trimming_length} \
         ${sep=' ' fastq_files}
    }

    output {
        Array[File] sai_files = glob('*.sai')
        Array[File] unmapped_files = glob('*.gz')
        File mapping_log = glob('mapping.log')[0]
        File mapping_results = glob('mapping.json')[0]
    }

    runtime {
        docker: 'quay.io/encode-dcc/mapping:v1.0'
        cpu: '2'
        memory: '17.1 GB'
        disks: 'local-disk 420 HDD'
    }
}


task post_processing {
    Array[File] initial_fastqs
    File reference_file
    Array[File] sai_files
    String trimming_length
    Array[File] unmapped_fastqs

    command {

        python /image_software/pipeline-container/src/encode_post_map.py \
         ${trimming_length} \
         ${reference_file} \
         ${sep=' ' unmapped_fastqs} \
         ${sep=' ' sai_files} \
         ${sep=' ' initial_fastqs}
    }

    output {
        File unfiltered_bam = glob('*.raw.srt.bam')[0]
        File unfiltered_flagstats = glob('*.raw.srt.bam.flagstat.qc')[0]
        File post_mapping_log = glob('post_mapping.log')[0]
        File post_mapping_results = glob('post_mapping.json')[0]
    }

    runtime {
        docker: 'quay.io/encode-dcc/post_mapping:v1.0'
        cpu: '2'
        memory: '17.1 GB'
        disks: 'local-disk 420 HDD'
    }
}


task filter_qc {
    File bam_file
    Array[File] fastq_files

    command {

        python /image_software/pipeline-container/src/filter_qc.py \
         ${bam_file} \
         ${sep=' ' fastq_files}
    }

    output {
        File dup_file_qc = glob('*.dup.qc')[0]
        File filtered_bam = glob('*final.bam')[0]
        File filtered_bam_bai = glob('*final.bam.bai')[0]
        File filtered_map_stats = glob('*final.flagstat.qc')[0]
        File pbc_file_qc = glob('*.pbc.qc')[0]
        File filter_qc_log = glob('filter_qc.log')[0]
        File filter_qc_results = glob('filter_qc.json')[0]
    }

    runtime {
        docker: 'quay.io/encode-dcc/filter:v1.0'
        cpu: '2'
        memory: '17.1 GB'
        disks: 'local-disk 420 HDD'
    }
}


task xcor {
    File bam_file
    Array[File] fastq_files

    command {

        python /image_software/pipeline-container/src/xcor.py \
         ${bam_file} \
         ${sep=' ' fastq_files}
    }

    output {
        File cc_file = glob('*.cc.qc')[0]
        File cc_plot = glob('*.cc.plot.pdf')[0]
        Array[File] tag_align = glob('*tagAlign.gz')
        File xcor_log = glob('xcor.log')[0]
        File xcor_results = glob('xcor.json')[0]
    }

    runtime {
        docker: 'quay.io/encode-dcc/xcor:v1.0'
        cpu: '2'
        memory: '17.1 GB'
        disks: 'local-disk 420 HDD'
    }
}


task gather_the_outputs {

    File unfiltered_bam
    File filtered_bam
    File unfiltered_flagstat
    File filtered_flagstat
    File dup_qc
    File pbc_qc
    File mapping_log
    File post_mapping_log
    File filter_qc_log
    File xcor_log
    File mapping_results
    File post_mapping_results
    File filter_qc_results
    File xcor_results
    File cc
    File cc_pdf
    Array[File] tag_align


    command {
        cp ${unfiltered_bam} .
        cp ${filtered_bam} .
        cp ${unfiltered_flagstat} .
        cp ${filtered_flagstat} .
        cp ${dup_qc} .
        cp ${pbc_qc} .
        cp ${mapping_log} .
        cp ${post_mapping_log} .
        cp ${filter_qc_log} .
        cp ${xcor_log} .
        cp ${mapping_results} .
        cp ${post_mapping_results} .
        cp ${filter_qc_results} .
        cp ${xcor_results} .
        cp ${cc} .
        cp ${cc_pdf} .
        cp ${sep=' ' tag_align} .
    }
}

# WORKFLOW DEFINITION

workflow encode_mapping_workflow {
    String trimming_parameter
    Array[File] fastqs
    File reference


    call mapping  {
        input: fastq_files=fastqs,
          reference_file=reference,
          trimming_length=trimming_parameter
    }

    call post_processing  {
        input: initial_fastqs=fastqs,
          reference_file=reference,
          sai_files=mapping.sai_files,
          trimming_length=trimming_parameter,
          unmapped_fastqs=mapping.unmapped_files
    }

    call filter_qc  {
        input: bam_file=post_processing.unfiltered_bam,
          fastq_files=fastqs
    }


    call xcor  {
        input: bam_file=filter_qc.filtered_bam,
          fastq_files=fastqs
    }

    call gather_the_outputs {
        input: cc = xcor.cc_file,
          cc_pdf = xcor.cc_plot,
          dup_qc = filter_qc.dup_file_qc,
          filter_qc_log = filter_qc.filter_qc_log,
          filter_qc_results = filter_qc.filter_qc_results,
          filtered_bam = filter_qc.filtered_bam,
          filtered_flagstat = filter_qc.filtered_map_stats,
          mapping_log = mapping.mapping_log,
          mapping_results = mapping.mapping_results,
          pbc_qc = filter_qc.pbc_file_qc,
          post_mapping_log = post_processing.post_mapping_log,
          post_mapping_results = post_processing.post_mapping_results,
          tag_align = xcor.tag_align,
          unfiltered_bam = post_processing.unfiltered_bam,
          unfiltered_flagstat = post_processing.unfiltered_flagstats,
          xcor_log = xcor.xcor_log,
          xcor_results = xcor.xcor_results
    }

}


