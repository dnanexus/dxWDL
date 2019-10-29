version 1.0

workflow test_reorg {

    call stage_one

    output {
        File output_file = stage_one.output_file
        File output_config_file = stage_one.output_config_file
    }

}

task stage_one {

    command {
        touch output_file && echo "This is the output file" >> output_file
        touch output_config_file && echo "This is the output config file" >> output_config_file
    }

    output {
        File output_file = "/home/dnanexus/output_file"
        File output_config_file = "/home/dnanexus/output_config_file"
    }
}