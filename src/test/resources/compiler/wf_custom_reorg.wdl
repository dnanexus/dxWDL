version 1.0

workflow test_reorg {

    call stage_one

    output {
        Array[File] output_files = stage_one.output_files
        File output_config_file = stage_one.output_config_file
    }

}


task stage_one {

    command {
        touch output_file && echo "This is the output file" >> output_file
        touch output_file_two && echo "This is the 2nd output file" >> output_file_two
        touch output_config_file && echo "This is the output config file" >> output_config_file
    }

    output {
        Array[File] output_files = ["/home/dnanexus/output_file", "/home/dnanexus/output_file_two" ]
        File output_config_file = "/home/dnanexus/output_config_file"
    }
}