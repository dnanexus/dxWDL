# Test that it is possible to save a docker image as
# a platform image.
#
# Note: this assumes dx-docker.
task platform_asset {
    command <<<
        echo "Major Major"
    >>>
    runtime {
        docker: "dx://dxWDL_playground:/test_data/ubuntu"
    }
    output {
        String result = read_string(stdout())
    }
}
