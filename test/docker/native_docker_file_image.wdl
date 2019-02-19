# Save a docker image to a platform file.
    # Run this image on a worker.

task native_docker_file_image {
    command {
        lsb_release --codename | cut -f 2
    }
    runtime {
        docker : "dx://dxWDL_playground:/test_data/ubuntu_18_04_minimal.tar"
    }
    output {
        String result = read_string(stdout())
    }
}
