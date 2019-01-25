# Test that it is possible to use a private docker registry.
#
task private_registry {
    command <<<
        echo "Major Major"
    >>>
    runtime {
        docker : "docker.io/orodeh/dxwdl_test:1.0"
    }
    output {
        String result = read_string(stdout())
    }
}
