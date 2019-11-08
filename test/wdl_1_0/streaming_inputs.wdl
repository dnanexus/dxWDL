version 1.0

workflow streaming_inputs {
    input {
         File f1
         File f2
    }

    call test_streaming {
        input:
            f1=f1,
            f2=f2
    }

    output {
        String r1 = test_streaming.r1
        String r2 = test_streaming.r2
    }
}

task test_streaming {
    input {
        File f1
        File f2
    }

    command <<<
    zcat ~{f1} > r1
    zcat ~{f2} > r2
    >>>

    output {
        File r1 = "r1"
        File r2 = "r2"
    }

    runtime {
        docker: "debian:stretch-slim"
    }

    parameter_meta {
        f1: "stream"
        f2: {
            stream: true
        }
    }
}