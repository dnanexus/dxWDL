version 1.0

task add_group {
    input {
        File a
        File b
    }
    
    command {
    cat ~{a} ~{b}
    }

    parameter_meta {
        a: {
            dx_type: "fastq"
        }
        b: {
            dx_type: {
                and_: [
                    "fastq", 
                    {
                        or_: ["Read1", "Read2"]
                    }
                ]
            }
        }
    }

    output {
        String result = stdout()
    }
}
