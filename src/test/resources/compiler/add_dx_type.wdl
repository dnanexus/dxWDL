version 1.0

task add_dx_type {
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
                and: [
                    "fastq", 
                    {
                        or: ["Read1", "Read2"]
                    }
                ]
            }
        }
    }

    output {
        String result = stdout()
    }
}
