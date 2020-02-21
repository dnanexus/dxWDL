version 1.0

# Should fail because dx_type is not allowed for non-file parameters

task add_dx_type {
    input {
        String a
    }
    
    command {
    echo ~{a}
    }

    parameter_meta {
        a: {
            dx_type: "fastq"
        }
    }

    output {
        String result = stdout()
    }
}
