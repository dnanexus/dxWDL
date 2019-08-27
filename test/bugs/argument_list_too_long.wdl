# wget https://github.com/dnanexus/dxWDL/releases/download/1.12/dxWDL-1.12.jar
# dx select project-FbBGFVj0P596KqVZ91p6fqzF
# java -jar dxWDL-1.12.jar compile toy.wdl -project project-FbBGFVj0P596KqVZ91p6fqzF -f
# dx run toy -y --name 25x239
# dx run toy -y --name 25x148 -i common.N=148
version 1.0

workflow argument_list_too_long {
    input {
        Int M = 25
        Int N = 239
    }

    # create MxN file matrix
    scatter (m in range(M)) {
        call create_files {
            input:
                m = m,
                N = N
        }
    }

    # emulate the crashing scatter/transpose from GLnexus-internal workflow
    scatter (row in transpose(create_files.files)) {
        call nop {
            input:
                files = row
        }
    }

    output{
        Array[Int] Ms = nop.count
    }
}

task create_files {
    input {
        Int m
        Int N
    }
    command <<<
        for i in $(seq ~{N}); do
            echo "~{m} $i" > "~{m}_${i}.txt"
        done
    >>>
    output {
        Array[File] files = glob("*.txt")
    }
}

task nop {
    input {
        Array[File] files
    }
    command <<<
        echo "Hello, world!"
    >>>
    output {
        Int count = length(files)
    }
}
