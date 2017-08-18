task ps {
    command {
        ps aux
    }
    output {
        File procs = stdout()
    }
}

task cgrep {
    String pattern
    File in_file

    parameter_meta {
        in_file : "stream"
    }
    command {
        grep '${pattern}' ${in_file} | wc -l
    }
    output {
        Int count = read_int(stdout())
    }
}

task wc {
    File in_file

    parameter_meta {
        in_file : "stream"
    }
    command {
        cat ${in_file} | wc -l
    }
    output {
        Int count = read_int(stdout())
    }
}

task head {
    File in_file
    Int num_lines

    parameter_meta {
        in_file : "stream"
    }
    command {
        head -n ${num_lines} ${in_file}
    }
    output {
        String result = read_string(stdout())
    }
}

task diff {
    File a
    File b

    parameter_meta {
        a : "stream"
        b : "stream"
    }
    command {
        diff ${a} ${b} | wc -l
    }
    output {
        Int result = read_int(stdout())
    }
}

# Make sure that two input files are
# in the same directory.
#
# Many bioinformatics tools assume that bam files and
# their indexes reside in the same directory. For example:
#     xxx/yyy/{A.bam, A.bai}
#     xxx/yyy/{A.fasta, A.fasta.fai}
#     xxx/yyy/{A.vcf, A.vcf.idx}
#
task Colocation {
    File A
    File B

    command <<<
python <<CODE
import os
dir_path_A = os.path.dirname("${A}")
dir_path_B = os.path.dirname("${B}")
print((dir_path_A == dir_path_B))
CODE
    >>>
    output {
        String result = read_string(stdout())
    }
}
