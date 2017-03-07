task wc {
    Array[File] files

    command {
        wc ${sep=' ' files}
    }
    output {
        String result = read_string(stdout())
        Array[File] result_files = files
    }
}

task diff {
    File A
    File B

    command {
        diff ${A} ${B} | wc -l
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
task colocation {
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

workflow file_array {
    Array[File] fs

    call wc {
        input : files=fs
    }
    call diff {
        input : A=wc.result_files[0],
                B=wc.result_files[1]
    }
    call colocation {
        input : A=fs[0], B=fs[1]
    }
    output {
        wc.result
        diff.result
        colocation.result
    }
}
