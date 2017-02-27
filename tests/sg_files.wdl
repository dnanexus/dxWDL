# This example calls the analysis task once for each element in the array that the prepare task outputs.
#
# Copied from the Broad Institute tutorial
#     https://github.com/broadinstitute/wdl

task prepare {
    command <<<
       python -c "print('one\ntwo\nthree\nfour')"
    >>>
    output {
        Array[String] array = read_lines(stdout())
    }
}

task analysis {
    String str
    command <<<
       echo "xyz12345" >> ${str}.txt
    >>>
    output {
        File out = "${str}.txt"
    }
}

task gather {
    Array[File] files
    command <<<
        wc ${sep=' ' files}
    >>>
    output {
        String str = read_string(stdout())
    }
}

workflow sg_files {
    call prepare
    scatter (x in prepare.array) {
        call analysis {input: str=x}
    }
    call gather {input: files=analysis.out}
}
