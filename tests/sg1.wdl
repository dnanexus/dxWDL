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
       python -c "print('_${str}_')"
    >>>
    output {
        String out = read_string(stdout())
    }
}

task gather {
    Array[String] array
    command <<<
        echo ${sep=' ' array}
    >>>
    output {
        String str = read_string(stdout())
    }
}

workflow sg1 {
    call prepare
    scatter (x in prepare.array) {
        call analysis {input: str=x}
    }
    call gather {input: array=analysis.out}
}
