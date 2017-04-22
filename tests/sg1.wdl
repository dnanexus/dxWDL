# This example calls the analysis task once for each element in the array that the prepare task outputs.
#
# Copied from the Broad Institute tutorial
#     https://github.com/broadinstitute/wdl

task vv_prepare {
    command <<<
       python -c "print('one\ntwo\nthree\nfour')"
    >>>
    output {
        Array[String] array = read_lines(stdout())
    }
}

task vv_analysis {
    String str
    command <<<
       python -c "print('_${str}_')"
    >>>
    output {
        String out = read_string(stdout())
    }
}

task vv_gather {
    Array[String] array
    command <<<
        echo ${sep=' ' array}
    >>>
    output {
        String str = read_string(stdout())
    }
}

workflow sg1 {
    call vv_prepare as prepare
    scatter (x in prepare.array) {
        call vv_analysis as analysis {input: str=x}
    }
    call vv_gather as gather {input: array=analysis.out}
}
