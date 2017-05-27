# This example calls the analysis task once for each element in the array that the prepare task outputs.
#
# Copied from the Broad Institute tutorial
#     https://github.com/broadinstitute/wdl
import "library_sg.wdl" as lib

task vv_analysis {
    String str
    command <<<
       python -c "print('_${str}_')"
    >>>
    output {
        String out = read_string(stdout())
    }
}

workflow sg1 {
    call lib.Prepare as prepare
    scatter (x in prepare.array) {
        call vv_analysis as analysis {input: str=x}
    }
    call lib.Gather as gather {input: array=analysis.out}
}
