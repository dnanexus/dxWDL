# This example calls the analysis task once for each element in the array that the prepare task outputs.
#
# Copied from the Broad Institute tutorial
#     https://github.com/broadinstitute/wdl

task mm_prepare {
    command <<<
       python -c "print('one\ntwo\nthree\nfour')"
    >>>
    output {
        Array[String] array = read_lines(stdout())
    }
}

task mm_analysis {
    String str
    command <<<
       echo "xyz12345" >> ${str}.txt
    >>>
    output {
        File out = "${str}.txt"
    }
}

task mm_gather {
    Array[File] files
    command <<<
        wc ${sep=' ' files}
    >>>
    output {
        String str = read_string(stdout())
    }
}

task mm_file_ident {
    File fileA
    File fileB

    command {
    }
    output {
      String result = fileA
    }
}

workflow sg_files {
    String wf_suffix = ".txt"

    call mm_prepare as prepare
    scatter (x in prepare.array) {
        call mm_analysis as analysis {input: str=x}
    }
    call mm_gather as gather {input: files=analysis.out}

    scatter (filename in analysis.out) {
        String prefix = ".txt"
        String prefix2 = ".cpp"
        String suffix = wf_suffix

        call mm_file_ident as ident {
          input:
             fileA = sub(filename, prefix, "") + suffix,
             fileB = sub(sub(filename, prefix, ""), prefix2, "") + suffix
        }
    }

}
