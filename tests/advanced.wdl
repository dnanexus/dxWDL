task jjj_str_animals {
    String s
    Int num_cores
    Int? num
    String? foo
    String family_i = "Family ${s}"

    command {
        echo "${s} --K -S --flags --contamination ${default=0 num} --s ${default="foobar" foo}"
    }
    runtime {
        cpu: num_cores
    }
    output {
        String result = read_string(stdout())
        String family = family_i
    }
}

task jjj_ident {
    String s

    command {
    }
    output {
      String dummy_result = ""
    }
}

task jjj_cgrep {
    File in_file
    String pattern
    Int num
    String buf_i
    String ignoredVar

    command {
        grep '${pattern}' ${in_file} | wc -l
    }
    output {
        Int count = read_int(stdout())
        String buf = buf_i
    }
}

workflow advanced {
    String pattern
    String species
    File file
    String p_nowhere="wdl ${pattern}"
    String p_tut = "tut"
    Array[String] patterns = [pattern, p_nowhere, p_tut]
    Int? i
    File? empty
    String unmapped_bam_suffix = "bam"
    Array[String] names = ["Jack.XX", "Gil.XX", "Jane.UU"]

    scatter (name in names) {
        call jjj_ident {
          input:
             s = sub(name, ".XX", "") + " XY"
        }
    }

    call jjj_str_animals as str_animals {
        input: s=species, num_cores=3
    }
    scatter (pt in patterns) {
        String s = "Salamander"
        String sub_strip_unmapped = unmapped_bam_suffix + "$"
        Int k = 5

        call jjj_cgrep as cgrep {
           input: in_file = file,
             pattern = pt,
             num=k,
             buf_i=sub_strip_unmapped,
             ignoredVar= sub(pt, ".edu", "")
        }
    }
    output {
        cgrep.count
        str_animals.result
        str_animals.family
    }
}
