import "library_sys_call.wdl" as lib
import "library_string.wdl" as lib_string

task Animals {
    String s
    Int num_cores
    Int disk_size
    Int? num
    String? foo
    String family_i = "Family ${s}"

    command {
        echo "${s} --K -S --flags --contamination ${default=0 num} --s ${default="foobar" foo}"
    }
    runtime {
       disks: "local-disk " + disk_size + " HDD"
       cpu: num_cores
       memory: "2 GB"
    }
    output {
        String result = read_string(stdout())
        String family = family_i
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

    call Animals as str_animals {
        input: s=species, num_cores=3, disk_size=40
    }
    scatter (name in names) {
        call lib_string.Concat as concat {
          input:
             x = sub(name, ".XX", "") + ".XY",
             y = sub(sub(name, ".XX", ""), ".UU", "") + ".unmerged"
        }
    }
    scatter (pt in patterns) {
        String s = "Salamander"
        String sub_strip_unmapped = unmapped_bam_suffix + "$"
        Int k = 5

        call lib.cgrep as cgrep {
           input: in_file = file, pattern = pt
        }
    }
    output {
        cgrep.count
        str_animals.result
        str_animals.family
    }
}
