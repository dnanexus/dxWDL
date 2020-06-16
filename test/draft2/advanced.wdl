import "library_sys_call.wdl" as lib
import "library_string.wdl" as lib_string

# Get version of BWA
task BroadGenomicsDocker {
  command {
    /usr/gitc/bwa 2>&1 | \
    grep -e '^Version' | \
    sed 's/Version: //'
  }
  runtime {
      docker: "broadinstitute/genomes-in-the-cloud:2.2.4-1469632282"
      memory: "3 GB"
      cpu: "1"
      disks: "local-disk 10 HDD"
  }
  output {
    String ver = read_string(stdout())
  }
}


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
    String p_test = "test"
    Array[String] patterns = [pattern, p_nowhere, p_test]
    Int? i
    File? empty
    String unmapped_bam_suffix = "bam"
    Array[String] names = ["Jack.XX", "Gil.XX", "Jane.UU"]

    call BroadGenomicsDocker
    call Animals as str_animals {
        input: s= species + "_family", num_cores=3, disk_size=40
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
        Array[Int] cgrep_count = cgrep.count
        String str_animals_result = str_animals.result
        String str_animals_family = str_animals.family
        String BroadGenomicsDocker_version = BroadGenomicsDocker.ver
    }
}
