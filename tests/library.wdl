# A library of individual tasks. The plan is to import this file into
#the workflows. This will avoid creating duplicate applets, and reduces compilation
#time.

task xxxx_Add {
    Int a
    Int b

    command {
        echo $((a + b))
    }
    output {
        Int result = a + b
    }
}

task xxxx_Multiply {
    Int a
    Int b

    command {
        echo $((a * b))
    }
    output {
        Int result = a * b
    }
}

task xxxx_cgrep {
    File in_file
    String pattern

    command {
        grep '${pattern}' ${in_file} | wc -l
    }
    output {
        Int count = read_int(stdout())
    }
}

task xxxx_wc {
    File in_file
    command {
        cat ${in_file} | wc -l
    }
    output {
        Int count = read_int(stdout())
    }
}

# A very simple WDL file, that includes a single task
task xxxx_join_many {
    Boolean b
    Int i
    Float x
    String s

    command {
    }
    output {
        String result = "${b}_${i}_${x}_${s}"
    }
}
