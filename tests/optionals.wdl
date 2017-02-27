task add {
    String i

    command {
        echo "$(($i + $i))"
    }
    output {
        Int result = read_int(stdout())
    }
}

task add_opt {
    Int? i

    command {
        echo "${default=3 i}"
    }
    output {
        Int result = read_int(stdout())
    }
}

workflow optionals {
    Int? arg1
    Int arg1_well_def = "${default=1 arg1}"

#    call add { input: i= "${default=1 arg1}" }
    call add_opt
    output {
        add_opt.result
    }
}
