# A sanity check, to see that Cromwell really works

task add {
    Int a
    Int b

    command {
        echo $((a + b))
    }
    output {
        Int result = a + b
    }
}


workflow simple {
    call add { input: a = 1, b = 3 }

    output {
        add.result
    }
}