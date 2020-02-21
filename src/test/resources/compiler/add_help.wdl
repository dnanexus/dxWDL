version 1.0

task add_help {
    input {
        Int a
        Int b
        String c
        String d
        String e
    }
    command {
        echo $((${a} + ${b}))
    }
    meta {
        summary: "Adds two int together"
    }

    parameter_meta {
        a: {
            help: "lefthand side"
        }
        b: {
            help: "righthand side"
        }
        c: {
            description: "Use this"
        }
        d: {
            help: "Use this",
            description: "Don't use this"
        }
        e: "Use this"
    }

    output {
        Int result = read_int(stdout())
    }
}
