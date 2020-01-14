version 1.0

task add_help {
    input {
        Int a
        Int b
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
		}

    output {
        Int result = read_int(stdout())
    }
}
