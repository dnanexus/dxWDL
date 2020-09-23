version development

task add_runtime_hints {
  input {
    Int a
    Int b
  }

  command {
    echo $((~{a} + ~{b}))
  }

  output {
    Int result = read_int(stdout())
  }

  runtime {
    container: "ubuntu"
  }

  hints {
    dnanexus: {
      ignore_reuse: true,
      restart: {
        default: 1,
        max: 5,
        errors: {
          UnresponsiveWorker: 2,
          ExecutionError: 2
        }
      },
      timeout: "12H30M",
      access: {
        network: ["*"],
        developer: true
      }
    }
  }
}
