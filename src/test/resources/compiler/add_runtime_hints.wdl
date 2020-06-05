version 1.0

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
    docker: "ubuntu"
    dx_ignore_reuse: true
    dx_restart: {
      "default": 1,
      "max": 5,
      "errors": {
        "UnresponsiveWorker": 2,
        "ExecutionError": 2
      }
    }
    dx_timeout: "12H30M"
    dx_access: {
      "network": ["*"],
      "developer": true
    }
  }
}
