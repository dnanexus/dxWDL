version 1.0

workflow inner_wf {
    input {
        Array[String] initial_arr = []
    }

    Array[String] non_input_decl = initial_arr

    call initial_task {
        input:
    }
    call second_task {
        input:
                input_metrics=initial_task.metrics,
        non_input_decl=non_input_decl,
    }

    output {
    }
}

task initial_task {
    input {
    }
    command <<<
    >>>
    output {
        Array[String] metrics = []
    }
}

task second_task {
    input {
        Array[String] non_input_decl
        Array[String] input_metrics
        Boolean unpassed_arg_default = true
    }
    command <<<
    >>>
    output {
    }
}
