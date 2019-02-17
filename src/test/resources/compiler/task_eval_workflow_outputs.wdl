version 1.0

task task_eval_workflow_outputs {
    input {
        Int a = 1
    }
    command {}
    output {
        Int x = a + 1
    }
}
