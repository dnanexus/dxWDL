version 1.0

workflow task_eval_workflow_outputs {
    input {
        Int a
    }
    command {}
    output {
        Int x = a + 1
    }
}
