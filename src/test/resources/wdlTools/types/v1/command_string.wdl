version 1.0

task foo {
  input {
    File? model_report
  }

  command <<<
    MODEL_REPORT=~{model_report}
  >>>
}
