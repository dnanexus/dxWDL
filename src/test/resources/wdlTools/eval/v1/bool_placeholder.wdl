version 1.0

task foo {
  Boolean flag = false

  command <<<
    ~{true="--yes" false="--no" flag}
    ~{true="--yes" false="--no" true}
  >>>
}
