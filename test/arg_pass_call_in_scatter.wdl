task two_args {
    Int x
    Int y

    command <<<
      echo ${y}
    >>>
    output {
      Int result = read_int(stdout())
    }
}

workflow arg_pass_call_in_scatter {
  Array[Int] xs
  scatter (x in xs) {
    call T { input: x=x }
  }
}